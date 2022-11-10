#include <iostream>
#include <unordered_map>

#include <arrow/api.h>
#include <arrow/compute/exec/expression.h>
#include <arrow/dataset/api.h>
#include <arrow/dataset/plan.h>
#include <arrow/filesystem/api.h>
#include <arrow/io/api.h>
#include <arrow/util/checked_cast.h>
#include <arrow/util/iterator.h>

#include <arrow/array/array_base.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/data.h>
#include <arrow/array/util.h>
#include <arrow/testing/random.h>
#include <arrow/util/key_value_metadata.h>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <thallium.hpp>
#include <bake-client.hpp>
#include <bake-server.hpp>

#include <yokan/cxx/server.hpp>
#include <yokan/cxx/admin.hpp>
#include <yokan/cxx/client.hpp>

#include "ace.h"
#include "trace.h"

namespace tl = thallium;
namespace bk = bake;
namespace cp = arrow::compute;
namespace yk = yokan;

#define BUFFER_SIZE 1024*1024

static char* read_input_file(const char* filename) {
    size_t ret;
    FILE*  fp = fopen(filename, "r");
    if (fp == NULL) {
        fprintf(stderr, "Could not open %s\n", filename);
        exit(-1);
    }
    fseek(fp, 0, SEEK_END);
    size_t sz = ftell(fp);
    fseek(fp, 0, SEEK_SET);
    char* buf = (char*)calloc(1, sz + 1);
    ret       = fread(buf, 1, sz, fp);
    if (ret != sz && ferror(fp)) {
        free(buf);
        perror("read_input_file");
        buf = NULL;
    }
    fclose(fp);
    return buf;
}

std::vector<std::pair<void*,std::size_t>> segments;

int main(int argc, char** argv) {

    if (argc < 4) {
        std::cout << "./ts [selectivity] [backend] [protocol]" << std::endl;
        exit(1);
    }

    std::string selectivity = argv[1];
    std::string backend = argv[2];
    std::string protocol = argv[3];

    tl::engine engine(protocol, THALLIUM_SERVER_MODE, true);
    margo_instance_id mid = engine.get_margo_instance();
    hg_addr_t svr_addr;
    hg_return_t hret = margo_addr_self(mid, &svr_addr);
    if (hret != HG_SUCCESS) {
        std::cerr << "Error: margo_addr_lookup()\n";
        margo_finalize(mid);
        return -1;
    }

    char *bake_config = read_input_file("bake_config.json");
    bk::provider *bp = bk::provider::create(
        mid, 0, ABT_POOL_NULL, std::string(bake_config, strlen(bake_config) + 1), ABT_IO_INSTANCE_NULL, NULL, NULL
    );

    char *yokan_config = read_input_file("yokan_config.json");
    yk::Provider yp(mid, 0, "ABCD", yokan_config, ABT_POOL_NULL, nullptr);
    yk::Client ycl(mid);
    yk::Admin admin(mid);
    yk_database_id_t db_id = admin.openDatabase(svr_addr, 0, "ABCD", "rocksdb", yokan_config);
    yk::Database db(ycl.handle(), svr_addr, 0, db_id);

    tl::remote_procedure do_rdma = engine.define("do_rdma");

    std::unordered_map<std::string, std::shared_ptr<arrow::RecordBatchReader>> reader_map;
    bk::client bcl(mid);
    bk::provider_handle bph(bcl, svr_addr, 0);
    bph.set_eager_limit(0);
    bk::target tid = bp->list_targets()[0];

    tl::bulk arrow_bulk;

    std::function<void(const tl::request&, const ScanReqRPCStub&)> scan = 
        [&engine, &reader_map, &mid, &svr_addr, &bp, &bcl, &bph, &tid, &db, &backend, &selectivity, &arrow_bulk, &segments](const tl::request &req, const ScanReqRPCStub& stub) {
            arrow::dataset::internal::Initialize();
            std::shared_ptr<arrow::RecordBatchReader> reader;

            if (backend == "dataset" || backend == "dataset+mem") {
                cp::ExecContext exec_ctx;
                reader = ScanDataset(exec_ctx, stub, backend, selectivity).ValueOrDie();
            } else if (backend == "file" || backend == "file+mmap") {
                reader = ScanFile(stub, backend, selectivity).ValueOrDie();
            } else if (backend == "bake") {
                size_t value_size = 28;
                void *value_buf = malloc(28);
                
                db.get((void*)stub.path.c_str(), stub.path.length(), value_buf, &value_size);
                bk::region rid(std::string((char*)value_buf, value_size));

                uint8_t *ptr = (uint8_t*)bcl.get_data(bph, tid, rid);
                reader = ScanBake(stub, ptr, selectivity).ValueOrDie();
            }

            std::string uuid = boost::uuids::to_string(boost::uuids::random_generator()());
            reader_map[uuid] = reader;

            segments.reserve(34);
            
            std::cout << "Allocating Segments: " << segments.size() << std::endl;
            for (int i = 0; i < segments.size(); i++) {
                auto buf = arrow::AllocateBuffer(BUFFER_SIZE).ValueOrDie();
                memset(buf->mutable_data(), 0, BUFFER_SIZE);
                segments[i].first = (void*)buf->mutable_data();
                segments[i].second = BUFFER_SIZE;
            }

            tl::bulk some_bulk = engine.expose(segments, tl::bulk_mode::read_write);
            return req.respond(uuid);
        };

    int64_t total_rows_written = 0;
    std::function<void(const tl::request&, const std::string&)> get_next_batch = 
        [&mid, &svr_addr, &engine, &do_rdma, &reader_map, &total_rows_written, &arrow_bulk](const tl::request &req, const std::string& uuid) {
            
            std::shared_ptr<arrow::RecordBatchReader> reader = reader_map[uuid];
            std::shared_ptr<arrow::RecordBatch> batch;

            if (reader->ReadNext(&batch).ok() && batch != nullptr) {

                std::vector<int64_t> data_buff_sizes;
                std::vector<int64_t> offset_buff_sizes;
                int64_t num_rows = batch->num_rows();
                total_rows_written += num_rows;

                std::vector<std::pair<void*,std::size_t>> segments(batch->num_columns()*2);

                std::string null_buff = "xx";

                for (int64_t i = 0; i < batch->num_columns(); i++) {
                    std::shared_ptr<arrow::Array> col_arr = batch->column(i);
                    arrow::Type::type type = col_arr->type_id();
                    int64_t null_count = col_arr->null_count();

                    int64_t data_size = 0;
                    int64_t offset_size = 0;

                    if (is_binary_like(type)) {
                        std::shared_ptr<arrow::Buffer> data_buff = 
                            std::static_pointer_cast<arrow::BinaryArray>(col_arr)->value_data();
                        std::shared_ptr<arrow::Buffer> offset_buff = 
                            std::static_pointer_cast<arrow::BinaryArray>(col_arr)->value_offsets();
                        data_size = data_buff->size();
                        offset_size = offset_buff->size();
                        // segments[i*2].first = (void*)data_buff->data();
                        memcpy(segments[i*2].first, (void*)data_buff->data(), data_size);
                        segments[i*2].second = data_size;
                        // segments[(i*2)+1].first = (void*)offset_buff->data();
                        memcpy(segments[(i*2)+1].first, (void*)offset_buff->data(), offset_size);
                        segments[(i*2)+1].second = offset_size;
                    } else {
                        std::shared_ptr<arrow::Buffer> data_buff = 
                            std::static_pointer_cast<arrow::PrimitiveArray>(col_arr)->values();
                        data_size = data_buff->size();
                        offset_size = null_buff.size() + 1; 
                        // segments[i*2].first  = (void*)data_buff->data();
                        memcpy(segments[i*2].first, (void*)data_buff->data(), data_size);
                        segments[i*2].second = data_size;
                        // segments[(i*2)+1].first = (void*)(&null_buff[0]);
                        memcpy(segments[(i*2)+1].first, (void*)(&null_buff[0]), offset_size);
                        segments[(i*2)+1].second = offset_size;
                    }

                    data_buff_sizes.push_back(data_size);
                    offset_buff_sizes.push_back(offset_size);
                }
                
                // tl::bulk arrow_bulk;
                
                // {
                //     Trace t("server: engine.expose");
                //     arrow_bulk = engine.expose(segments, tl::bulk_mode::read_only);
                // }
                do_rdma.on(req.get_endpoint())(num_rows, data_buff_sizes, offset_buff_sizes, arrow_bulk);
                return req.respond(0);
            } else {
                reader_map.erase(uuid);
                return req.respond(1);
            }
        };
    
    engine.define("scan", scan);
    engine.define("get_next_batch", get_next_batch);

    std::cout << "Server running at address " << engine.self() << std::endl;    
    engine.wait_for_finalize();        
};
