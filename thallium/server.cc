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

#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/data.h"
#include "arrow/array/util.h"
#include "arrow/testing/random.h"
#include "arrow/util/key_value_metadata.h"

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

namespace tl = thallium;
namespace bk = bake;
namespace cp = arrow::compute;
namespace yk = yokan;

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

class concurrent_queue {
    private:
        std::deque<std::shared_ptr<arrow::RecordBatch>> batch_queue;
        tl::mutex m;
        tl::condition_variable cv;
        bool alive;
    public:
        void start() { alive = true; }
        void end() { alive = false; }

        void push(std::shared_ptr<arrow::RecordBatch> batch) {
            std::unique_lock<tl::mutex> lock(m);
            batch_queue.push_back(batch);
            lock.unlock();
            cv.notify_one();
        }

        void clear() { batch_queue.clear(); }
        size_t size() { return batch_queue.size(); }

        bool empty() {
            bool emp = false;
            {
                std::lock_guard<tl::mutex> lock(m);
                emp = batch_queue.empty();
            }
            return emp;
        }

        void wait_and_pop(std::shared_ptr<arrow::RecordBatch> &batch) {
            std::unique_lock<tl::mutex> lock(m);
            while (batch_queue.empty() && alive) {
                cv.wait(lock);
            }
            batch = batch_queue.front();
            batch_queue.pop_front();
        }
};

concurrent_queue cq;

void scan_handler(void *arg) {
    cq.start();
    std::cout << "Initial size : " << cq.size() << std::endl;
    arrow::RecordBatchReader *reader = (arrow::RecordBatchReader*)arg;
    std::shared_ptr<arrow::RecordBatch> batch;
    reader->ReadNext(&batch);
    while (batch != nullptr) {
        cq.push(batch);
        reader->ReadNext(&batch);
    }
    cq.end();
}

int main(int argc, char** argv) {

    if (argc < 2) {
        std::cout << "./ts[bench_mode]" << std::endl;
        exit(1);
    }

    int bench_mode = atoi(argv[1]);
    tl::engine engine("verbs://ibp130s0", THALLIUM_SERVER_MODE, true);
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

    tl::managed<tl::xstream> xstream = 
        tl::xstream::create(tl::scheduler::predef::deflt, engine.get_progress_pool());

    std::function<void(const tl::request&, const ScanReqRPCStub&)> scan = 
        [&reader_map, &mid, &svr_addr, &bp, &bcl, &bph, &tid, &db, &bench_mode, &xstream](const tl::request &req, const ScanReqRPCStub& stub) {
            arrow::dataset::internal::Initialize();
            std::shared_ptr<arrow::RecordBatchReader> reader;

            if (bench_mode == 1 || bench_mode == 2) {
                std::cout << "Running transport benchmark in mode " << bench_mode << std::endl;
                cp::ExecContext exec_ctx;
                reader = ScanBenchmark(exec_ctx, stub, bench_mode).ValueOrDie();
            } else if (bench_mode == 3) {
                std::cout << "Scanning data from ext4 using mmap\n";
                reader = ScanEXT4MMap(stub).ValueOrDie();
            } else if (bench_mode == 4) {
                std::cout << "Scanning data from ext4\n";
                reader = ScanEXT4(stub).ValueOrDie();
            } else if (bench_mode == 5) {
                std::cout << "Scanning data from bake\n";
                size_t value_size = 28;
                void *value_buf = malloc(28);
                
                db.get((void*)stub.path.c_str(), stub.path.length(), value_buf, &value_size);
                bk::region rid(std::string((char*)value_buf, value_size));

                uint8_t *ptr = (uint8_t*)bcl.get_data(bph, tid, rid);
                reader = ScanBake(stub, ptr).ValueOrDie();
            }

            xstream->make_thread([&]() {
                std::cout << "Made thread for " << stub.path.c_str() << std::endl;
                scan_handler((void*)reader.get());
            });
            
        };

    std::function<void(const tl::request&)> clear = 
        [](const tl::request &req) {
            cq.clear();
            req.respond(0);
        };

    int64_t total_rows_written = 0;
    std::function<void(const tl::request&)> get_next_batch = 
        [&mid, &svr_addr, &engine, &do_rdma, &reader_map, &total_rows_written](const tl::request &req) {
            
            std::shared_ptr<arrow::RecordBatch> batch = nullptr;
            cq.wait_and_pop(batch);

            if (batch) {

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
                        segments[i*2].first = (void*)data_buff->data();
                        segments[i*2].second = data_size;
                        segments[(i*2)+1].first = (void*)offset_buff->data();
                        segments[(i*2)+1].second = offset_size;
                    } else {
                        std::shared_ptr<arrow::Buffer> data_buff = 
                            std::static_pointer_cast<arrow::PrimitiveArray>(col_arr)->values();
                        data_size = data_buff->size();
                        offset_size = null_buff.size() + 1; 
                        segments[i*2].first  = (void*)data_buff->data();
                        segments[i*2].second = data_size;
                        segments[(i*2)+1].first = (void*)(&null_buff[0]);
                        segments[(i*2)+1].second = offset_size;
                    }

                    data_buff_sizes.push_back(data_size);
                    offset_buff_sizes.push_back(offset_size);
                }

                tl::bulk arrow_bulk = engine.expose(segments, tl::bulk_mode::read_only);
                do_rdma.on(req.get_endpoint())(num_rows, data_buff_sizes, offset_buff_sizes, arrow_bulk);
                return req.respond(0);
            } else {
                return req.respond(1);
            }
        };
    
    engine.define("scan", scan).disable_response();
    engine.define("get_next_batch", get_next_batch);
    engine.define("clear", clear);

    std::cout << "Server running at address " << engine.self() << std::endl;    
    engine.wait_for_finalize();        
};
