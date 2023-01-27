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

#include "ace.h"

namespace tl = thallium;
namespace cp = arrow::compute;

class MeasureExecutionTime{
  private:
      const std::chrono::steady_clock::time_point begin;
      const std::string caller;
      ofstream log;
  public:
      MeasureExecutionTime(const std::string& caller):caller(caller),begin(std::chrono::steady_clock::now()){
        log.open ("example.txt");
      }
      ~MeasureExecutionTime(){
          const auto duration=std::chrono::steady_clock::now()-begin;
          std::string s = caller + " : " + std::to_string((double)std::chrono::duration_cast<std::chrono::microseconds>(duration).count()/1000) + "\n";
          std::cout << s;
          log << s;
          log.close();
      }
};


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

    tl::remote_procedure do_rdma = engine.define("do_rdma");
    std::unordered_map<std::string, std::shared_ptr<arrow::RecordBatchReader>> reader_map;

    std::function<void(const tl::request&, const ScanReqRPCStub&)> scan = 
        [&reader_map, &mid, &svr_addr, &backend, &selectivity](const tl::request &req, const ScanReqRPCStub& stub) {
            arrow::dataset::internal::Initialize();
            std::shared_ptr<arrow::RecordBatchReader> reader;

            if (backend == "dataset" || backend == "dataset+mem") {
                cp::ExecContext exec_ctx;
                reader = ScanDataset(exec_ctx, stub, backend, selectivity).ValueOrDie();
            } else if (backend == "file" || backend == "file+mmap") {
                reader = ScanFile(stub, backend, selectivity).ValueOrDie();
            }

            std::string uuid = boost::uuids::to_string(boost::uuids::random_generator()());
            reader_map[uuid] = reader;
            return req.respond(uuid);
        };

    int64_t total_rows_written = 0;
    std::function<void(const tl::request&, const std::string&)> get_next_batch = 
        [&mid, &svr_addr, &engine, &do_rdma, &reader_map, &total_rows_written](const tl::request &req, const std::string& uuid) {
            
            std::shared_ptr<arrow::RecordBatchReader> reader = reader_map[uuid];
            std::shared_ptr<arrow::RecordBatch> batch;

            {
                MeasureExecutionTime m("I/O");
                reader->ReadNext(&batch);
            }

            if (batch != nullptr) {

                std::vector<int64_t> data_buff_sizes;
                std::vector<int64_t> offset_buff_sizes;
                int64_t num_rows = batch->num_rows();
                total_rows_written += num_rows;

                std::vector<std::pair<void*,std::size_t>> segments;
                segments.reserve(batch->num_columns()*2);

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
                        segments.emplace_back(std::make_pair((void*)data_buff->data(), data_size));
                        segments.emplace_back(std::make_pair((void*)offset_buff->data(), offset_size));
                    } else {

                        std::shared_ptr<arrow::Buffer> data_buff = 
                            std::static_pointer_cast<arrow::PrimitiveArray>(col_arr)->values();

                        data_size = data_buff->size();
                        offset_size = null_buff.size() + 1; 
                        segments.emplace_back(std::make_pair((void*)data_buff->data(), data_size));
                        segments.emplace_back(std::make_pair((void*)(&null_buff[0]), offset_size));
                    }

                    data_buff_sizes.push_back(data_size);
                    offset_buff_sizes.push_back(offset_size);
                }

                tl::bulk arrow_bulk;
                {
                    MeasureExecutionTime m("server_expose");
                    arrow_bulk = engine.expose(segments, tl::bulk_mode::read_only);
                }
                
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
