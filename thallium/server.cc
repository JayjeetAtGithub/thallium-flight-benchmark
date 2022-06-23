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

#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <thallium.hpp>

#include "ace.h"

namespace tl = thallium;
namespace cp = arrow::compute;


int main(int argc, char** argv) {
    tl::engine engine("verbs", THALLIUM_SERVER_MODE);
    
    tl::remote_procedure do_rdma = engine.define("do_rdma");

    std::unordered_map<std::string, std::shared_ptr<arrow::RecordBatchReader>> reader_map;
    
    std::function<void(const tl::request&, const ScanReqRPCStub&)> scan = 
        [&reader_map](const tl::request &req, const ScanReqRPCStub& stub) {
            
            arrow::dataset::internal::Initialize();
            cp::ExecContext exec_context;
            std::shared_ptr<ScanResultConsumer> consumer = ScanB(exec_context, stub).ValueOrDie();
            std::shared_ptr<arrow::RecordBatchReader> reader = consumer->reader;

            std::string uuid = generate_uuid();
            reader_map[uuid] = reader;
            return req.respond(uuid);
        };

    int64_t total_rows_written = 0;
    std::function<void(const tl::request&, const std::string&)> get_next_batch = 
        [&engine, &do_rdma, &reader_map, &total_rows_written](const tl::request &req, const std::string& uuid) {
            
            std::shared_ptr<arrow::RecordBatchReader> reader = reader_map[uuid];
            std::shared_ptr<arrow::RecordBatch> batch;
            if (reader->ReadNext(&batch).ok() && batch != nullptr) {
                total_rows_written += batch->num_rows();

                int64_t num_rows;
                int64_t num_cols;
                std::vector<int> types;
                std::vector<int64_t> data_buff_sizes;
                std::vector<int64_t> offset_buff_sizes;

                num_rows = batch->num_rows();
                num_cols = batch->num_columns();
                std::vector<std::pair<void*,std::size_t>> segments(num_cols*2);

                std::string null_buff = "xx";

                for (int64_t i = 0; i < num_cols; i++) {
                    std::shared_ptr<arrow::Array> col_arr = batch->column(i);
                    arrow::Type::type type = col_arr->type_id();
                    int64_t null_count = col_arr->null_count();

                    types.push_back((int)type);

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
                do_rdma.on(req.get_endpoint())(num_rows, num_cols, types, data_buff_sizes, offset_buff_sizes, arrow_bulk);
                return req.respond(0);
            } else {
                return req.respond(1);
            }
        };
    
    engine.define("scan", scan);
    engine.define("get_next_batch", get_next_batch);

    std::cout << "Server running at address " << engine.self() << std::endl;            
};
