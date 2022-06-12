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
#include "payload.h"


namespace tl = thallium;
namespace cp = arrow::compute;


// arrow::Result<std::shared_ptr<arrow::RecordBatch>> Scan(const scan_request& req) {
//     std::cout << "Filter: " << req.filter_buffer << std::endl;
//     std::cout << "Projection: " << req.projection_buffer << std::endl;
//     // define schema
//     auto f0 = arrow::field("f0", arrow::int64());
//     auto f1 = arrow::field("f1", arrow::binary());
//     auto f2 = arrow::field("f2", arrow::float64());

//     auto metadata = arrow::key_value_metadata({"foo"}, {"bar"});
//     auto schema = arrow::schema({f0, f1, f2}, metadata);

//     // generate some data
//     arrow::Int64Builder long_builder = arrow::Int64Builder();
//     std::vector<int64_t> values = {1, 2, 3};
//     ARROW_RETURN_NOT_OK(long_builder.AppendValues(values));
//     ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> a0, long_builder.Finish());

//     arrow::StringBuilder str_builder = arrow::StringBuilder();
//     std::vector<std::string> strvals = {"x", "y", "z"};
//     ARROW_RETURN_NOT_OK(str_builder.AppendValues(strvals));
//     ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> a1, str_builder.Finish());

//     arrow::DoubleBuilder dbl_builder = arrow::DoubleBuilder();
//     std::vector<double> dblvals = {1.1, 1.2, 2.3};
//     ARROW_RETURN_NOT_OK(dbl_builder.AppendValues(dblvals));
//     ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> a2, dbl_builder.Finish());

//     // create a record batch
//     auto batch = arrow::RecordBatch::Make(schema, 3, {a0, a1, a2});
//     return batch;
// }

int main(int argc, char** argv) {
    tl::engine engine("tcp", THALLIUM_SERVER_MODE);
    
    tl::remote_procedure do_rdma = engine.define("do_rdma");

    std::unordered_map<std::string, std::shared_ptr<arrow::RecordBatchReader>> reader_map;
    
    std::function<void(const tl::request&, const scan_request&)> scan = 
        [&reader_map](const tl::request &req, const scan_request& scan_req) {
            
            arrow::dataset::internal::Initialize();
            cp::ExecContext exec_context;
            std::shared_ptr<ScanResultConsumer> consumer = Scan(exec_context).ValueOrDie();
            std::shared_ptr<arrow::RecordBatchReader> reader = consumer->reader;
            auto t = reader->ToTable().ValueOrDie();
            std::cout << "Table: " << t->ToString() << std::endl;

            std::string uuid = generate_uuid();
            reader_map[uuid] = reader;
            return req.respond(uuid);
        };

    std::function<void(const tl::request&, const std::string&)> get_next_batch = 
        [&engine, &do_rdma, &reader_map](const tl::request &req, const std::string& uuid) {
            
            std::shared_ptr<arrow::RecordBatchReader> reader = reader_map[uuid];
            std::shared_ptr<arrow::RecordBatch> batch;
            if (reader->ReadNext(&batch).ok()) {
                std::cout << "Batch: " << batch->ToString() << std::endl;

                int num_cols = batch->num_columns();
                std::vector<std::pair<void*,std::size_t>> segments(num_cols*2);

                rdma_request rdma_req(num_cols);

                for (int64_t i = 0; i < num_cols; i++) {
                    std::shared_ptr<arrow::Array> col_arr = batch->column(i);
                    arrow::Type::type type = col_arr->type_id();
                    int64_t num_rows = col_arr->length();
                    int64_t null_count = col_arr->null_count();
                    int64_t offset = col_arr->offset();

                    rdma_req.num_rows = num_rows;
                    rdma_req.types.push_back((int)type);

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
                        segments[i*2].first  = (void*)data_buff->data();
                        segments[i*2].second = data_size;
                    }
                    rdma_req.data_buff_sizes.push_back(data_size);
                    rdma_req.offset_buff_sizes.push_back(offset_size);

                    tl::bulk arrow_bulk = engine.expose(segments, tl::bulk_mode::read_only);
                    do_rdma.on(req.get_endpoint())(rdma_req, arrow_bulk);
                }
                return req.respond(0);
            } else {
                return req.respond(1);
            }
        };
    
    engine.define("scan", scan);
    engine.define("get_next_batch", get_next_batch);

    std::cout << "Server running at address " << engine.self() << std::endl;            
};
