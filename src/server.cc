#include <iostream>

#include <arrow/api.h>
#include <arrow/compute/exec/expression.h>
#include <arrow/dataset/api.h>
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

#include "request.h"


namespace tl = thallium;


arrow::Result<std::shared_ptr<arrow::RecordBatch>> Scan(const scan_request& req) {
    std::cout << "Filter: " << req.filter_buffer << std::endl;
    std::cout << "Projection: " << req.projection_buffer << std::endl;
    // define schema
    auto f0 = arrow::field("f0", arrow::int64());
    auto f1 = arrow::field("f1", arrow::binary());
    auto f2 = arrow::field("f2", arrow::float64());

    auto metadata = arrow::key_value_metadata({"foo"}, {"bar"});
    auto schema = arrow::schema({f0, f1, f2}, metadata);

    // generate some data
    arrow::Int64Builder long_builder = arrow::Int64Builder();
    std::vector<int64_t> values = {1, 2, 3};
    ARROW_RETURN_NOT_OK(long_builder.AppendValues(values));
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> a0, long_builder.Finish());

    arrow::StringBuilder str_builder = arrow::StringBuilder();
    std::vector<std::string> strvals = {"x", "y", "z"};
    ARROW_RETURN_NOT_OK(str_builder.AppendValues(strvals));
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> a1, str_builder.Finish());

    arrow::DoubleBuilder dbl_builder = arrow::DoubleBuilder();
    std::vector<double> dblvals = {1.1, 1.2, 2.3};
    ARROW_RETURN_NOT_OK(dbl_builder.AppendValues(dblvals));
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> a2, dbl_builder.Finish());

    // create a record batch
    auto batch = arrow::RecordBatch::Make(schema, 3, {a0, a1, a2});
    return batch;
}

int main(int argc, char** argv) {
    tl::engine engine("tcp", THALLIUM_SERVER_MODE);
    
    tl::remote_procedure do_rdma = engine.define("do_rdma").disable_response();
    
    std::function<void(const tl::request&, const scan_request&)> scan = 
        [&engine, &do_rdma](const tl::request &req, const scan_request& scan_req) {

            auto b = Scan(scan_req).ValueOrDie();
            std::cout << "Batch: " << b->ToString() << std::endl;

            int num_columns = b->num_columns();
            for (int i = 0; i < num_columns; i++) {
                std::shared_ptr<arrow::Array> col_arr = b->column(i);
                arrow::Type::type type = col_arr->type_id();
                int64_t length = col_arr->length();
                int64_t null_count = col_arr->null_count();
                int64_t offset = col_arr->offset();

                std::vector<std::pair<void*,std::size_t>> segments;
                int64_t total_bytes = 0;
                if (is_binary_like(type)) {
                    std::cout << "Binary-like" << std::endl;
                    std::shared_ptr<arrow::Buffer> data_buff = 
                        std::static_pointer_cast<arrow::BinaryArray>(col_arr)->value_data();
                    std::shared_ptr<arrow::Buffer> offset_buff = 
                        std::static_pointer_cast<arrow::BinaryArray>(col_arr)->value_offsets();
                    int64_t data_size = data_buff->size();
                    int64_t offset_size = offset_buff->size();
                    segments.resize(2);
                    segments[0].first = (void*)data_buff->data();
                    segments[0].second = data_size;
                    segments[1].first = (void*)offset_buff->data();
                    segments[1].second = offset_size;
                    total_bytes = data_size + offset_size;
                } else {
                    std::cout << "Not binary-like" << std::endl;
                    std::shared_ptr<arrow::Buffer> data_buff = 
                        std::static_pointer_cast<arrow::PrimitiveArray>(col_arr)->values();
                    int64_t data_size = data_buff->size();
                    segments[0].first  = (void*)data_buff->data();
                    segments[0].second = data_size;
                    total_bytes = data_size;
                }

                tl::bulk arrow_bulk = engine.expose(segments, tl::bulk_mode::read_only);
                do_rdma.on(req.get_endpoint())((int)type, length, total_bytes, arrow_bulk);
            }
        };
    engine.define("scan", scan);
    std::cout << "Server running at address " << engine.self() << std::endl;
}
