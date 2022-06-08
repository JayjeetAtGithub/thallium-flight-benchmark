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


arrow::Result<std::shared_ptr<arrow::RecordBatch>> Scan() {
    // Define schema
    auto f0 = arrow::field("f0", arrow::int64());
    // auto f1 = arrow::field("f1", arrow::binary());
    // auto f2 = arrow::field("f2", arrow::float64());

    auto metadata = arrow::key_value_metadata({"foo"}, {"bar"});
    auto schema = arrow::schema({f0}, metadata);

    // Generate some data
    arrow::Int64Builder long_builder = arrow::Int64Builder();
    std::vector<int64_t> values = {1, 2, 3};
    ARROW_RETURN_NOT_OK(long_builder.AppendValues(values));
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> a0, long_builder.Finish());

    // arrow::StringBuilder str_builder = arrow::StringBuilder();
    // std::vector<std::string> strvals = {"x", "y", "z"};
    // ARROW_RETURN_NOT_OK(str_builder.AppendValues(strvals));
    // ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> a1, str_builder.Finish());

    // arrow::DoubleBuilder dbl_builder = arrow::DoubleBuilder();
    // std::vector<double> dblvals = {1.1, 1.1, 2.3};
    // ARROW_RETURN_NOT_OK(dbl_builder.AppendValues(dblvals));
    // ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> a2, dbl_builder.Finish());

    // Create a record batch
    auto batch = arrow::RecordBatch::Make(schema, 3, {a0});
    return batch;
}

int main(int argc, char** argv) {

    // define the thalllium server
    tl::engine engine("tcp", THALLIUM_SERVER_MODE);

    // define the remote do_rdma procedure
    tl::remote_procedure do_rdma = engine.define("do_rdma");

    // define the RPC method   
    std::function<void(const tl::request&, const scan_request&)> scan = 
        [&engine, &do_rdma](const tl::request &req, const scan_request& sr) {
            std::string buffer = "mattieu";
            std::cout << "Filter: " << sr.filter_buffer << std::endl;
            std::cout << "Projection: " << sr.projection_buffer << std::endl;

            auto b = Scan().ValueOrDie();
            std::cout << "Batch: " << b->ToString() << std::endl;

            // now we need to send by column array to the client
            int num_columns = b->num_columns();
            for (int i = 0; i < num_columns; i++) {
                auto col_arr = b->column(i);
                int type = (int)col_arr->type_id();
                int64_t length = col_arr->length();
                int64_t null_count = col_arr->null_count();
                int64_t offset = col_arr->offset();
                std::shared_ptr<arrow::Buffer> data_buff = col_arr->values();

                // send the column array to the client
                std::vector<std::pair<void*,std::size_t>> segments(1);
                segments[0].first  = (void*)(data_buff->data()[0]);
                segments[0].second = data_buff->size();
                tl::bulk arrow_bulk = engine.expose(segments, tl::bulk_mode::read_only);
                std::cout << "About to do RDMA " << req.get_endpoint() << std::endl;
                do_rdma.on(req.get_endpoint())(arrow_bulk);
            }
        };
    engine.define("scan", scan);

    // run the server
    std::cout << "Server running at address " << engine.self() << std::endl;
}
