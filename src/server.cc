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
    const int length = 10;

    auto f0 = arrow::field("f0", arrow::int32());
    auto f1 = arrow::field("f1", arrow::uint8());
    auto f2 = arrow::field("f2", arrow::int16());

    auto metadata = arrow::key_value_metadata({"foo"}, {"bar"});
    auto schema = arrow::schema({f0, f1, f2}, metadata);

    arrow::random::RandomArrayGenerator gen(42);

    auto a0 = gen.ArrayOf(arrow::int32(), length);
    auto a1 = gen.ArrayOf(arrow::uint8(), length);
    auto a2 = gen.ArrayOf(arrow::int16(), length);

    auto batch = arrow::RecordBatch::Make(schema, length, {a0, a1, a2});
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


            std::vector<std::pair<void*,std::size_t>> segments(1);
            segments[0].first  = (void*)(&buffer[0]);
            segments[0].second = buffer.size()+1;
            tl::bulk arrow_bulk = engine.expose(segments, tl::bulk_mode::read_only);
            std::cout << "About to do RDMA " << req.get_endpoint() << std::endl;
            do_rdma.on(req.get_endpoint())(arrow_bulk);
        };
    engine.define("scan", scan);

    // run the server
    std::cout << "Server running at address " << engine.self() << std::endl;
}
