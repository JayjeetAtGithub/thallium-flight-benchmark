#include <iostream>
#include <thread>
#include <chrono>
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

#include "util.h"
#include "payload.h"


namespace tl = thallium;


int main(int argc, char** argv) {

    tl::engine engine("tcp", THALLIUM_SERVER_MODE);
    tl::endpoint server_endpoint = engine.lookup(argv[1]);
    std::cout << "Client running at address " << engine.self() << std::endl;

    tl::remote_procedure scan = engine.define("scan");
    tl::remote_procedure get_next_batch = engine.define("get_next_batch");
    
    std::vector<std::shared_ptr<arrow::Array>> columns;

    std::function<void(const tl::request&, int&, int64_t&, int64_t&, int64_t&, tl::bulk&)> f =
        [&engine, &columns](const tl::request& req, int& type_id, int64_t& length, int64_t& data_size, int64_t& offset_size, tl::bulk& b) {

            std::shared_ptr<arrow::DataType> type = type_from_id(type_id);        

            if (is_binary_like(type->id())) {
                std::unique_ptr<arrow::Buffer> data_buff = arrow::AllocateBuffer(data_size).ValueOrDie();
                std::unique_ptr<arrow::Buffer> offset_buff = arrow::AllocateBuffer(offset_size).ValueOrDie();

                std::vector<std::pair<void*,std::size_t>> segments(2);
                segments[0].first  = (void*)data_buff->mutable_data();
                segments[0].second = data_buff->size();
                segments[1].first  = (void*)offset_buff->mutable_data();
                segments[1].second = offset_buff->size();

                tl::bulk local = engine.expose(segments, tl::bulk_mode::write_only);
                b.on(req.get_endpoint()) >> local;
                std::shared_ptr<arrow::Array> col = 
                    std::make_shared<arrow::StringArray>(length, std::move(offset_buff), std::move(data_buff));
                columns.push_back(col);
            } else {
                std::unique_ptr<arrow::Buffer> data_buff = arrow::AllocateBuffer(data_size).ValueOrDie();
                
                std::vector<std::pair<void*,std::size_t>> segments(1);
                segments[0].first  = (void*)data_buff->mutable_data();
                segments[0].second = data_buff->size();
                
                tl::bulk local = engine.expose(segments, tl::bulk_mode::write_only);
                b.on(req.get_endpoint()) >> local;
                std::shared_ptr<arrow::Array> col = 
                    std::make_shared<arrow::PrimitiveArray>(type, length, std::move(data_buff));
                columns.push_back(col);
            }
        };
    engine.define("do_rdma", f).disable_response();
    
    char *filter_buffer = new char[6];
    filter_buffer[0] = 'f';
    filter_buffer[1] = 'i';
    filter_buffer[2] = 'l';
    filter_buffer[3] = 't';
    filter_buffer[4] = 'e';
    filter_buffer[5] = 'r';

    char *projection_buffer = new char[4];
    projection_buffer[0] = 'p';
    projection_buffer[1] = 'r';
    projection_buffer[2] = 'o';
    projection_buffer[3] = 'j';

    scan_request req(filter_buffer, 6, projection_buffer, 4);

    // all for a particular storage server
    std::string uuid = scan.on(server_endpoint)(req);


    if (e != 200) {
        std::cout << "Error: " << e << std::endl;
        return 1;
    } else {
        std::cout << "Scan success" << std::endl;

        int e = get_next_batch.on(server_endpoint)(uuid);
    }


}
