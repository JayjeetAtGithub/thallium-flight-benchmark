#include <iostream>
#include <thread>
#include <chrono>

#include <thallium.hpp>

#include "request.h"


namespace tl = thallium;

int main(int argc, char** argv) {

    tl::engine engine("tcp", THALLIUM_SERVER_MODE);
    tl::endpoint server_endpoint = engine.lookup(argv[1]);
    std::cout << "Client running at address " << engine.self() << std::endl;

    tl::remote_procedure scan = engine.define("scan");

    // define the RDMA handler method
    std::function<void(const tl::request&, tl::bulk&)> f =
        [&engine](const tl::request& req, tl::bulk& b) {
            std::cout << "RDMA received from " << req.get_endpoint() << std::endl;
            tl::endpoint ep = req.get_endpoint();
            std::vector<int64_t> v(24);
            std::vector<std::pair<void*,std::size_t>> segments(1);
            segments[0].first  = (void*)(&v[0]);
            segments[0].second = v.size();
            tl::bulk local = engine.expose(segments, tl::bulk_mode::write_only);
            b.on(ep) >> local;

            std::shared_ptr<arrow::PrimitiveArray> arr = std::make_shared<arrow::PrimitiveArray>(arrow::int64(), 3, &v[0]);
            auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("a", arrow::int64())}), 3, {arr});    
            std::cout << "Batch: " << batch->ToString() << std::endl;

            std::cout << "Client received bulk: ";
            for(auto c : v) std::cout << c;
            std::cout << std::endl;
        };
    engine.define("do_rdma", f);
    
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

    // execute the RPC scan method on the server
    // for (int i = 0; i < 5; ++i) {
        // std::cout << "Doing RPC " << i << std::endl;
        scan.on(server_endpoint)(req);
        // tl::thread::sleep(engine, 1); // sleep for 1 second
    // }
}
