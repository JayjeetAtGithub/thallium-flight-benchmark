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
            std::vector<char> v(6);
            std::vector<std::pair<void*,std::size_t>> segments(1);
            segments[0].first  = (void*)(&v[0]);
            segments[0].second = v.size();
            tl::bulk local = engine.expose(segments, tl::bulk_mode::write_only);
            b.on(ep) >> local;
            std::cout << "Client received bulk: ";
            for(auto c : v) std::cout << c;
            std::cout << std::endl;
        };
    engine.define("do_rdma", f);

    scan_request req(1011);

    // execute the RPC scan method on the server
    // for (int i = 0; i < 5; ++i) {
        std::cout << "Doing RPC " << i << std::endl;
        scan.on(server_endpoint)(req);
        // tl::thread::sleep(engine, 1); // sleep for 1 second
    // }
}
