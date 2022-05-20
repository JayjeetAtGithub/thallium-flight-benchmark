// #include <thallium.hpp>

// namespace tl = thallium;

// int main(int argc, char** argv) {

//     if(argc != 2) {
//         std::cerr << "Usage: " << argv[0] << " <address>" << std::endl;
//         exit(0);
//     }

//     tl::engine engine("tcp", THALLIUM_CLIENT_MODE);
//     tl::remote_procedure hello = engine.define("hello").disable_response();
//     tl::endpoint server = engine.lookup(argv[1]);
//     hello.on(server)();

//     return 0;
// }

#include <iostream>
#include <thallium.hpp>

namespace tl = thallium;

int main(int argc, char** argv) {

    tl::engine engine("tcp", THALLIUM_CLIENT_MODE);
    std::cout << "Client running at address " << engine.self() << std::endl;
    tl::remote_procedure remote_do_rdma = engine.define("do_rdma").disable_response();
    tl::remote_procedure scan = engine.define("scan").disable_response();
    tl::endpoint server_endpoint = engine.lookup(argv[1]);

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
            std::cout << "Server received bulk: ";
            for(auto c : v) std::cout << c;
            std::cout << std::endl;
        };
    engine.define("do_rdma", f).disable_response();

    // execute the RPC scan method on the server
    scan.on(server_endpoint)();

    return 0;
}
