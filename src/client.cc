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
    tl::remote_procedure remote_do_rdma = engine.define("do_rdma").disable_response();
    tl::endpoint server_endpoint = engine.lookup(argv[1]);

    std::string buffer = "Matthieu";
    std::vector<std::pair<void*,std::size_t>> segments(1);
    segments[0].first  = (void*)(&buffer[0]);
    segments[0].second = buffer.size()+1;

    tl::bulk myBulk = engine.expose(segments, tl::bulk_mode::read_only);

    remote_do_rdma.on(server_endpoint)(myBulk);

    return 0;
}
