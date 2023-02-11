#include <iostream>
#include <thread>
#include <chrono>
#include <fstream>

#include <thallium.hpp>
#include <boost/crc.hpp>

namespace tl = thallium;

uint32_t calc_crc(const std::string& my_string) {
    boost::crc_32_type result;
    result.process_bytes(my_string.data(), my_string.length());
    return result.checksum();
}

class MeasureExecutionTime{
    private:
        const std::chrono::steady_clock::time_point begin;
        const std::string caller;
        std::ofstream log;
    public:
        MeasureExecutionTime(const std::string& caller):caller(caller),begin(std::chrono::steady_clock::now()) {
            log.open("result_client.txt", std::ios_base::app);
        }
        
        ~MeasureExecutionTime() {
            const auto duration=std::chrono::steady_clock::now()-begin;
            std::string s = caller + " : " + std::to_string((double)std::chrono::duration_cast<std::chrono::microseconds>(duration).count()/1000) + "\n";
            std::cout << s;
            log << s;
            log.close();
        }
};

#ifndef MEASURE_FUNCTION_EXECUTION_TIME
#define MEASURE_FUNCTION_EXECUTION_TIME const MeasureExecutionTime measureExecutionTime(__FUNCTION__);
#endif

std::vector<std::pair<void*,std::size_t>> segments(1);     
tl::bulk local;

size_t GetNext(tl::engine& engine, tl::endpoint& endpoint, bool flag) {
    std::function<void(const tl::request&, tl::bulk&)> f =
        [&engine, &endpoint, &flag, &segments, &local](const tl::request& req, tl::bulk& b) {
            if (flag) {       
                {
                    MeasureExecutionTime m("memory_allocate");
                    segments[0].first = (uint8_t*)malloc((32*1024*1024)+1);
                    segments[0].second = (32*1024*1024)+1;
                }
                {
                    MeasureExecutionTime m("client_expose");
                    local = engine.expose(segments, tl::bulk_mode::write_only);
                }
            }

            {
                MeasureExecutionTime m("RDMA");
                b.on(req.get_endpoint()) >> local;
                segments[0].second = (32*1024*1024)+1;
            }

            {
                MeasureExecutionTime m("calc_crc");
                uint32_t crc = calc_crc(std::string((char*)segments[0].first, (32*1024*1024)+1));
                std::cout << "crc: " << crc << std::endl;
            }

            return req.respond(0);
        };
    engine.define("do_rdma", f);
    tl::remote_procedure get_next = engine.define("get_next");

    return get_next.on(endpoint)();
}

int main(int argc, char **argv) {
    if (argc < 2) {
        std::cout << "./tc [uri]" << std::endl;
        exit(1);
    }

    std::string uri = argv[1];
    std::string protocol = "ofi+verbs";

    tl::engine engine(protocol, THALLIUM_SERVER_MODE, true);
    tl::endpoint endpoint = engine.lookup(uri);
    
    tl::remote_procedure scan = engine.define("scan");
    int e = scan.on(endpoint)();

    {
        MEASURE_FUNCTION_EXECUTION_TIME
        for (int i = 0; i < 100; i++) {
            GetNext(engine, endpoint, (i == 0));
        }
    }

    engine.finalize();
    return 0;
}
