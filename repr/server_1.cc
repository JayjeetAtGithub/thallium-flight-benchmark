#include <iostream>
#include <unordered_map>
#include <fstream>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/crc.hpp>

#include <thallium.hpp>


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
            log.open("result_server.txt", std::ios_base::app);
        }

        ~MeasureExecutionTime() {
            const auto duration=std::chrono::steady_clock::now()-begin;
            std::string s = caller + " : " + std::to_string((double)std::chrono::duration_cast<std::chrono::microseconds>(duration).count()/1000) + "\n";
            std::cout << s;
            log << s;
            log.close();
        }
};

static char* read_input_file(const char* filename) {
    size_t ret;
    FILE*  fp = fopen(filename, "r");
    if (fp == NULL) {
        fprintf(stderr, "Could not open %s\n", filename);
        exit(-1);
    }
    fseek(fp, 0, SEEK_END);
    size_t sz = ftell(fp);
    fseek(fp, 0, SEEK_SET);
    char* buf = (char*)calloc(1, sz + 1);
    ret       = fread(buf, 1, sz, fp);
    if (ret != sz && ferror(fp)) {
        free(buf);
        perror("read_input_file");
        buf = NULL;
    }
    fclose(fp);
    return buf;
}

int main(int argc, char** argv) {
    std::string protocol = "ofi+verbs";

    tl::engine engine(protocol, THALLIUM_SERVER_MODE, true);
    margo_instance_id mid = engine.get_margo_instance();
    hg_addr_t svr_addr;
    hg_return_t hret = margo_addr_self(mid, &svr_addr);
    if (hret != HG_SUCCESS) {
        std::cerr << "Error: margo_addr_lookup()\n";
        margo_finalize(mid);
        return -1;
    }

    tl::remote_procedure do_rdma = engine.define("do_rdma");

    uint8_t *data_buff = (uint8_t*)malloc(32*1024*1024);

    std::function<void(const tl::request&)> scan = 
        [&mid, &svr_addr, &data_buff](const tl::request &req) {
            {
                MeasureExecutionTime m("I/O");
                std::string filename = "blob";
                uint8_t *dbuff = (uint8_t*)read_input_file(filename.c_str());
                memcpy(data_buff, dbuff, 32*1024*1024);
            }

            return req.respond(0);
        };

    bool flag = true;
    uint8_t* buff;
    {
        MeasureExecutionTime m("memory_allocate");
        buff = (uint8_t*)malloc((32*1024*1024)+1);
    }
    std::vector<std::pair<void*,std::size_t>> segments(1);
    tl::bulk bulk;

    int8_t key = 1;
    std::function<void(const tl::request&)> get_next = 
        [&mid, &svr_addr, &engine, &do_rdma, &data_buff, &key, &buff, &flag, &segments, &bulk](const tl::request &req) {            
            if (flag) {
                {
                    MeasureExecutionTime m("server_expose");
                    segments[0].first = buff;
                    segments[0].second = (32*1024*1024)+1;
                    bulk = engine.expose(segments, tl::bulk_mode::read_write);
                }
                flag = false;
            }
            
            {
                MeasureExecutionTime m("memcpy");
                memcpy(buff, data_buff, 32*1024*1024);
                memcpy(buff+32*1024*1024, &key, 1);
                if (key == 1) {
                    key = 0;
                } else {
                    key = 1;
                }
            }
            {
                MeasureExecutionTime m("calc_crc");
                uint32_t crc = calc_crc(std::string((char*)buff, (32*1024*1024)+1));
                std::cout << "crc: " << crc << std::endl;
            }
            do_rdma.on(req.get_endpoint())(bulk);
            return req.respond(0);
        };
    
    engine.define("scan", scan);
    engine.define("get_next", get_next);

    std::cout << "Server running at address " << engine.self() << std::endl;    
    engine.wait_for_finalize();        
};