#include <sys/stat.h>
#include <memory>
#include <fstream>
#include <iostream>

#include <bake-client.hpp>
#include <bake-server.hpp>

#include <yokan/cxx/server.hpp>
#include <yokan/cxx/admin.hpp>
#include <yokan/cxx/client.hpp>

static char* read_input_file(const char* filename);

namespace bk = bake;
namespace yk = yokan;

int main(int argc, char* argv[]) {    
    // read input file
    const char* filename = argv[1];
    struct stat file_st;
    stat(filename, &file_st);
    uint8_t *buffer = new uint8_t[file_st.st_size];
    std::ifstream fin(filename, std::ios::in | std::ios::binary );
    fin.read((char*)buffer, file_st.st_size);

    // initialize margo instance
    margo_instance_id mid = margo_init("verbs://ibp130s0", MARGO_SERVER_MODE, 0, 0);
    if (mid == MARGO_INSTANCE_NULL) {
        std::cerr << "Error: margo_init()\n";
        return -1;
    }
    
    // get the margo address
    hg_addr_t svr_addr;
    hg_return_t hret = margo_addr_self(mid, &svr_addr);
    if (hret != HG_SUCCESS) {
        std::cerr << "Error: margo_addr_lookup()\n";
        margo_finalize(mid);
        return -1;
    }

    // read the bake config file
    char *config = read_input_file("bake/config.json");

    // setup the bake provider
    uint64_t config_size = strlen(config) + 1;
    bk::provider *p = bk::provider::create(
        mid, 0, ABT_POOL_NULL, std::string(config, config_size), ABT_IO_INSTANCE_NULL, NULL, NULL);

    // display the bake config
    std::string cfg = p->get_config();
    std::cout << cfg << std::endl;

    // initiate the bake client, provider, and get the target
    bk::client bcl(mid);
    bk::provider_handle bph(bcl, svr_addr, 0);
    bph.set_eager_limit(0);
    bk::target tid = p->list_targets()[0];

    // start yokan provider, create a database, and initialize the db handle
    char *yokan_config = read_input_file("yokan_config.json");
    yk::Provider yp(mid, 0, "ABCD", yokan_config, ABT_POOL_NULL, nullptr);
    yk::Client ycl(mid);
    yk::Admin admin(mid);
    yk_database_id_t db_id = admin.openDatabase(svr_addr, 0, "ABCD", "rocksdb", yokan_config);
    yk::Database db(ycl.handle(), svr_addr, 0, db_id);

    // write the data to bake
    uint64_t buffer_size = file_st.st_size;
    std::cout << "Wrote: " << buffer_size << " bytes" << std::endl;
    bk::region rid = bcl.create_write_persist(bph, tid, buffer, buffer_size);
    std::string rid_str = std::string(rid);

    // write file metadata to yokan
    db.put((void*)filename, strlen(filename) + 1, (void*)rid_str.c_str(), rid_str.length());
    
    // free resources
    free(buffer);
    margo_addr_free(mid, svr_addr);
    margo_finalize(mid);
    return 0;
}

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
