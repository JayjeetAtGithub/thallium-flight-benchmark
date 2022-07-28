#include <memory>
#include <iostream>

#include <bake-client.hpp>
#include <bake-server.hpp>

static char* read_input_file(const char* filename);

namespace bk = bake;

int main(int argc, char* argv[]) {    
    margo_instance_id mid = margo_init("verbs://ibp130s0", MARGO_SERVER_MODE, 0, 0);
    if (mid == MARGO_INSTANCE_NULL) {
        std::cerr << "Error: margo_init()\n";
        return -1;
    }
    
    hg_addr_t svr_addr;
    hg_return_t hret = margo_addr_self(mid, &svr_addr);
    if (hret != HG_SUCCESS) {
        std::cerr << "Error: margo_addr_lookup()\n";
        margo_finalize(mid);
        return -1;
    }

    char *config = read_input_file("bake/config.json");

    // setup provider
    bk::provider *p = bk::provider::create(
        mid, 0, ABT_POOL_NULL, std::string(config, strlen(config) + 1), ABT_IO_INSTANCE_NULL, NULL, NULL);

    std::cout << "successfully setup provider" << std::endl;
    std::string cfg = p->get_config();
    std::cout << cfg << std::endl;

    bk::client bcl(mid);
    bk::provider_handle bph(bcl, svr_addr, 0);
    bph.set_eager_limit(0);
    bk::target tid = p->list_targets()[0];

    // try zero copy access
    bake::region rid("AAAAAO0B3hifXASe0Ag8AAAAAAA=");
    char* zero_copy_pointer = (char*)bcl.get_data(bph, tid, );
    std::string str((char*)zero_copy_pointer, 5);
    std::cout << str << std::endl;

    // free resources
    free(zero_copy_pointer);
    margo_addr_free(mid, svr_addr);
    margo_finalize(mid);
    return 0;
}

static char* read_input_file(const char* filename)
{
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