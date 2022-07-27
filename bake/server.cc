#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <stdlib.h>
#include <mercury.h>
#include <abt.h>
#include <margo.h>
#include <memory>
#include <iostream>

#include <bake-client.hpp>

static char* read_input_file(const char* filename);

namespace bk = bake;

int main(int argc, char* argv[]) {

    char                   cli_addr_prefix[64] = {0};


    char* input_file  = argv[1];
    char *bake_svr_addr_str = argv[2];
    char *test_str = read_input_file(input_file);

    /* initialize Margo using the transport portion of the server
     * address (i.e., the part before the first : character if present)
    //  */
    // for (int i = 0; (i < 63 && bake_svr_addr_str[i] != '\0'
    //              && bake_svr_addr_str[i] != ':');
    //      i++)
    //     cli_addr_prefix[i] = bake_svr_addr_str[i];

    margo_instance_id mid = margo_init("verbs", MARGO_SERVER_MODE, 0, 0);
    if (mid == MARGO_INSTANCE_NULL) {
        std::cerr << "Error: margo_init()\n";
        return -1;
    }
    
    hg_addr_t svr_addr;
    hg_return_t hret = margo_addr_lookup(mid, bake_svr_addr_str, &svr_addr);
    if (hret != HG_SUCCESS) {
        std::cerr << "Error: margo_addr_lookup()\n";
        margo_finalize(mid);
        return -1;
    }

    bk::client bcl(mid);
    bk::provider_handle bph(bcl, svr_addr, 1);
    bph.set_eager_limit(0);

    bk::target tid = bcl.probe(bph, 1)[0];

    /**** write phase ****/
    uint64_t buf_size = strlen(test_str) + 1;

    auto region = bcl.create_write_persist(bph, tid, test_str, buf_size);


    /**** read-back phase ****/

    void *buf = (void*)malloc(buf_size);
    memset(buf, 0, buf_size);

    bcl.read(bph, tid, region, 0, buf, buf_size);

    /* check to make sure we get back the string we expect */
    if (strcmp((char*)buf, test_str) != 0) {
        std::cerr << "Error: unexpected buffer contents returned from BAKE\n";
        delete buf;
        margo_addr_free(mid, svr_addr);
        margo_finalize(mid);
        return -1;
    }

    std::string s((char*)buf, buf_size);
    std::cout << s << std::endl;

    char* zc_str = (char*)bcl.get_data(bph, tid, region);
    std::string zc_s((char*)zc_str, buf_size);
    std::cout << zc_s << std::endl;


    delete buf;
    delete test_str;
    margo_addr_free(mid, svr_addr);
    margo_finalize(mid);
    return ret;
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