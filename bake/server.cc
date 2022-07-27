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

    int                    i;
    char                   cli_addr_prefix[64] = {0};
    char*                  bake_svr_addr_str;
    margo_instance_id      mid;
    hg_addr_t              svr_addr;
    uint8_t                mplex_id;
    char*                  test_str = NULL;
    hg_return_t            hret;
    int                    ret;

    if (argc != 4) {
        fprintf(stderr,
                "Usage: create-write-persist-test <input file> <bake server "
                "addr> <mplex id>\n");
        fprintf(
            stderr,
            "  Example: ./create-write-persist-test tcp://localhost:1234 1\n");
        return (-1);
    }
    char* input_file  = argv[1];
    bake_svr_addr_str = argv[2];
    mplex_id          = atoi(argv[3]);

    test_str = read_input_file(input_file);

    /* initialize Margo using the transport portion of the server
     * address (i.e., the part before the first : character if present)
     */
    for (i = 0; (i < 63 && bake_svr_addr_str[i] != '\0'
                 && bake_svr_addr_str[i] != ':');
         i++)
        cli_addr_prefix[i] = bake_svr_addr_str[i];

    /* start margo */
    mid = margo_init(cli_addr_prefix, MARGO_SERVER_MODE, 0, 0);
    if (mid == MARGO_INSTANCE_NULL) {
        fprintf(stderr, "Error: margo_init()\n");
        return (-1);
    }
    
    /* look up the BAKE server address */
    hret = margo_addr_lookup(mid, bake_svr_addr_str, &svr_addr);
    if (hret != HG_SUCCESS) {
        std::cerr << "Error: margo_addr_lookup() :" << ret << "\n";
        margo_finalize(mid);
        return (-1);
    }

    bk::client bcl(mid);
    bk::provider_handle bph(bcl, svr_addr, 1);
    bph.set_eager_limit(0);

    bk::target tid = bcl.probe(bph, 1)[0];

    /**** write phase ****/
    uint64_t buf_size = strlen(test_str) + 1;

    auto region = bcl.create_write_persist(bph, tid, test_str, buf_size);


    // /**** read-back phase ****/

    void *buf = (void*)malloc(buf_size);
    memset(buf, 0, buf_size);

    bcl.read(bph, tid, region, 0, buf, buf_size);

    /* check to make sure we get back the string we expect */
    if (strcmp((char*)buf, test_str) != 0) {
        fprintf(stderr,
                "Error: unexpected buffer contents returned from BAKE\n");
        free(buf);
        margo_addr_free(mid, svr_addr);
        margo_finalize(mid);
        return (-1);
    }

    // /* get a raw pointer to the data */
    // void *ptr;
    // ret = bake_get_data(bph, bti, the_rid, &ptr);
    
    // fprintf(stdout, "coming till here 2\n");
    // if (ret != 0) {
    //     bake_perror("Error: bake_get_data()", ret);
    //     bake_provider_handle_release(bph);
    //     margo_addr_free(mid, svr_addr);
    //     bake_client_finalize(bcl);
    //     margo_finalize(mid);
    //     return (-1);
    // }


    // /* shutdown the server */
    // // ret = bake_shutdown_service(bcl, svr_addr);

    // /**** cleanup ****/

    free(buf);
    free(test_str);
    margo_addr_free(mid, svr_addr);
    margo_finalize(mid);
    return (ret);
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