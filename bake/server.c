#include <bake-client.h>

int main(int argc, char **argv) {
    char *svr_addr_str;
    hg_addr_t svr_addr;
    bake_client_t bcl;
    bake_provider_handle_t bph;
    uint8_t mplex_id;
    uint32_t target_number;
    bake_region_id_t rid;

    margo_instance_id mid = margo_init("tcp", MARGO_SERVER_MODE, 0, -1);
    margo_addr_lookup(mid, svr_addr_str, &svr_addr);

    bake_client_init(mid, &bcl);
	/* Creates the provider handle */
	bake_provider_handle_create(bcl, svr_addr, mplex_id, &bph);
	/* Asks the provider for up to target_number target ids */
	uint32_t num_targets = 0;
	bti = calloc(num_targets, sizeof(*bti));
	bake_probe(bph, target_number, bti, &num_targets);
	if(num_targets < target_number) {
		fprintf(stderr, "Error: provider has only %d storage targets\n", num_targets);
	}
	/* Create a region */
	size_t size = ...; // size of the region to create
	bake_create(bph, bti[target_number-1], size, &rid);
	/* Write data into the region at offset 0 */
	char* buf = ...;
	bake_write(bph, rid, 0, buf, size);
	/* Make all modifications persistent */
	bake_persist(bph, rid);
	/* Release provider handle */
	bake_provider_handle_release(bph);
	/* Release BAKE client */
	bake_client_finalize(bcl);
	/* Cleanup Margo resources */
	margo_addr_free(mid, svr_addr);
	margo_finalize(mid);
	return 0;
}
