/**
 * libfabric shim — wraps static inline functions from libfabric headers
 * into real exported symbols that Rust FFI can link against.
 *
 * fi_mr_reg, fi_mr_desc, fi_mr_key, fi_close are all static inlines
 * that call through vtable pointers (domain->mr->reg, etc).
 *
 * Build: gcc -shared -fPIC -o libfabric_shim.so fabric_shim.c \
 *            -I/opt/amazon/efa/include -L/opt/amazon/efa/lib64 -lfabric
 */

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_rma.h>

/* fi_mr_reg — the main one we need for benchmarking registration cost */
int shim_fi_mr_reg(struct fid_domain *domain, const void *buf, size_t len,
                   uint64_t access, uint64_t offset, uint64_t requested_key,
                   uint64_t flags, struct fid_mr **mr, void *context) {
    return fi_mr_reg(domain, buf, len, access, offset, requested_key, flags, mr, context);
}

/* fi_mr_desc */
void *shim_fi_mr_desc(struct fid_mr *mr) {
    return fi_mr_desc(mr);
}

/* fi_mr_key */
uint64_t shim_fi_mr_key(struct fid_mr *mr) {
    return fi_mr_key(mr);
}

/* fi_close */
int shim_fi_close(struct fid *fid) {
    return fi_close(fid);
}

/* fi_getinfo */
int shim_fi_getinfo(uint32_t version, const char *node, const char *service,
                    uint64_t flags, const struct fi_info *hints,
                    struct fi_info **info) {
    return fi_getinfo(version, node, service, flags, hints, info);
}

/* fi_freeinfo */
void shim_fi_freeinfo(struct fi_info *info) {
    fi_freeinfo(info);
}

/* fi_fabric — takes fi_info and extracts fabric_attr internally */
int shim_fi_fabric(struct fi_info *info, struct fid_fabric **fabric,
                   void *context) {
    return fi_fabric(info->fabric_attr, fabric, context);
}

/* fi_domain */
int shim_fi_domain(struct fid_fabric *fabric, struct fi_info *info,
                   struct fid_domain **domain, void *context) {
    return fi_domain(fabric, info, domain, context);
}

/* fi_version */
uint32_t shim_fi_version(void) {
    return fi_version();
}
