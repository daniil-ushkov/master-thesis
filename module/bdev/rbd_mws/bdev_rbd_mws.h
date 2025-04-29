#ifndef SPDK_BDEV_RBD_MWS_H
#define SPDK_BDEV_RBD_MWS_H

#include "spdk/stdinc.h"
#include "spdk/bdev.h"
#include "spdk/rpc.h"

#include <rados/librados.h>

#define STATE_MSG_MAX 64

enum bdev_rbd_mws_error {
	BDEV_RBD_MWS_E_CLUSTER_ALREADY_EXISTS = 256,
	BDEV_RBD_MWS_E_BDEV_ALREADY_EXISTS,
	BDEV_RBD_MWS_E_NO_SUCH_CLUSTER,
	BDEV_RBD_MWS_E_NO_SUCH_BDEV,
	BDEV_RBD_MWS_E_CLUSTER_BUSY,
};

const char *bdev_rbd_mws_get_error_msg(int rc);

struct bdev_rbd_mws_cluster_info {
	char *name;
	char *config_file;
	char *core_mask;
	char *user;
};

int bdev_rbd_mws_register_cluster(const struct bdev_rbd_mws_cluster_info *info);

int bdev_rbd_mws_unregister_cluster(const char *name);

bool bdev_rbd_mws_has_cluster(const char *name);

int bdev_rbd_mws_dump_clusters(struct spdk_json_write_ctx *w, const char *name);

struct bdev_rbd_mws_info {
	char *name;
	char *cluster_name;
	char *pool_name;
	char *rbd_name;
	uint32_t block_size;
};

int bdev_rbd_mws_create(const struct bdev_rbd_mws_info *info);

int bdev_rbd_mws_delete(const char *name);

bool bdev_rbd_mws_has_bdev(const char *name);

int bdev_rbd_mws_dump_bdevs(struct spdk_json_write_ctx *w, const char *name);

int bdev_rbd_mws_dump_bdevs_extra(struct spdk_json_write_ctx *w, const char *name, bool dump_extra);

typedef void (*bdev_rbd_mws_get_rados_pool_stat_cb)(void *, int, struct rados_pool_stat_t *);

void bdev_rbd_mws_get_rados_pool_stat(
	const char *cluster_name,
	const char *pool_name,
	bdev_rbd_mws_get_rados_pool_stat_cb cb,
	void *cb_arg);

typedef void (*bdev_rbd_mws_get_rados_cluster_stat_cb)(void *, int, struct rados_cluster_stat_t *);

void bdev_rbd_mws_get_rados_cluster_stat(
	const char *cluster_name,
	bdev_rbd_mws_get_rados_cluster_stat_cb cb,
	void *cb_arg);

#endif /* SPDK_BDEV_RBD_MWS_H */
