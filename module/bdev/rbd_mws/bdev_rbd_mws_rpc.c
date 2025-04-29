#include "bdev_rbd_mws.h"

#include "spdk/jsonrpc.h"
#include "spdk/util.h"

/*
 * bdev_rbd_mws_register_cluster
 */

struct rpc_bdev_rbd_mws_register_cluster_req {
	char *name;
	char *config_file;
	char *core_mask;
	char *user;
};

static void
free_rpc_bdev_rbd_mws_register_cluster_req(struct rpc_bdev_rbd_mws_register_cluster_req *req)
{
	free(req->name);
	free(req->config_file);
	free(req->core_mask);
	free(req->user);
}

static const struct spdk_json_object_decoder rpc_bdev_rbd_mws_register_cluster_decoders[] = {
	{"name", offsetof(struct rpc_bdev_rbd_mws_register_cluster_req, name), spdk_json_decode_string, false},
	{"config_file", offsetof(struct rpc_bdev_rbd_mws_register_cluster_req, config_file), spdk_json_decode_string, false},
	{"core_mask", offsetof(struct rpc_bdev_rbd_mws_register_cluster_req, core_mask), spdk_json_decode_string, true},
	{"user", offsetof(struct rpc_bdev_rbd_mws_register_cluster_req, user), spdk_json_decode_string, true},
};

static void
rpc_bdev_rbd_mws_register_cluster(struct spdk_jsonrpc_request *request,
				  const struct spdk_json_val *params)
{
	struct rpc_bdev_rbd_mws_register_cluster_req req = {};
	struct bdev_rbd_mws_cluster_info info;
	int rc;

	if (spdk_json_decode_object(params, rpc_bdev_rbd_mws_register_cluster_decoders,
				    SPDK_COUNTOF(rpc_bdev_rbd_mws_register_cluster_decoders),
				    &req)) {
		rc = -EINVAL;
		spdk_jsonrpc_send_error_response(request, rc, bdev_rbd_mws_get_error_msg(-rc));
		goto cleanup;
	}

	info.name = req.name;
	info.config_file = req.config_file;
	info.core_mask = req.core_mask;
	info.user = req.user;

	rc = bdev_rbd_mws_register_cluster(&info);
	if (rc < 0) {
		spdk_jsonrpc_send_error_response(request, rc, bdev_rbd_mws_get_error_msg(-rc));
	} else {
		spdk_jsonrpc_send_bool_response(request, true);
	}

cleanup:
	free_rpc_bdev_rbd_mws_register_cluster_req(&req);
}
SPDK_RPC_REGISTER("bdev_rbd_mws_register_cluster", rpc_bdev_rbd_mws_register_cluster,
		  SPDK_RPC_RUNTIME)

/*
 * bdev_rbd_mws_unregister_cluster
 */

struct rpc_bdev_rbd_mws_unregister_cluster_req {
	char *name;
};

static void
free_rpc_bdev_rbd_mws_unregister_cluster_req(struct rpc_bdev_rbd_mws_unregister_cluster_req *req)
{
	free(req->name);
}

static const struct spdk_json_object_decoder rpc_bdev_rbd_mws_unregister_cluster_decoders[] = {
	{"name", offsetof(struct rpc_bdev_rbd_mws_unregister_cluster_req, name), spdk_json_decode_string, false},
};

static void
rpc_bdev_rbd_mws_unregister_cluster(struct spdk_jsonrpc_request *request,
				    const struct spdk_json_val *params)
{
	struct rpc_bdev_rbd_mws_unregister_cluster_req req = {};
	int rc;

	if (spdk_json_decode_object(params, rpc_bdev_rbd_mws_unregister_cluster_decoders,
				    SPDK_COUNTOF(rpc_bdev_rbd_mws_unregister_cluster_decoders),
				    &req)) {
		rc = -EINVAL;
		spdk_jsonrpc_send_error_response(request, rc, bdev_rbd_mws_get_error_msg(-rc));
		goto cleanup;
	}


	rc = bdev_rbd_mws_unregister_cluster(req.name);
	if (rc < 0) {
		spdk_jsonrpc_send_error_response(request, rc, bdev_rbd_mws_get_error_msg(-rc));
	} else {
		spdk_jsonrpc_send_bool_response(request, true);
	}

cleanup:
	free_rpc_bdev_rbd_mws_unregister_cluster_req(&req);
}
SPDK_RPC_REGISTER("bdev_rbd_mws_unregister_cluster", rpc_bdev_rbd_mws_unregister_cluster,
		  SPDK_RPC_RUNTIME)

/*
 * bdev_rbd_mws_get_clusters
 */

struct rpc_bdev_rbd_mws_get_clusters_req {
	char *name;
};

static void
free_rpc_bdev_rbd_mws_get_clusters_req(struct rpc_bdev_rbd_mws_get_clusters_req *req)
{
	free(req->name);
}

static const struct spdk_json_object_decoder rpc_bdev_rbd_mws_get_clusters_decoders[] = {
	{"name", offsetof(struct rpc_bdev_rbd_mws_get_clusters_req, name), spdk_json_decode_string, true},
};

static void
rpc_bdev_rbd_mws_get_clusters(struct spdk_jsonrpc_request *request,
			      const struct spdk_json_val *params)
{
	struct rpc_bdev_rbd_mws_get_clusters_req req = {};
	struct spdk_json_write_ctx *w;
	int rc;

	if (params != NULL && spdk_json_decode_object(params, rpc_bdev_rbd_mws_get_clusters_decoders,
			SPDK_COUNTOF(rpc_bdev_rbd_mws_get_clusters_decoders),
			&req)) {
		rc = -EINVAL;
		spdk_jsonrpc_send_error_response(request, rc, bdev_rbd_mws_get_error_msg(-rc));
		goto cleanup;
	}

	if (req.name != NULL && !bdev_rbd_mws_has_cluster(req.name)) {
		rc = -BDEV_RBD_MWS_E_NO_SUCH_CLUSTER;
		spdk_jsonrpc_send_error_response(request, rc, bdev_rbd_mws_get_error_msg(-rc));
		goto cleanup;
	}

	w = spdk_jsonrpc_begin_result(request);
	(void) bdev_rbd_mws_dump_clusters(w, req.name);
	spdk_jsonrpc_end_result(request, w);

cleanup:
	free_rpc_bdev_rbd_mws_get_clusters_req(&req);
}
SPDK_RPC_REGISTER("bdev_rbd_mws_get_clusters", rpc_bdev_rbd_mws_get_clusters, SPDK_RPC_RUNTIME)

/*
 * bdev_rbd_mws_create
 */

struct rpc_bdev_rbd_mws_create_req {
	char *cluster_name;
	char *pool_name;
	char *rbd_name;
	char *name;
	uint32_t block_size;
};

static void
free_rpc_bdev_rbd_mws_create_req(struct rpc_bdev_rbd_mws_create_req *req)
{
	free(req->cluster_name);
	free(req->pool_name);
	free(req->rbd_name);
	free(req->name);
}

static const struct spdk_json_object_decoder rpc_bdev_rbd_mws_create_decoders[] = {
	{"cluster_name", offsetof(struct rpc_bdev_rbd_mws_create_req, cluster_name), spdk_json_decode_string, false},
	{"pool_name", offsetof(struct rpc_bdev_rbd_mws_create_req, pool_name), spdk_json_decode_string, false},
	{"rbd_name", offsetof(struct rpc_bdev_rbd_mws_create_req, rbd_name), spdk_json_decode_string, false},
	{"name", offsetof(struct rpc_bdev_rbd_mws_create_req, name), spdk_json_decode_string, false},
	{"block_size", offsetof(struct rpc_bdev_rbd_mws_create_req, block_size), spdk_json_decode_uint32, false},
};

static void
rpc_bdev_rbd_mws_create(struct spdk_jsonrpc_request *request,
			const struct spdk_json_val *params)
{
	struct rpc_bdev_rbd_mws_create_req req = {};
	struct bdev_rbd_mws_info info;
	int rc;

	if (spdk_json_decode_object(params, rpc_bdev_rbd_mws_create_decoders,
				    SPDK_COUNTOF(rpc_bdev_rbd_mws_create_decoders),
				    &req)) {
		rc = -EINVAL;
		spdk_jsonrpc_send_error_response(request, rc, bdev_rbd_mws_get_error_msg(-rc));
		goto cleanup;
	}

	info.name = req.name;
	info.cluster_name = req.cluster_name;
	info.pool_name = req.pool_name;
	info.rbd_name = req.rbd_name;
	info.block_size = req.block_size;

	rc = bdev_rbd_mws_create(&info);
	if (rc < 0) {
		spdk_jsonrpc_send_error_response(request, rc, bdev_rbd_mws_get_error_msg(-rc));
	} else {
		spdk_jsonrpc_send_bool_response(request, true);
	}

cleanup:
	free_rpc_bdev_rbd_mws_create_req(&req);
}
SPDK_RPC_REGISTER("bdev_rbd_mws_create", rpc_bdev_rbd_mws_create, SPDK_RPC_RUNTIME)

/*
 * bdev_rbd_mws_delete
 */

struct rpc_bdev_rbd_mws_delete_req {
	char *name;
};

static void
free_rpc_bdev_rbd_mws_delete_req(struct rpc_bdev_rbd_mws_delete_req *req)
{
	free(req->name);
}

static const struct spdk_json_object_decoder rpc_bdev_rbd_mws_delete_decoders[] = {
	{"name", offsetof(struct rpc_bdev_rbd_mws_delete_req, name), spdk_json_decode_string, false},
};

static void
rpc_bdev_rbd_mws_delete(struct spdk_jsonrpc_request *request,
			const struct spdk_json_val *params)
{
	struct rpc_bdev_rbd_mws_delete_req req = {};
	int rc;

	if (spdk_json_decode_object(params, rpc_bdev_rbd_mws_delete_decoders,
				    SPDK_COUNTOF(rpc_bdev_rbd_mws_delete_decoders),
				    &req)) {
		rc = -EINVAL;
		spdk_jsonrpc_send_error_response(request, rc, bdev_rbd_mws_get_error_msg(-rc));
		goto cleanup;
	}

	rc = bdev_rbd_mws_delete(req.name);
	if (rc < 0) {
		spdk_jsonrpc_send_error_response(request, rc, bdev_rbd_mws_get_error_msg(-rc));
	} else {
		spdk_jsonrpc_send_bool_response(request, true);
	}

cleanup:
	free_rpc_bdev_rbd_mws_delete_req(&req);
}
SPDK_RPC_REGISTER("bdev_rbd_mws_delete", rpc_bdev_rbd_mws_delete, SPDK_RPC_RUNTIME)

/*
 * bdev_rbd_mws_get_bdevs
 */

struct rpc_bdev_rbd_mws_get_bdevs_req {
	char *name;
};

static void
free_rpc_bdev_rbd_mws_get_bdevs_req(struct rpc_bdev_rbd_mws_get_bdevs_req *req)
{
	free(req->name);
}

static const struct spdk_json_object_decoder rpc_bdev_rbd_mws_get_bdevs_decoders[] = {
	{"name", offsetof(struct rpc_bdev_rbd_mws_get_bdevs_req, name), spdk_json_decode_string, false},
};

static void
rpc_bdev_rbd_mws_get_bdevs(struct spdk_jsonrpc_request *request,
			   const struct spdk_json_val *params)
{
	struct rpc_bdev_rbd_mws_get_bdevs_req req = {};
	struct spdk_json_write_ctx *w;
	int rc;

	if (params != NULL && spdk_json_decode_object(params, rpc_bdev_rbd_mws_get_bdevs_decoders,
			SPDK_COUNTOF(rpc_bdev_rbd_mws_get_bdevs_decoders),
			&req)) {
		rc = -EINVAL;
		spdk_jsonrpc_send_error_response(request, rc, bdev_rbd_mws_get_error_msg(-rc));
		goto cleanup;
	}

	if (req.name != NULL && !bdev_rbd_mws_has_bdev(req.name)) {
		rc = -BDEV_RBD_MWS_E_NO_SUCH_BDEV;
		spdk_jsonrpc_send_error_response(request, rc, bdev_rbd_mws_get_error_msg(-rc));
		goto cleanup;
	}

	w = spdk_jsonrpc_begin_result(request);
	(void) bdev_rbd_mws_dump_bdevs_extra(w, req.name, true);
	spdk_jsonrpc_end_result(request, w);

cleanup:
	free_rpc_bdev_rbd_mws_get_bdevs_req(&req);
}
SPDK_RPC_REGISTER("bdev_rbd_mws_get_bdevs", rpc_bdev_rbd_mws_get_bdevs, SPDK_RPC_RUNTIME)

/*
 * bdev_rbd_mws_get_rados_pool_stat
 */

struct rpc_bdev_rbd_mws_get_rados_pool_stat_req {
	char *cluster_name;
	char *pool_name;
};

static void
free_rpc_bdev_rbd_mws_get_rados_pool_stat_req(struct rpc_bdev_rbd_mws_get_rados_pool_stat_req *req)
{
	free(req->cluster_name);
	free(req->pool_name);
}

static const struct spdk_json_object_decoder rpc_bdev_rbd_mws_get_rados_pool_stat_decoders[] = {
	{"cluster_name", offsetof(struct rpc_bdev_rbd_mws_get_rados_pool_stat_req, cluster_name), spdk_json_decode_string, false},
	{"pool_name", offsetof(struct rpc_bdev_rbd_mws_get_rados_pool_stat_req, pool_name), spdk_json_decode_string, false},
};

static void
rpc_bdev_rbd_mws_get_rados_pool_stat(struct spdk_jsonrpc_request *request,
				     const struct spdk_json_val *params)
{
	struct rpc_bdev_rbd_mws_get_rados_pool_stat_req req = {};
	int rc;

	if (spdk_json_decode_object(params, rpc_bdev_rbd_mws_get_rados_pool_stat_decoders,
				    SPDK_COUNTOF(rpc_bdev_rbd_mws_get_rados_pool_stat_decoders),
				    &req)) {
		rc = -EINVAL;
		spdk_jsonrpc_send_error_response(request, rc, bdev_rbd_mws_get_error_msg(-rc));
		goto cleanup;
	}

	rc = -ENOSYS;
	spdk_jsonrpc_send_error_response(request, rc, bdev_rbd_mws_get_error_msg(-rc));

cleanup:
	free_rpc_bdev_rbd_mws_get_rados_pool_stat_req(&req);
}
SPDK_RPC_REGISTER("bdev_rbd_mws_get_rados_pool_stat", rpc_bdev_rbd_mws_get_rados_pool_stat,
		  SPDK_RPC_RUNTIME)

/*
 * bdev_rbd_mws_get_rados_cluster_stat
 */

struct rpc_bdev_rbd_mws_get_rados_cluster_stat_req {
	char *cluster_name;
};

static void
free_rpc_bdev_rbd_mws_get_rados_cluster_stat_req(struct rpc_bdev_rbd_mws_get_rados_cluster_stat_req
		*req)
{
	free(req->cluster_name);
}

static const struct spdk_json_object_decoder rpc_bdev_rbd_mws_get_rados_cluster_stat_decoders[] = {
	{"cluster_name", offsetof(struct rpc_bdev_rbd_mws_get_rados_cluster_stat_req, cluster_name), spdk_json_decode_string, false},
};

static void
rpc_bdev_rbd_mws_get_rados_cluster_stat(struct spdk_jsonrpc_request *request,
					const struct spdk_json_val *params)
{
	struct rpc_bdev_rbd_mws_get_rados_cluster_stat_req req = {};
	int rc;

	if (spdk_json_decode_object(params, rpc_bdev_rbd_mws_get_rados_cluster_stat_decoders,
				    SPDK_COUNTOF(rpc_bdev_rbd_mws_get_rados_cluster_stat_decoders),
				    &req)) {
		rc = -EINVAL;
		spdk_jsonrpc_send_error_response(request, rc, bdev_rbd_mws_get_error_msg(-rc));
		goto cleanup;
	}

	rc = -ENOSYS;
	spdk_jsonrpc_send_error_response(request, rc, bdev_rbd_mws_get_error_msg(-rc));

cleanup:
	free_rpc_bdev_rbd_mws_get_rados_cluster_stat_req(&req);
}
SPDK_RPC_REGISTER("bdev_rbd_mws_get_rados_cluster_stat", rpc_bdev_rbd_mws_get_rados_cluster_stat,
		  SPDK_RPC_RUNTIME)
