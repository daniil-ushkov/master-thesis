#include "bdev_rbd_mws.h"

#include "spdk/json.h"
#include "spdk/log.h"
#include "spdk/stdinc.h"
#include "spdk/env.h"
#include "spdk/queue_extras.h"
#include "spdk/bdev_module.h"
#include "spdk/string.h"
#include "spdk/thread.h"

#include <sys/queue.h>

#include <rados/librados.h>
#include <rbd/librbd.h>

#define RBD_MWS_RADOS_CONNECT_TIMEOUT_S "30"

#define STR_MAX 1000

#define POLLER_PERIOD_US 500000

#define CREATE_BACKOFF_S 30

#define CREATE_RETRY_LIMIT 10

/* types */

enum state {
	/* terminal states */
	DELETED = 0,
	CREATED,

	TERMINAL_STATES_END,
	FAILURE_STATES_BEGIN = TERMINAL_STATES_END,

	/* failure states */
	CREATE_FAILED = FAILURE_STATES_BEGIN,

	FAILURE_STATES_END,
	PROCESSING_STATES_BEGIN = FAILURE_STATES_END,

	/* processing states */
	OPENING = PROCESSING_STATES_BEGIN,
	BDEV_UNREGISTERING,
	IO_DEVICE_UNREGISTERING,
	FLUSHING,
	CLOSING,

	PROCESSING_STATES_END
};

struct cluster {
	struct bdev_rbd_mws_cluster_info *info;
	TAILQ_ENTRY(cluster) link;
	rados_t rados;
	int ref_count;
};

struct bdev {
	TAILQ_ENTRY(bdev) link;
	struct bdev_rbd_mws_info *info;
	struct cluster *cluster;
	rados_ioctx_t ioctx;
	rbd_image_info_t image_info;
	rbd_image_t image;
	enum state expected_state;
	enum state state;
	uint64_t create_failed_timestamp_s;
	struct spdk_bdev bdev;
	rbd_completion_t c;
	bool create_failed;
	const char *last_err_msg;
	int last_err_code;
	bool async_destruct;
	uint64_t watch_handle;
	uint64_t remaining_retry_count;
};

struct completion_ctx {
	void (*cb)(void *);
	void *arg;
};

struct bdev_io {
	struct spdk_thread *submit_td;
	enum spdk_bdev_io_status status;
	rbd_completion_t c;
};

struct io_channel {
};

/* static function declarations */

static int set_cpuset(struct spdk_cpuset *set);
static int do_register_cluster(void *ctx);

static int module_init(void);
static void module_fini(void);
static int get_ctx_size(void);

static void update_bdev_size(void *ctx);
static void watch_cb(void *ctx);
static void check_open_rc_and_register(void *arg);
static void open_bdev(struct bdev *bdev);

static void fail_create(struct bdev *bdev, const char *msg, int rc);
static void finish_delete(void *ctx);
static void check_close_rc_and_free_bdev(void *ctx);
static void close_bdev(void *ctx);
static void check_flush_rc_and_close_bdev(void *ctx);
static void flush_bdev(void *ctx);
static void unregister_io_device(void *ctx);
static void unregister_bdev(struct bdev *bdev);

static int poll_bdevs(void *ctx);

static struct cluster *alloc_cluster(void);
static void free_cluster(struct cluster *cluster);
static struct bdev_rbd_mws_cluster_info *dup_cluster_info(const struct bdev_rbd_mws_cluster_info
		*info);
static void free_cluster_info(struct bdev_rbd_mws_cluster_info *info);
static int check_cluster_info(const struct bdev_rbd_mws_cluster_info *info);
static struct cluster *get_cluster(const char *name);

static struct bdev *alloc_bdev(void);
static void free_bdev(struct bdev *bdev);
static struct bdev_rbd_mws_info *dup_bdev_info(const struct bdev_rbd_mws_info *info);
static void free_bdev_info(struct bdev_rbd_mws_info *info);
static struct bdev *get_bdev(const char *name);

static void completion_cb(rbd_completion_t c, void *arg);
static int create_completion(void (*cb)(void *), void *arg, rbd_completion_t *c);

static int destruct(void *ctx);
static void get_buf_cb(struct spdk_io_channel *ch, struct spdk_bdev_io *spdk_bdev_io, bool success);
static void bdev_io_complete(rbd_completion_t c, void *ctx);
static void bdev_io_submit(void *ctx);
static void bdev_io_set_status_cb(void *ctx);
static void bdev_io_set_status(struct spdk_bdev_io *spdk_bdev_io, enum spdk_bdev_io_status status);
static void submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *spdk_bdev_io);
static bool io_type_supported(void *ctx, enum spdk_bdev_io_type io_type);
static struct spdk_io_channel *get_io_channel(void *ctx);
static int dump_info_json(void *ctx, struct spdk_json_write_ctx *w);
static void dump_device_stat_json(void *ctx, struct spdk_json_write_ctx *w);

static int io_channel_create_cb(void *io_device, void *ctx_buf);
static void io_channel_destroy_cb(void *io_device, void *ctx_buf);
static void dump_bdev(struct spdk_json_write_ctx *w, struct bdev *bdev, bool dump_extra);
static void dump_cluster(struct spdk_json_write_ctx *w, struct cluster *cluster);

/* globals */

static const char *state_name[] = {
	[DELETED] = "DELETED",
	[CREATED] = "CREATED",
	[CREATE_FAILED] = "CREATE_FAILED",
	[OPENING] = "OPENING",
	[BDEV_UNREGISTERING] = "BDEV_UNREGISTERING",
	[IO_DEVICE_UNREGISTERING] = "IO_DEVICE_UNREGISTERING",
	[FLUSHING] = "FLUSHING",
	[CLOSING] = "CLOSING",
};

static struct spdk_bdev_module g_module = {
	.name = "rbd_mws",
	.module_init = module_init,
	.module_fini = module_fini,
	.get_ctx_size = get_ctx_size,
};
SPDK_BDEV_MODULE_REGISTER(rbd, &g_module);

static struct spdk_thread *g_thread = NULL;
static struct spdk_poller *g_poller = NULL;

static TAILQ_HEAD(, cluster) g_clusters = TAILQ_HEAD_INITIALIZER(g_clusters);
static int g_clusters_count = 0;

static TAILQ_HEAD(, bdev) g_bdevs = TAILQ_HEAD_INITIALIZER(g_bdevs);
static int g_bdevs_count = 0;

static struct spdk_bdev_fn_table g_bdev_fn_table = {
	.destruct              = destruct,
	.submit_request        = submit_request,
	.io_type_supported     = io_type_supported,
	.get_io_channel        = get_io_channel,
	.dump_info_json        = dump_info_json,
	.dump_device_stat_json = dump_device_stat_json,
};

/* public functions definitions */

const char *
bdev_rbd_mws_get_error_msg(int rc)
{
	switch (rc) {
	case BDEV_RBD_MWS_E_CLUSTER_ALREADY_EXISTS:
		return "cluster already exists";
	case BDEV_RBD_MWS_E_BDEV_ALREADY_EXISTS:
		return "bdev already exists";
	case BDEV_RBD_MWS_E_NO_SUCH_CLUSTER:
		return "no such cluster";
	case BDEV_RBD_MWS_E_NO_SUCH_BDEV:
		return "no such bdev";
	case BDEV_RBD_MWS_E_CLUSTER_BUSY:
		return "cluster is busy";
	default:
		return spdk_strerror(rc);
	}
}

int
bdev_rbd_mws_register_cluster(const struct bdev_rbd_mws_cluster_info *info)
{
	return spdk_call_unaffinitized1(do_register_cluster, (void *) info);
}

static int
set_cpuset(struct spdk_cpuset *set)
{
#ifdef __linux__
	uint32_t lcore;
	cpu_set_t mask;

	assert(set != NULL);
	CPU_ZERO(&mask);

	/* get the core id on current spdk_cpuset and set to cpu_set_t */
	for (lcore = 0; lcore < SPDK_CPUSET_SIZE; lcore++) {
		if (spdk_cpuset_get_cpu(set, lcore)) {
			CPU_SET(lcore, &mask);
		}
	}

	/* change current thread core mask */
	if (sched_setaffinity(0, sizeof(mask), &mask) < 0) {
		SPDK_ERRLOG("Could not change thread cpu affinity: %d\n", errno);
		return -1;
	}

	return 0;
#else
	SPDK_ERRLOG("SPDK non spdk thread cpumask setup supports only Linux platform now.\n");
	return -ENOTSUP;
#endif
}

static int
do_register_cluster(void *ctx)
{
	const struct bdev_rbd_mws_cluster_info *info = ctx;
	int rc;
	struct cluster *cluster;
	struct spdk_cpuset cpuset = {};

	assert(spdk_get_thread() == g_thread);

	rc = check_cluster_info(info);
	if (rc < 0) {
		return rc;
	}

	cluster = get_cluster(info->name);
	if (cluster != NULL) {
		return -BDEV_RBD_MWS_E_CLUSTER_ALREADY_EXISTS;
	}

	cluster = alloc_cluster();
	if (cluster == NULL) {
		rc = -ENOMEM;
		goto fail;
	}

	cluster->info = dup_cluster_info(info);
	if (cluster->info == NULL) {
		rc = -ENOMEM;
		goto fail;
	}

	if (cluster->info->core_mask != NULL) {
		if (spdk_cpuset_parse(&cpuset, cluster->info->core_mask) < 0) {
			SPDK_ERRLOG("Could not parse core_mask=%s for cluster=%s\n", cluster->info->core_mask,
				    cluster->info->name);
			rc = -EINVAL;
			goto fail;
		}

		if (set_cpuset(&cpuset) < 0) {
			SPDK_ERRLOG("Could not set core_mask=%s for cluster=%s\n", cluster->info->core_mask,
				    cluster->info->name);
			rc = -EINVAL;
			goto fail;
		}
	}

	rc = rados_create(&cluster->rados, cluster->info->user);
	if (rc < 0) {
		goto fail;
	}

	rc = rados_conf_read_file(cluster->rados, cluster->info->config_file);
	if (rc < 0) {
		goto fail_shutdown;
	}

	/* Avoid infinite wait on network issues */
	rc = rados_conf_set(cluster->rados, "client_mount_timeout", RBD_MWS_RADOS_CONNECT_TIMEOUT_S);
	if (rc < 0) {
		goto fail_shutdown;
	}

	rc = rados_connect(cluster->rados);
	if (rc < 0) {
		goto fail_shutdown;
	}

	return 0;

fail_shutdown:
	rados_shutdown(cluster->rados);
fail:
	free_cluster_info(cluster->info);
	free_cluster(cluster);
	return rc;
}

int
bdev_rbd_mws_unregister_cluster(const char *name)
{
	struct cluster *cluster;

	assert(spdk_get_thread() == g_thread);

	cluster = get_cluster(name);
	if (cluster == NULL) {
		return -BDEV_RBD_MWS_E_NO_SUCH_CLUSTER;
	}

	if (cluster->ref_count != 0) {
		return -BDEV_RBD_MWS_E_CLUSTER_BUSY;
	}

	rados_shutdown(cluster->rados);
	free_cluster_info(cluster->info);
	free_cluster(cluster);
	return 0;
}

bool
bdev_rbd_mws_has_cluster(const char *name)
{
	return get_cluster(name) != NULL;
}

int
bdev_rbd_mws_dump_clusters(struct spdk_json_write_ctx *w, const char *name)
{
	struct cluster *cluster;

	if (name != NULL) {
		cluster = get_cluster(name);
		if (cluster == NULL) {
			return -BDEV_RBD_MWS_E_NO_SUCH_CLUSTER;
		}

		spdk_json_write_array_begin(w);
		dump_cluster(w, cluster);
		spdk_json_write_array_end(w);

		return 0;
	}

	spdk_json_write_array_begin(w);
	TAILQ_FOREACH(cluster, &g_clusters, link) {
		dump_cluster(w, cluster);
	}
	spdk_json_write_array_end(w);

	return 0;
}

int
bdev_rbd_mws_create(const struct bdev_rbd_mws_info *info)
{
	struct bdev *bdev;
	int rc;

	assert(spdk_get_thread() == g_thread);

	bdev = get_bdev(info->name);
	if (bdev != NULL) {
		return -BDEV_RBD_MWS_E_BDEV_ALREADY_EXISTS;
	}

	bdev = alloc_bdev();
	if (bdev == NULL) {
		rc = -ENOMEM;
		goto fail;
	}

	bdev->info = dup_bdev_info(info);
	if (bdev->info == NULL) {
		rc = -ENOMEM;
		goto fail;
	}

	bdev->cluster = get_cluster(bdev->info->cluster_name);
	if (bdev->cluster == NULL) {
		rc = -BDEV_RBD_MWS_E_NO_SUCH_CLUSTER;
		goto fail;
	}
	bdev->cluster->ref_count++;

	bdev->expected_state = CREATED;

	bdev->remaining_retry_count = CREATE_RETRY_LIMIT;

	return 0;
fail:
	free_bdev(bdev);
	return rc;
}

int
bdev_rbd_mws_delete(const char *name)
{
	struct bdev *bdev;

	assert(spdk_get_thread() == g_thread);

	bdev = get_bdev(name);
	if (bdev == NULL) {
		return -BDEV_RBD_MWS_E_NO_SUCH_BDEV;
	}

	bdev->expected_state = DELETED;

	return 0;
}

bool
bdev_rbd_mws_has_bdev(const char *name)
{
	return get_bdev(name) != NULL;
}

int
bdev_rbd_mws_dump_bdevs(struct spdk_json_write_ctx *w, const char *name)
{
	return bdev_rbd_mws_dump_bdevs_extra(w, name, false);
}

int
bdev_rbd_mws_dump_bdevs_extra(struct spdk_json_write_ctx *w, const char *name, bool dump_extra)
{
	struct bdev *bdev;

	if (name != NULL) {
		bdev = get_bdev(name);
		if (bdev == NULL) {
			return -BDEV_RBD_MWS_E_NO_SUCH_BDEV;
		}

		spdk_json_write_array_begin(w);
		dump_bdev(w, bdev, dump_extra);
		spdk_json_write_array_end(w);

		return 0;
	}

	spdk_json_write_array_begin(w);
	TAILQ_FOREACH(bdev, &g_bdevs, link) {
		dump_bdev(w, bdev, dump_extra);
	}
	spdk_json_write_array_end(w);

	return 0;
}

void
bdev_rbd_mws_get_rados_pool_stat(
	const char *cluster_name,
	const char *pool_name,
	bdev_rbd_mws_get_rados_pool_stat_cb cb,
	void *cb_arg)
{
	/* TODO(udav): implement */
}

void
bdev_rbd_mws_get_rados_cluster_stat(
	const char *cluster_name,
	bdev_rbd_mws_get_rados_cluster_stat_cb cb,
	void *cb_arg)
{
	/* TODO(udav): implement */
}

/* private functions definitions */

static int
module_init(void)
{
	/* TODO(udav): make sure that it is needed thread (which is used by rpc poller) */
	g_thread = spdk_thread_get_app_thread();
	g_poller = SPDK_POLLER_REGISTER(poll_bdevs, NULL, POLLER_PERIOD_US);
	return 0;
}

static void
module_fini(void)
{
	spdk_poller_unregister(&g_poller);
}

static int
get_ctx_size(void)
{
	return sizeof(struct bdev_io);
}

static void
update_bdev_size(void *ctx)
{
	struct bdev *bdev = ctx;
	uint64_t current_size_in_bytes = 0;
	int rc;

	rc = rbd_get_size(bdev->image, &current_size_in_bytes);
	if (rc < 0) {
		SPDK_ERRLOG("Failed getting rbd size (%s): %d\n", bdev->info->name, rc);
		return;
	}

	rc = spdk_bdev_notify_blockcnt_change(&bdev->bdev, current_size_in_bytes / bdev->bdev.blocklen);
	if (rc < 0) {
		SPDK_ERRLOG("Failed to notify block count change (%s): %d\n", bdev->info->name, rc);
	}
}

static void
watch_cb(void *ctx)
{
	spdk_thread_send_msg(spdk_thread_get_app_thread(), update_bdev_size, ctx);
}

static void
check_open_rc_and_register(void *ctx)
{
	struct bdev *bdev;
	int rc;

	assert(spdk_get_thread() == g_thread);

	bdev = ctx;
	rc = rbd_aio_get_return_value(bdev->c);

	rbd_aio_release(bdev->c);

	if (rc < 0) {
		fail_create(bdev, "Could not open rbd", rc);
		finish_delete(bdev);
		return;
	}

	rc = rbd_stat(bdev->image, &bdev->image_info, sizeof bdev->image_info);
	if (rc < 0) {
		fail_create(bdev, "Could not stat rbd", rc);
		close_bdev(bdev);
		return;
	}

	bdev->bdev.name = strndup(bdev->info->name, STR_MAX);
	if (bdev->bdev.name == NULL) {
		fail_create(bdev, "Could not allocate bdev name", rc);
		close_bdev(bdev);
		return;
	}

	spdk_bdev_set_product_name(&bdev->bdev, "RBD-MWS Disk");
	bdev->bdev.write_cache = 0;
	bdev->bdev.blocklen = bdev->info->block_size;
	bdev->bdev.blockcnt = bdev->image_info.size / bdev->bdev.blocklen;
	bdev->bdev.ctxt = bdev;
	bdev->bdev.fn_table = &g_bdev_fn_table;
	bdev->bdev.module = &g_module;

	spdk_io_device_register(bdev,
				io_channel_create_cb,
				io_channel_destroy_cb,
				sizeof(struct io_channel),
				bdev->info->name);

	rc = spdk_bdev_register(&bdev->bdev);
	if (rc < 0) {
		fail_create(bdev, "Could not register bdev", rc);
		unregister_io_device(bdev);
		return;
	}

	rc = rbd_update_watch(bdev->image, &bdev->watch_handle, watch_cb, bdev);
	if (rc < 0) {
		fail_create(bdev, "Could not watch rbd", rc);
		unregister_bdev(bdev);
		return;
	}

	bdev->state = CREATED;

	/* reset retry limit and error */
	bdev->remaining_retry_count = CREATE_RETRY_LIMIT;
	bdev->last_err_msg = NULL;
	bdev->last_err_code = 0;
}

static void
open_bdev(struct bdev *bdev)
{
	int rc;

	rc = create_completion(check_open_rc_and_register, bdev, &bdev->c);
	if (rc < 0) {
		fail_create(bdev, "Could not create completion for opening rbd", rc);
		finish_delete(bdev);
		return;
	}

	rc = rados_ioctx_create(bdev->cluster->rados, bdev->info->pool_name, &bdev->ioctx);
	if (rc < 0) {
		fail_create(bdev, "Could not create ioctx for opening rbd", rc);
		finish_delete(bdev);
		return;
	}

	bdev->state = OPENING;
	(void) rbd_aio_open(bdev->ioctx, bdev->info->rbd_name, &bdev->image, NULL, bdev->c);
}

static void
fail_create(struct bdev *bdev, const char *msg, int rc)
{
	SPDK_ERRLOG("%s (%s): %d\n", msg, bdev->info->name, rc);
	bdev->create_failed = true;
	bdev->last_err_msg = msg;
	bdev->last_err_code = rc;
}

static void
finish_delete(void *ctx)
{
	assert(spdk_get_thread() == g_thread);

	struct bdev *bdev = ctx;

	if (bdev->async_destruct) {
		bdev->async_destruct = false;
		spdk_bdev_destruct_done(&bdev->bdev, 0);
	}

	if (bdev->create_failed) {
		bdev->remaining_retry_count--;
		if (bdev->remaining_retry_count == 0) {
			SPDK_WARNLOG("Retry limit for creating bdev %s exhausted, delete it\n", bdev->info->name);
			bdev->state = DELETED;
			free_bdev(bdev);
			return;
		}
		bdev->create_failed = false;
		bdev->create_failed_timestamp_s = spdk_get_ticks() / spdk_get_ticks_hz();
		bdev->state = CREATE_FAILED;
	} else {
		bdev->state = DELETED;
		free_bdev(bdev);
	}
}

static void
check_close_rc_and_free_bdev(void *ctx)
{
	struct bdev *bdev;
	int rc;

	assert(spdk_get_thread() == g_thread);

	bdev = ctx;
	rc = rbd_aio_get_return_value(bdev->c);

	rbd_aio_release(bdev->c);

	if (rc < 0) {
		SPDK_ERRLOG("Could not close rbd (%s): %d\n", bdev->info->name, rc);
	}

	finish_delete(bdev);
}

static void
close_bdev(void *ctx)
{
	assert(spdk_get_thread() == g_thread);

	struct bdev *bdev = ctx;
	int rc;

	rc = create_completion(check_close_rc_and_free_bdev, bdev, &bdev->c);
	if (rc < 0) {
		/* Could not close, so finish and return. */
		SPDK_ERRLOG("Could not flush rbd (%s): %d\n", bdev->info->name, rc);
		finish_delete(bdev);
		return;
	}

	bdev->state = CLOSING;
	(void) rbd_aio_close(bdev->image, bdev->c);
}

static void
check_flush_rc_and_close_bdev(void *ctx)
{
	struct bdev *bdev;
	int rc;

	assert(spdk_get_thread() == g_thread);

	bdev = ctx;
	rc = rbd_aio_get_return_value(bdev->c);

	rbd_aio_release(bdev->c);

	if (rc < 0) {
		/* Do not finish and return, try to close. */
		SPDK_ERRLOG("Could not flush rbd (%s): %d\n", bdev->info->name, rc);
	}

	close_bdev(bdev);
}

static void
flush_bdev(void *ctx)
{
	struct bdev *bdev;
	int rc;

	assert(spdk_get_thread() == g_thread);

	bdev = ctx;

	free(bdev->bdev.name);

	rc = create_completion(check_flush_rc_and_close_bdev, bdev, &bdev->c);
	if (rc < 0) {
		/* Could not flush, try to close. */
		SPDK_ERRLOG("Could not flush rbd (%s): %d\n", bdev->info->name, rc);
		close_bdev(bdev);
		return;
	}

	bdev->state = FLUSHING;
	(void) rbd_aio_flush(bdev->image, bdev->c);
}

static void
unregister_io_device(void *ctx)
{
	struct bdev *bdev;

	assert(spdk_get_thread() == g_thread);

	bdev = ctx;

	bdev->state = IO_DEVICE_UNREGISTERING;
	spdk_io_device_unregister(bdev, flush_bdev);
}

static void
unregister_bdev(struct bdev *bdev)
{
	int rc;

	rc = rbd_update_unwatch(bdev->image, bdev->watch_handle);
	if (rc < 0) {
		SPDK_ERRLOG("Could not unwatch rbd (%s): %d\n", bdev->info->name, rc);
	}

	bdev->state = BDEV_UNREGISTERING;
	spdk_bdev_unregister(&bdev->bdev, NULL, NULL);
}

static int
poll_bdevs(void *ctx)
{
	struct bdev *bdev, *tmp;
	uint64_t timestamp_s;
	bool busy = false;

	assert(spdk_get_thread() == g_thread);

	TAILQ_FOREACH_SAFE(bdev, &g_bdevs, link, tmp) {
		assert(bdev->expected_state == CREATED || bdev->expected_state == DELETED);

		if (bdev->state == DELETED && bdev->expected_state == DELETED) {
			free_bdev(bdev);
			busy |= true;
		} else if (bdev->state == DELETED && bdev->expected_state == CREATED) {
			open_bdev(bdev);
			busy |= true;
		} else if (bdev->state == CREATED && bdev->expected_state == DELETED) {
			unregister_bdev(bdev);
			busy |= true;
		} else if (bdev->state == CREATE_FAILED && bdev->expected_state == DELETED) {
			bdev->state = DELETED;
			free_bdev(bdev);
			busy |= true;
		} else if (bdev->state == CREATE_FAILED && bdev->expected_state == CREATED) {
			timestamp_s = spdk_get_ticks() / spdk_get_ticks_hz();
			if (timestamp_s - bdev->create_failed_timestamp_s >= CREATE_BACKOFF_S) {
				bdev->state = DELETED;
				busy |= true;
			}
		} else {
			/* do nothing */
		}
	}

	return busy ? SPDK_POLLER_BUSY : SPDK_POLLER_IDLE;
}

static struct cluster *
alloc_cluster(void)
{
	struct cluster *cluster;

	cluster = calloc(1, sizeof(*cluster));
	if (cluster == NULL) {
		goto fail;
	}

	TAILQ_INSERT_TAIL(&g_clusters, cluster, link);
	g_clusters_count++;

	return cluster;

fail:
	free_cluster(cluster);
	return NULL;
}

static void
free_cluster(struct cluster *cluster)
{
	if (cluster == NULL) {
		return;
	}

	TAILQ_REMOVE(&g_clusters, cluster, link);
	g_clusters_count--;

	free(cluster);
}

static struct bdev_rbd_mws_cluster_info *
dup_cluster_info(const struct bdev_rbd_mws_cluster_info *info)
{
	struct bdev_rbd_mws_cluster_info *info_dup;

	info_dup = calloc(1, sizeof(*info_dup));
	if (info_dup == NULL) {
		goto fail;
	}

	info_dup->name = strndup(info->name, STR_MAX);
	if (info_dup->name == NULL) {
		goto fail;
	}

	info_dup->config_file = strndup(info->config_file, STR_MAX);
	if (info_dup->config_file == NULL) {
		goto fail;
	}

	if (info->core_mask != NULL) {
		info_dup->core_mask = strndup(info->core_mask, STR_MAX);
		if (info_dup->core_mask == NULL) {
			goto fail;
		}
	}

	if (info->user != NULL) {
		info_dup->user = strndup(info->user, STR_MAX);
		if (info_dup->user == NULL) {
			goto fail;
		}
	}

	return info_dup;

fail:
	free_cluster_info(info_dup);
	return NULL;
}

static void
free_cluster_info(struct bdev_rbd_mws_cluster_info *info)
{
	if (info == NULL) {
		return;
	}

	free(info->config_file);
	free(info->name);
	free(info);
}

static int
check_cluster_info(const struct bdev_rbd_mws_cluster_info *info)
{
	if (info->name == NULL) {
		return -EINVAL;
	}

	if (info->config_file == NULL) {
		return -EINVAL;
	}

	return 0;
}

static struct cluster *get_cluster(const char *name);

static struct cluster *
get_cluster(const char *name)
{
	struct cluster *cluster;
	TAILQ_FOREACH(cluster, &g_clusters, link) {
		if (strncmp(cluster->info->name, name, STR_MAX) == 0) {
			return cluster;
		}
	}
	return NULL;
}

static struct bdev *
alloc_bdev(void)
{
	struct bdev *bdev;
	bdev = calloc(1, sizeof * bdev);
	if (bdev == NULL) {
		goto fail;
	}

	TAILQ_INSERT_TAIL(&g_bdevs, bdev, link);
	g_bdevs_count++;

	return bdev;
fail:
	free_bdev(bdev);
	return NULL;
}

static void
free_bdev(struct bdev *bdev)
{
	if (bdev == NULL) {
		return;
	}

	assert(bdev->state == DELETED);

	rados_ioctx_destroy(bdev->ioctx);

	if (bdev->cluster != NULL) {
		bdev->cluster->ref_count--;
	}

	free_bdev_info(bdev->info);

	TAILQ_REMOVE(&g_bdevs, bdev, link);
	g_bdevs_count--;

	free(bdev);
}

static struct bdev_rbd_mws_info *
dup_bdev_info(const struct bdev_rbd_mws_info *info)
{
	struct bdev_rbd_mws_info *info_dup;

	info_dup = calloc(1, sizeof * info_dup);
	if (info_dup == NULL) {
		goto fail;
	}

	info_dup->name = strndup(info->name, STR_MAX);
	if (info_dup->name == NULL) {
		goto fail;
	}

	info_dup->cluster_name = strndup(info->cluster_name, STR_MAX);
	if (info_dup->cluster_name == NULL) {
		goto fail;
	}

	info_dup->pool_name = strndup(info->pool_name, STR_MAX);
	if (info_dup->pool_name == NULL) {
		goto fail;
	}

	info_dup->rbd_name = strndup(info->rbd_name, STR_MAX);
	if (info_dup->rbd_name == NULL) {
		goto fail;
	}

	info_dup->block_size = info->block_size;

	return info_dup;

fail:
	free_bdev_info(info_dup);
	return NULL;
}

static void
free_bdev_info(struct bdev_rbd_mws_info *info)
{
	if (info == NULL) {
		return;
	}

	free(info->rbd_name);
	free(info->pool_name);
	free(info->cluster_name);
	free(info->name);
	free(info);
}

static struct bdev *
get_bdev(const char *name)
{
	struct bdev *bdev;
	TAILQ_FOREACH(bdev, &g_bdevs, link) {
		if (strncmp(bdev->info->name, name, STR_MAX) == 0) {
			return bdev;
		}
	}
	return NULL;
}

static void
completion_cb(rbd_completion_t c, void *arg)
{
	struct completion_ctx *ctx = arg;
	spdk_thread_send_msg(g_thread, ctx->cb, ctx->arg);
	free(ctx);
}

static int
create_completion(void (*cb)(void *), void *arg, rbd_completion_t *c)
{
	struct completion_ctx *ctx;

	ctx = calloc(1, sizeof * ctx);
	if (ctx == NULL) {
		return -ENOMEM;
	}

	ctx->cb = cb;
	ctx->arg = arg;

	(void) rbd_aio_create_completion(ctx, completion_cb, c);

	return 0;
}

static int
destruct(void *ctx)
{
	assert(spdk_get_thread() == g_thread);

	struct bdev *bdev = ctx;

	bdev->async_destruct = true;
	unregister_io_device(ctx);

	/* Return 1 to indicate the destruct path is asynchronous. */
	return 1;
}

static void
get_buf_cb(struct spdk_io_channel *ch, struct spdk_bdev_io *spdk_bdev_io, bool success)
{

	if (!success) {
		bdev_io_set_status(spdk_bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}

	bdev_io_submit(spdk_bdev_io);
}

static void
bdev_io_complete(rbd_completion_t c, void *ctx)
{
	struct spdk_bdev_io *spdk_bdev_io = ctx;
	uint64_t len = spdk_bdev_io->u.bdev.num_blocks * spdk_bdev_io->bdev->blocklen;
	int rc = rbd_aio_get_return_value(c);
	int status;

	switch (spdk_bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_READ:
		status = rc == (int) len ? SPDK_BDEV_IO_STATUS_SUCCESS : SPDK_BDEV_IO_STATUS_FAILED;
		break;
	case SPDK_BDEV_IO_TYPE_COMPARE_AND_WRITE:
		status = rc == -EILSEQ ? SPDK_BDEV_IO_STATUS_MISCOMPARE : SPDK_BDEV_IO_STATUS_SUCCESS;
		break;
	default:
		status = rc == 0 ? SPDK_BDEV_IO_STATUS_SUCCESS : SPDK_BDEV_IO_STATUS_FAILED;
	}

	rbd_aio_release(c);
	bdev_io_set_status(spdk_bdev_io, status);
}

static void
bdev_io_submit(void *ctx)
{
	struct spdk_bdev_io *spdk_bdev_io = ctx;
	struct bdev_io *bdev_io = (struct bdev_io *) spdk_bdev_io->driver_ctx;
	struct bdev *bdev = spdk_bdev_io->bdev->ctxt;
	struct iovec *iov = spdk_bdev_io->u.bdev.iovs;
	int iovcnt = spdk_bdev_io->u.bdev.iovcnt;
	uint64_t offset = spdk_bdev_io->u.bdev.offset_blocks * spdk_bdev_io->bdev->blocklen;
	uint64_t len = spdk_bdev_io->u.bdev.num_blocks * spdk_bdev_io->bdev->blocklen;
	int rc;

	rc = rbd_aio_create_completion(spdk_bdev_io, bdev_io_complete, &bdev_io->c);
	if (rc < 0) {
		goto fail;
	}

	switch (spdk_bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_READ:
		if (spdk_likely(iovcnt == 1)) {
			rc = rbd_aio_read(bdev->image, offset, iov[0].iov_len, iov[0].iov_base,
					  bdev_io->c);
		} else {
			rc = rbd_aio_readv(bdev->image, iov, iovcnt, offset, bdev_io->c);
		}
		break;
	case SPDK_BDEV_IO_TYPE_WRITE:
		if (spdk_likely(iovcnt == 1)) {
			rc = rbd_aio_write(bdev->image, offset, iov[0].iov_len, iov[0].iov_base,
					   bdev_io->c);
		} else {
			rc = rbd_aio_writev(bdev->image, iov, iovcnt, offset, bdev_io->c);
		}
		break;
	case SPDK_BDEV_IO_TYPE_UNMAP:
		rc = rbd_aio_discard(bdev->image, offset, len, bdev_io->c);
		break;
	case SPDK_BDEV_IO_TYPE_FLUSH:
		rc = rbd_aio_flush(bdev->image, bdev_io->c);
		break;
	case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
		rc = rbd_aio_write_zeroes(bdev->image, offset, len, bdev_io->c, /* zero_flags */ 0,
					  /* op_flags */ 0);
		break;
#ifdef LIBRBD_SUPPORTS_COMPARE_AND_WRITE_IOVEC
	case SPDK_BDEV_IO_TYPE_COMPARE_AND_WRITE:
		rc = rbd_aio_compare_and_writev(bdev->image, offset, iov /* cmp */, iovcnt,
						spdk_bdev_io->u.bdev.fused_iovs /* write */,
						spdk_bdev_io->u.bdev.fused_iovcnt,
						bdev_io->c, NULL,
						/* op_flags */ 0);
		break;
#endif
	default:
		/* This should not happen.
		 * Function should only be called with supported io types in bdev_rbd_submit_request
		 */
		SPDK_ERRLOG("Unsupported IO type =%d\n", spdk_bdev_io->type);
		rc = -ENOTSUP;
		break;
	}

	if (rc < 0) {
		rbd_aio_release(bdev_io->c);
		goto fail;
	}

	return;

fail:
	bdev_io_set_status(spdk_bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
}

static void
bdev_io_set_status_cb(void *ctx)
{
	struct bdev_io *bdev_io = ctx;

	spdk_bdev_io_complete(spdk_bdev_io_from_ctx(bdev_io), bdev_io->status);
}

static void
bdev_io_set_status(struct spdk_bdev_io *spdk_bdev_io, enum spdk_bdev_io_status status)
{
	struct bdev_io *bdev_io = (struct bdev_io *) spdk_bdev_io->driver_ctx;
	struct spdk_thread *current_thread = spdk_get_thread();

	bdev_io->status = status;

	assert(bdev_io->submit_td != NULL);

	if (bdev_io->submit_td != current_thread) {
		spdk_thread_send_msg(bdev_io->submit_td, bdev_io_set_status_cb, bdev_io);
	} else {
		bdev_io_set_status_cb(bdev_io);
	}
}

static void
submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *spdk_bdev_io)
{
	struct spdk_thread *submit_td = spdk_io_channel_get_thread(ch);
	struct bdev_io *bdev_io = (struct bdev_io *)spdk_bdev_io->driver_ctx;

	bdev_io->submit_td = submit_td;
	switch (spdk_bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_READ:
		spdk_bdev_io_get_buf(spdk_bdev_io, get_buf_cb,
				     spdk_bdev_io->u.bdev.num_blocks * spdk_bdev_io->bdev->blocklen);
		break;

	case SPDK_BDEV_IO_TYPE_WRITE:
	case SPDK_BDEV_IO_TYPE_UNMAP:
	case SPDK_BDEV_IO_TYPE_FLUSH:
	case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
#ifdef LIBRBD_SUPPORTS_COMPARE_AND_WRITE_IOVEC
	case SPDK_BDEV_IO_TYPE_COMPARE_AND_WRITE:
#endif
		bdev_io_submit(spdk_bdev_io);
		break;

	default:
		SPDK_ERRLOG("Unsupported IO type =%d\n", spdk_bdev_io->type);
		bdev_io_set_status(spdk_bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		break;
	}
}

static bool
io_type_supported(void *ctx, enum spdk_bdev_io_type io_type)
{
	switch (io_type) {
	case SPDK_BDEV_IO_TYPE_READ:
	case SPDK_BDEV_IO_TYPE_WRITE:
	case SPDK_BDEV_IO_TYPE_UNMAP:
	case SPDK_BDEV_IO_TYPE_FLUSH:
	case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
#ifdef LIBRBD_SUPPORTS_COMPARE_AND_WRITE_IOVEC
	case SPDK_BDEV_IO_TYPE_COMPARE_AND_WRITE:
#endif
		return true;
	default:
		return false;
	}
}

static struct spdk_io_channel *
get_io_channel(void *ctx)
{
	return spdk_get_io_channel(ctx);
}

static int
dump_info_json(void *ctx, struct spdk_json_write_ctx *w)
{
	struct bdev *bdev;

	assert(spdk_get_thread() == g_thread);

	bdev = ctx;

	spdk_json_write_named_object_begin(w, "rbd");

	/* fields for backward compatibility with rbd module */
	spdk_json_write_named_string(w, "pool_name", bdev->info->pool_name);
	spdk_json_write_named_string(w, "rbd_name", bdev->info->rbd_name);
	spdk_json_write_named_string(w, "config_file", bdev->cluster->info->config_file);

	spdk_json_write_object_end(w);

	return 0;
}

static void
dump_device_stat_json(void *ctx, struct spdk_json_write_ctx *w)
{
	struct bdev *bdev = ctx;
	rbd_perf_counters_t counters;

	rbd_get_image_perf_counters(bdev->image, &counters);

	spdk_json_write_named_object_begin(w, "rbd_image_perf_counters");
	spdk_json_write_named_uint64(w, "rd", counters.rd);
	spdk_json_write_named_uint64(w, "rd_bytes", counters.rd_bytes);
	spdk_json_write_named_uint64(w, "rd_latency", counters.rd_latency);
	spdk_json_write_named_uint64(w, "wr", counters.wr);
	spdk_json_write_named_uint64(w, "wr_bytes", counters.wr_bytes);
	spdk_json_write_named_uint64(w, "wr_latency", counters.wr_latency);
	spdk_json_write_named_uint64(w, "discard", counters.discard);
	spdk_json_write_named_uint64(w, "discard_bytes", counters.discard_bytes);
	spdk_json_write_named_uint64(w, "discard_latency", counters.discard_latency);
	spdk_json_write_named_uint64(w, "flush", counters.flush);
	spdk_json_write_named_uint64(w, "flush_latency", counters.flush_latency);
	spdk_json_write_named_uint64(w, "ws", counters.ws);
	spdk_json_write_named_uint64(w, "ws_bytes", counters.ws_bytes);
	spdk_json_write_named_uint64(w, "ws_latency", counters.ws_latency);
	spdk_json_write_named_uint64(w, "cmp", counters.cmp);
	spdk_json_write_named_uint64(w, "cmp_bytes", counters.cmp_bytes);
	spdk_json_write_named_uint64(w, "cmp_latency", counters.cmp_latency);
	spdk_json_write_named_uint64(w, "snap_create", counters.snap_create);
	spdk_json_write_named_uint64(w, "snap_remove", counters.snap_remove);
	spdk_json_write_named_uint64(w, "snap_rollback", counters.snap_rollback);
	spdk_json_write_named_uint64(w, "snap_rename", counters.snap_rename);
	spdk_json_write_named_uint64(w, "notify", counters.notify);
	spdk_json_write_named_uint64(w, "resize", counters.resize);
	spdk_json_write_named_uint64(w, "readahead", counters.readahead);
	spdk_json_write_named_uint64(w, "readahead_bytes", counters.readahead_bytes);
	spdk_json_write_named_uint64(w, "invalidate_cache", counters.invalidate_cache);
	spdk_json_write_named_uint64(w, "opened_time", counters.opened_time);
	spdk_json_write_named_uint64(w, "lock_acquired_time", counters.lock_acquired_time);
	spdk_json_write_object_end(w);
}

static int
io_channel_create_cb(void *io_device, void *ctx_buf)
{
	return 0;
}

static void
io_channel_destroy_cb(void *io_device, void *ctx_buf)
{
}

static void
dump_bdev(struct spdk_json_write_ctx *w, struct bdev *bdev, bool dump_extra)
{
	spdk_json_write_object_begin(w);
	spdk_json_write_named_string(w, "name", bdev->info->name);
	if (dump_extra) {
		spdk_json_write_named_string(w, "cluster_name", bdev->info->cluster_name);
		spdk_json_write_named_string(w, "pool_name", bdev->info->pool_name);
		spdk_json_write_named_string(w, "rbd_name", bdev->info->rbd_name);
		if (bdev->remaining_retry_count != CREATE_RETRY_LIMIT) {
			spdk_json_write_named_int64(w, "remaining_retry_count", bdev->remaining_retry_count);
		}
		if (bdev->last_err_msg) {
			spdk_json_write_named_string(w, "last_error_msg", bdev->last_err_msg);
			spdk_json_write_named_int64(w, "last_error_code", bdev->last_err_code);
		}
	}
	spdk_json_write_named_string(w, "expected_state", state_name[bdev->expected_state]);
	spdk_json_write_named_string(w, "state", state_name[bdev->state]);
	spdk_json_write_object_end(w);
}

static void
dump_cluster(struct spdk_json_write_ctx *w, struct cluster *cluster)
{
	spdk_json_write_object_begin(w);
	spdk_json_write_named_string(w, "name", cluster->info->name);
	spdk_json_write_named_string(w, "config_file", cluster->info->config_file);
	if (cluster->info->core_mask != NULL) {
		spdk_json_write_named_string(w, "core_mask", cluster->info->core_mask);
	}
	if (cluster->info->user != NULL) {
		spdk_json_write_named_string(w, "user", cluster->info->user);
	}
	spdk_json_write_object_end(w);
}
