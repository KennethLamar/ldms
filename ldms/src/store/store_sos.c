/* -*- c-basic-offset: 8 -*-
 * Copyright (c) 2012 Open Grid Computing, Inc. All rights reserved.
 * Copyright (c) 2012 Sandia Corporation. All rights reserved.
 * Under the terms of Contract DE-AC04-94AL85000, there is a non-exclusive
 * license for use of this work by or on behalf of the U.S. Government.
 * Export of this program may require a license from the United States
 * Government.
 *
 * This software is available to you under a choice of one of two
 * licenses.  You may choose to be licensed under the terms of the GNU
 * General Public License (GPL) Version 2, available from the file
 * COPYING in the main directory of this source tree, or the BSD-type
 * license below:
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *      Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *
 *      Redistributions in binary form must reproduce the above
 *      copyright notice, this list of conditions and the following
 *      disclaimer in the documentation and/or other materials provided
 *      with the distribution.
 *
 *      Neither the name of Sandia nor the names of any contributors may
 *      be used to endorse or promote products derived from this software
 *      without specific prior written permission.
 *
 *      Neither the name of Open Grid Computing nor the names of any
 *      contributors may be used to endorse or promote products derived
 *      from this software without specific prior written permission.
 *
 *      Modified source versions must be plainly marked as such, and
 *      must not be misrepresented as being the original software.
 *
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
#include <sys/queue.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include <linux/limits.h>
#include <pthread.h>
#include <errno.h>
#include <sos/sos.h>
#include <coll/idx.h>
#include "ldms.h"
#include "ldmsd.h"

SOS_OBJ_BEGIN(ovis_metric_class_int32, "OvisMetric_int32")
	SOS_OBJ_ATTR_WITH_KEY("tv_sec", SOS_TYPE_UINT32),
	SOS_OBJ_ATTR("tv_usec", SOS_TYPE_UINT32),
	SOS_OBJ_ATTR_WITH_KEY("metric_id", SOS_TYPE_UINT64),
	SOS_OBJ_ATTR("value", SOS_TYPE_INT32)
SOS_OBJ_END(4);

SOS_OBJ_BEGIN(ovis_metric_class_int64, "OvisMetric_int64")
	SOS_OBJ_ATTR_WITH_KEY("tv_sec", SOS_TYPE_UINT32),
	SOS_OBJ_ATTR("tv_usec", SOS_TYPE_UINT32),
	SOS_OBJ_ATTR_WITH_KEY("metric_id", SOS_TYPE_UINT64),
	SOS_OBJ_ATTR("value", SOS_TYPE_INT64)
SOS_OBJ_END(4);

SOS_OBJ_BEGIN(ovis_metric_class_uint32, "OvisMetric_uint32")
	SOS_OBJ_ATTR_WITH_KEY("tv_sec", SOS_TYPE_UINT32),
	SOS_OBJ_ATTR("tv_usec", SOS_TYPE_UINT32),
	SOS_OBJ_ATTR_WITH_KEY("metric_id", SOS_TYPE_UINT64),
	SOS_OBJ_ATTR("value", SOS_TYPE_UINT32)
SOS_OBJ_END(4);

SOS_OBJ_BEGIN(ovis_metric_class_uint64, "OvisMetric_uint64")
	SOS_OBJ_ATTR_WITH_KEY("tv_sec", SOS_TYPE_UINT32),
	SOS_OBJ_ATTR("tv_usec", SOS_TYPE_UINT32),
	SOS_OBJ_ATTR_WITH_KEY("metric_id", SOS_TYPE_UINT64),
	SOS_OBJ_ATTR("value", SOS_TYPE_UINT64)
SOS_OBJ_END(4);

SOS_OBJ_BEGIN(ovis_metric_class_double, "OvisMetric_double")
	SOS_OBJ_ATTR_WITH_KEY("tv_sec", SOS_TYPE_UINT32),
	SOS_OBJ_ATTR("tv_usec", SOS_TYPE_UINT32),
	SOS_OBJ_ATTR_WITH_KEY("metric_id", SOS_TYPE_UINT64),
	SOS_OBJ_ATTR("value", SOS_TYPE_DOUBLE)
SOS_OBJ_END(4);

#define TV_SEC_COL	0
#define TV_USEC_COL	1
#define GROUP_COL	2
#define VALUE_COL	3

enum {
	LDMSD_SOS_INT32 = 0,
	LDMSD_SOS_INT64,
	LDMSD_SOS_UINT32,
	LDMSD_SOS_UINT64,
	LDMSD_SOS_DOUBLE,
	LDMSD_SOS_NUM_TYPE,
};

/*
 * NOTE:
 *   (sos::path) = (root_path)/(comp_type)/(metric)
 */

static idx_t store_idx;
static char tmp_path[PATH_MAX];
static char *root_path; /**< store root path */
static ldmsd_msg_log_f msglog;

#define _stringify(_x) #_x
#define stringify(_x) _stringify(_x)

/**
 * \brief Store for individual metric.
 */
struct sos_metric_store {
	sos_t sos; /**< sos handle */
	pthread_mutex_t lock; /**< lock at metric store level */
	char *path; /**< path of the sos store */
	LIST_ENTRY(sos_metric_store) entry;
};

struct sos_store_instance {
	struct ldmsd_store *store;
	char *path; /**< (root_path)/(comp_type) */
	char *container;
	void *ucontext;
	idx_t ms_idx;
	LIST_HEAD(ms_list, sos_metric_store) ms_list;
	int metric_count;
	struct sos_metric_store **ms;
};

pthread_mutex_t cfg_lock;

/**
 * \brief Configuration
 */
static int config(struct attr_value_list *kwl, struct attr_value_list *avl)
{
	char *value;
	value = av_value(avl, "path");
	if (!value)
		goto err;

	pthread_mutex_lock(&cfg_lock);
	if (root_path)
		free(root_path);
	root_path = strdup(value);
	pthread_mutex_unlock(&cfg_lock);
	if (!root_path)
		return ENOMEM;
	return 0;
 err:
	return EINVAL;
}

static void term(void)
{
}

static const char *usage(void)
{
	return  "    config name=store_sos path=<path>\n"
		"        - Set the root path for the storage of SOS files.\n"
		"        path      The path to the root of the SOS directory\n";
}

static ldmsd_store_handle_t
get_store(const char *container)
{
	ldmsd_store_handle_t sh;

	pthread_mutex_lock(&cfg_lock);
	/*
	 * Add a component type directory if one does not
	 * already exist
	 */
	sh = idx_find(store_idx, (void *)container, strlen(container));
	pthread_mutex_unlock(&cfg_lock);
	return sh;
}

static void *get_ucontext(ldmsd_store_handle_t _sh)
{
	struct sos_store_instance *si = _sh;
	return si->ucontext;
}

int store_sos_open_sos(struct sos_metric_store *ms, ldms_metric_t m)
{
	enum ldms_value_type type = ldms_get_metric_type(m);

	switch (type) {
	case LDMS_V_S32:
		ms->sos = sos_open(ms->path, O_RDWR|O_CREAT,
				0660, &ovis_metric_class_int32);
		if (!ms->sos) {
			msglog("store_sos: Failed to open "
					"'Ovismetric_int32\n");
			return ENOMEM;
		}
		break;
	case LDMS_V_S64:
		ms->sos = sos_open(ms->path, O_RDWR|O_CREAT,
				0660, &ovis_metric_class_int64);
		if (!ms->sos) {
			msglog("store_sos: Failed to open "
					"'Ovismetric_int64\n");
			return ENOMEM;
		}
		break;
	case LDMS_V_U32:
		ms->sos = sos_open(ms->path, O_RDWR|O_CREAT,
					0660, &ovis_metric_class_uint32);
		if (!ms->sos) {
			msglog("store_sos: Failed to open "
					"'Ovismetric_uint32\n");
			return ENOMEM;
		}
		break;
	case LDMS_V_U64:
		ms->sos = sos_open(ms->path, O_RDWR|O_CREAT,
					0660, &ovis_metric_class_uint64);
		if (!ms->sos) {
			msglog("store_sos: Failed to open "
					"'Ovismetric_uint64\n");
			return ENOMEM;
		}
		break;
	case LDMS_V_F:
	case LDMS_V_D:
		ms->sos = sos_open(ms->path, O_RDWR|O_CREAT,
					0660, &ovis_metric_class_double);
		if (!ms->sos) {
			msglog("store_sos: Failed to open "
					"'Ovismetric_double\n");
			return ENOMEM;
		}
		break;
	default:
		msglog("store_sos: not support ldms_value_type '%s'\n",
						ldms_type_to_str(type));
		return ENOTSUP;
	}

	return 0;
}

static ldmsd_store_handle_t
new_store(struct ldmsd_store *s, const char *comp_type, const char *container,
	  struct ldmsd_store_metric_index_list *metric_list, void *ucontext)
{
	struct sos_store_instance *si;
	struct sos_metric_store *ms;
	int i, metric_count;

	pthread_mutex_lock(&cfg_lock);
	/*
	 * Add a component type directory if one does not
	 * already exist
	 */
	si = idx_find(store_idx, (void *)container, strlen(container));
	if (!si) {
		/*
		 * First, count the metric.
		 */
		metric_count = 0;
		struct ldmsd_store_metric_index *x;
		LIST_FOREACH(x, metric_list, entry) {
			metric_count++;
		}
		sprintf(tmp_path, "%s/%s", root_path, comp_type);
		mkdir(tmp_path, 0777);

		/*
		 * Open a new store for this component-type and
		 * metric combination
		 */
		si = calloc(1, sizeof(*si));
		if (!si)
			goto out;
		si->metric_count = metric_count;
		si->ms_idx = idx_create();
		if (!si->ms_idx)
			goto err1;
		si->ucontext = ucontext;
		si->store = s;
		si->path = strdup(tmp_path);
		if (!si->path)
			goto err2;
		si->container = strdup(container);
		if (!si->container)
			goto err3;

		if (metric_count == 0) {
			idx_add(store_idx, (void *)container,
					strlen(container), si);
			goto out;
		}

		si->ms = calloc(metric_count,
				sizeof(struct sos_metric_store *));
		if (!si->ms)
			goto err4;

		i = 0;
		char buff[128];
		char *name;
		LIST_FOREACH(x, metric_list, entry) {
			name = strchr(x->name, '#');
			if (name) {
				int len = name - x->name;
				name = strncpy(buff, x->name, len);
				name[len] = 0;
			} else {
				name = x->name;
			}
			ms = idx_find(si->ms_idx, name, strlen(name));
			if (ms) {
				si->ms[i++] = ms;
				continue;
			}
			/* Create ms if not exist */
			ms = calloc(1, sizeof(*ms));
			if (!ms)
				goto err5;
			sprintf(tmp_path, "%s/%s", si->path, name);
			ms->path = strdup(tmp_path);
			if (!ms->path) {
				free(ms);
				goto err5;
			}

			/*
			 * NOTE: sos will be opened the first time
			 * the metric to be stored.
			 */

			pthread_mutex_init(&ms->lock, NULL);
			idx_add(si->ms_idx, name, strlen(name), ms);
			LIST_INSERT_HEAD(&si->ms_list, ms, entry);
			si->ms[i++] = ms;
		}
		idx_add(store_idx, (void *)container, strlen(container), si);
	}
	goto out;
err5:
	while (ms = LIST_FIRST(&si->ms_list)) {
		LIST_REMOVE(ms, entry);
		if (ms->path)
			free(ms->path);
		free(ms);
	}
	free(si->ms);
err4:
	free(si->container);
err3:
	free(si->path);
err2:
	idx_destroy(si->ms_idx);
err1:
	free(si);
	si = NULL;
out:
	pthread_mutex_unlock(&cfg_lock);
	return si;
}

int store_sos_create_ms_list(struct sos_store_instance *si, ldms_mvec_t mvec)
{
	int i;
	si->metric_count = mvec->count;
	si->ms = calloc(mvec->count, sizeof(struct sos_metric_store *));
	if (!si->ms)
		return ENOMEM;

	char buff[128];
	char *name, *metric_name;
	struct sos_metric_store *ms;
	for (i = 0; i < mvec->count; i++) {
		metric_name = ldms_get_metric_name(mvec->v[i]);
		name = strchr(metric_name, '#');
		if (name) {
			int len = name - metric_name;
			name = strncpy(buff, metric_name, len);
			name[len] = 0;
		} else {
			name = metric_name;
		}
		ms = idx_find(si->ms_idx, name, strlen(name));
		if (ms) {
			si->ms[i] = ms;
			continue;
		}
		/* Create ms if not exist */
		ms = calloc(1, sizeof(*ms));
		if (!ms)
			goto err;
		sprintf(tmp_path, "%s/%s", si->path, name);
		ms->path = strdup(tmp_path);
		if (!ms->path) {
			free(ms);
			goto err;
		}

		/*
		 * NOTE: sos will be opened the first time
		 * the metric to be stored.
		 */

		pthread_mutex_init(&ms->lock, NULL);
		idx_add(si->ms_idx, name, strlen(name), ms);
		LIST_INSERT_HEAD(&si->ms_list, ms, entry);
		si->ms[i] = ms;
	}
	return 0;
err:
	while (ms = LIST_FIRST(&si->ms_list)) {
		LIST_REMOVE(ms, entry);
		if (ms->path)
			free(ms->path);
		free(ms);
	}
	free(si->ms);
	free(si->container);
	free(si->path);
	idx_destroy(si->ms_idx);
	return -1;
}

static int
store(ldmsd_store_handle_t _sh, ldms_set_t set, ldms_mvec_t mvec)
{
	struct sos_store_instance *si;
	sos_obj_t obj;
	int i;
	int rc = 0;
	int last_rc = 0;
	int last_errno = 0;

	if (!_sh)
		return EINVAL;

	si = _sh;

	/* When call new_store the lookup_cb hasn't finished yet. */
	if (si->metric_count == 0) {
		rc = store_sos_create_ms_list(si, mvec);
		if (rc) {
			msglog("store_sos: Failed to create store "
						"for each metric.\n");
			return -1;
		}
	}

	const struct ldms_timestamp *ts = ldms_get_timestamp(set);

	for (i = 0; i < mvec->count; i++) {
		pthread_mutex_lock(&si->ms[i]->lock);

		if (!si->ms[i]->sos) {
			if (store_sos_open_sos(si->ms[i], mvec->v[i])) {
				pthread_mutex_unlock(&si->ms[i]->lock);
				return ENOMEM;
			}
		}

		obj = sos_obj_new(si->ms[i]->sos);
		if (!obj) {
			msglog("Error %d: %s at %s:%d\n", errno,
					strerror(errno), __FILE__, __LINE__);
			errno = ENOMEM;
			pthread_mutex_unlock(&si->ms[i]->lock);
			return -1;
		}
		uint64_t metric_id = ldms_get_user_data(mvec->v[i]);
		sos_obj_attr_set(si->ms[i]->sos, 0, obj, (void*)&ts->sec);
		sos_obj_attr_set(si->ms[i]->sos, 1, obj, (void*)&ts->usec);
		sos_obj_attr_set(si->ms[i]->sos, 2, obj, &metric_id);
		sos_obj_attr_set(si->ms[i]->sos, 3, obj,
				 ldms_get_value_ptr(mvec->v[i]));
		rc = sos_obj_add(si->ms[i]->sos, obj);
		pthread_mutex_unlock(&si->ms[i]->lock);
		if (rc) {
			last_errno = errno;
			last_rc = rc;
			msglog("Error %d: %s at %s:%d\n", errno,
					strerror(errno), __FILE__, __LINE__);
		}
	}

	if (last_errno)
		errno = last_errno;
	return last_rc;
}

static int flush_store(ldmsd_store_handle_t _sh)
{
	struct sos_store_instance *si = _sh;
	struct sos_metric_store *ms;
	if (!_sh)
		return EINVAL;
	int i;
	LIST_FOREACH(ms, &si->ms_list, entry) {
		pthread_mutex_lock(&ms->lock);
		sos_flush(ms->sos);
		pthread_mutex_lock(&ms->lock);
	}
	return 0;
}

static void close_store(ldmsd_store_handle_t _sh)
{
	/*
	 * NOTE: This close function looks like destroy to me.
	 */
	struct sos_store_instance *si = _sh;
	struct sos_metric_store *ms;
	if (!_sh)
		return;
	int i;
	while (ms = LIST_FIRST(&si->ms_list)) {
		if (ms->sos)
			sos_close(ms->sos);
		if (ms->path)
			free(ms->path);
		free(ms);
	}
	idx_delete(store_idx, (void *)(si->container), strlen(si->container));
	free(si->path);
	free(si->container);
	idx_destroy(si->ms_idx);
	free(si);
}

static void destroy_store(ldmsd_store_handle_t _sh)
{
	close_store(_sh);
}

static struct ldmsd_store store_sos = {
	.base = {
		.name = "sos",
		.term = term,
		.config = config,
		.usage = usage,
	},
	.get = get_store,
	.new = new_store,
	.destroy = destroy_store,
	.get_context = get_ucontext,
	.store = store,
	.flush = flush_store,
	.close = close_store,
};

struct ldmsd_plugin *get_plugin(ldmsd_msg_log_f pf)
{
	msglog = pf;
	return &store_sos.base;
}

static void __attribute__ ((constructor)) store_sos_init();
static void store_sos_init()
{
	store_idx = idx_create();
	pthread_mutex_init(&cfg_lock, NULL);
}

static void __attribute__ ((destructor)) store_sos_fini(void);
static void store_sos_fini()
{
	pthread_mutex_destroy(&cfg_lock);
	idx_destroy(store_idx);
}
