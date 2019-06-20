/* -*- c-basic-offset: 8 -*-
 *
 * Copyright (c) 2019 Open Grid Computing, Inc. All rights reserved.
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
#define _GNU_SOURCE
#include <sys/errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/queue.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <unistd.h>
#include <assert.h>
#include <papi.h>
#include <json/json_util.h>
#include "ldms.h"
#include "ldmsd.h"
#include "ldmsd_stream.h"
#include "../sampler_base.h"
#include "papi_sampler.h"

static ldmsd_msg_log_f msglog;
static char *papi_stream_name;
static char *papi_config_path;
base_data_t papi_base;

pthread_mutex_t job_lock = PTHREAD_MUTEX_INITIALIZER;
struct rbt job_tree;		/* indexed by job_id */

#define DEFAULT_JOB_EXPIRY	60
static int papi_job_expiry = DEFAULT_JOB_EXPIRY;
LIST_HEAD(,job_data) job_expiry_list; /* list of jobs awaiting cleanup */

/*
 * Find the job_data record with the specified job_id
 */
static job_data_t get_job_data(uint64_t job_id)
{
	job_data_t jd = NULL;
	struct rbn *rbn;
	rbn = rbt_find(&job_tree, &job_id);
	if (rbn)
		jd = container_of(rbn, struct job_data, job_ent);
	return jd;
}

/*
 * Allocate a job_data slot for the specified job_id.
 *
 * The slot table is consulted for the next available slot.
 */
static job_data_t alloc_job_data(uint64_t job_id)
{
	job_data_t jd;

	jd = calloc(1, sizeof *jd);
	if (jd) {
		jd->base = papi_base;
		jd->job_id = job_id;
		jd->job_state = JOB_PAPI_IDLE;
		jd->task_init_count = 0;

		TAILQ_INIT(&jd->event_list);
		LIST_INIT(&jd->task_list);

		rbn_init(&jd->job_ent, &jd->job_id);
		rbt_ins(&job_tree, &jd->job_ent);
	}
	return jd;
 	free(jd);
	errno = ENOMEM;
	return NULL;
}

static void release_job_data(job_data_t jd)
{
	job_task_t t;
	jd->job_state = JOB_PAPI_COMPLETE;
	if (jd->papi_init) {
		LIST_FOREACH(t, &jd->task_list, entry) {
			PAPI_cleanup_eventset(t->event_set);
			PAPI_destroy_eventset(&t->event_set);
		}
		jd->papi_init = 0;
	}
	if (jd->set)
		ldms_set_unpublish(jd->set);
	rbt_del(&job_tree, &jd->job_ent);
	LIST_INSERT_HEAD(&job_expiry_list, jd, expiry_entry);
}

static void free_job_data(job_data_t jd)
{
	papi_event_t ev;
	job_task_t t;

	if (jd->set)
		ldms_set_delete(jd->set);

	while (!TAILQ_EMPTY(&jd->event_list)) {
		ev = TAILQ_FIRST(&jd->event_list);
		free(ev->event_name);
		TAILQ_REMOVE(&jd->event_list, ev, entry);
		free(ev);
	}

	while (!LIST_EMPTY(&jd->task_list)) {
		t = LIST_FIRST(&jd->task_list);
		LIST_REMOVE(t, entry);
		free(t);
	}

	free(jd->schema_name);
	free(jd->instance_name);
	free(jd);
}

static void *cleanup_proc(void *arg)
{
	time_t now;
	job_data_t job;
	LIST_HEAD(,job_data) delete_list;
	LIST_INIT(&delete_list);
	while (1) {
		sleep(papi_job_expiry);
		now = time(NULL);
		pthread_mutex_lock(&job_lock);
		LIST_FOREACH(job, &job_expiry_list, expiry_entry) {
			if ((now - job->job_end) > papi_job_expiry)
				LIST_INSERT_HEAD(&delete_list, job, delete_entry);
		}
		while (!LIST_EMPTY(&delete_list)) {
			job = LIST_FIRST(&delete_list);
			LIST_REMOVE(job, expiry_entry);
			LIST_REMOVE(job, delete_entry);
			free_job_data(job);
		}
		pthread_mutex_unlock(&job_lock);
	}
	return NULL;
}

static int create_metric_set(job_data_t job)
{
	ldms_schema_t schema;
	int i, rc;
	job_task_t t;

	schema = ldms_schema_new(job->schema_name);
	if (schema == NULL) {
		job->job_state = JOB_PAPI_ERROR;
		return ENOMEM;
	}

	/* component_id */
	job->comp_id_mid = ldms_schema_meta_add(schema, "component_id", LDMS_V_U64);
	if (job->comp_id_mid < 0)
		goto err;
	/* job_id */
	job->job_id_mid = ldms_schema_meta_add(schema, "job_id", LDMS_V_U64);
	if (job->job_id_mid < 0)
		goto err;
	/* app_id */
	job->app_id_mid = ldms_schema_meta_add(schema, "app_id", LDMS_V_U64);
	if (job->app_id_mid < 0)
		goto err;
	/* job_state */
	job->job_state_mid =
		ldms_schema_metric_add(schema, "job_state", LDMS_V_U8);
	if (job->job_state_mid < 0)
		goto err;
	/* job_start */
	job->job_start_mid =
		ldms_schema_meta_add(schema, "job_start", LDMS_V_U32);
	if (job->job_start_mid < 0)
		goto err;
	/* job_end */
	job->job_end_mid =
		ldms_schema_metric_add(schema, "job_end", LDMS_V_U32);
	if (job->job_end_mid < 0)
		goto err;
	/* task_count */
	job->task_count_mid =
		ldms_schema_meta_add(schema, "task_count", LDMS_V_U32);
	if (job->task_count_mid < 0)
		goto err;

	/* task_pid */
	job->task_pids_mid =
		ldms_schema_meta_array_add(schema, "task_pids",
					   LDMS_V_U32_ARRAY,
					   job->task_count);
	if (job->task_pids_mid < 0)
		goto err;

	/* task_pid */
	job->task_ranks_mid =
		ldms_schema_meta_array_add(schema, "task_ranks",
					   LDMS_V_U32_ARRAY,
					   job->task_count);
	if (job->task_ranks_mid < 0)
		goto err;

	/* events */
	papi_event_t ev;
	TAILQ_FOREACH(ev, &job->event_list, entry) {
		ev->mid = ldms_schema_metric_array_add(schema,
						       ev->event_name,
						       LDMS_V_S64_ARRAY,
						       job->task_count);
		if (ev->mid < 0)
			goto err;
	}

	job->instance_name = malloc(256);
	snprintf(job->instance_name, 256, "%s/%s/%llu",
		 job->base->producer_name, job->base->schema_name,
		 job->job_id);
	job->set = ldms_set_new_with_auth(job->instance_name, schema,
					  job->base->uid, job->base->gid,
					  job->base->perm);
	if (!job->set) {
		rc = errno;
		msglog(LDMSD_LERROR,
		       "papi_sampler [%d]: Error %d creating the metric set '%s'.\n",
		       __LINE__, rc);
		goto err;
	}
	ldms_set_producer_name_set(job->set, job->base->producer_name);
	ldms_metric_set_u64(job->set, job->job_id_mid, job->job_id);
	ldms_metric_set_u64(job->set, job->comp_id_mid, job->base->component_id);
	ldms_metric_set_u64(job->set, job->app_id_mid, job->app_id);
	ldms_metric_set_u32(job->set, job->task_count_mid, job->task_count);
	ldms_metric_set_u32(job->set, job->job_start_mid, job->job_start);
	ldms_metric_set_u32(job->set, job->job_end_mid, job->job_end);
	ldms_metric_set_u8(job->set, job->job_state_mid, job->job_state);
	i = 0;
	LIST_FOREACH(t, &job->task_list, entry) {
		ldms_metric_array_set_u32(job->set, job->task_pids_mid, i, t->pid);
		ldms_metric_array_set_u32(job->set, job->task_ranks_mid, i++, t->rank);
	}
	ldms_set_publish(job->set);
	ldms_schema_delete(schema);
	return 0;
 err:
	job->job_state = JOB_PAPI_ERROR;
	if (schema)
		ldms_schema_delete(schema);
	return rc;
}

static const char *usage(struct ldmsd_plugin *self)
{
	return  "config name=kokkos_store path=<path> port=<port_no> log=<path>\n"
		"     path      The path to the root of the SOS container store.\n"
		"     port      The port number to listen on for incoming connections (defaults to 18080).\n"
		"     log       The log file for sample updates (defaults to /var/log/kokkos.log).\n";
}

static ldms_set_t get_set(struct ldmsd_sampler *self)
{
	return NULL;
}

static void sample_job(job_data_t job)
{
	long long values[64];
	papi_event_t ev;
	job_task_t t;
	int ev_idx, task_idx, rc;

	ldms_transaction_begin(job->set);
	ldms_metric_set_u8(job->set, job->job_state_mid, job->job_state);
	task_idx = 0;
	LIST_FOREACH(t, &job->task_list, entry) {
		rc = PAPI_read(t->event_set, values);
		if (rc != PAPI_OK) {
			msglog(LDMSD_LERROR, "papi_sampler [%d]: PAPI error '%s' "
			       "reading EventSet for pid %d.\n", __LINE__,
			       PAPI_strerror(rc), t->pid);
		}
		rc = PAPI_reset(t->event_set);
		ev_idx = 0;
		TAILQ_FOREACH(ev, &job->event_list, entry) {
			ldms_metric_array_set_s64(job->set, ev->mid,
						  task_idx, values[ev_idx++]);
		}
		task_idx++;
	}
 out:
	ldms_transaction_end(job->set);
}

static int sample(struct ldmsd_sampler *self)
{
	job_data_t job;
	struct rbn *rbn;
	pthread_mutex_lock(&job_lock);
	RBT_FOREACH(rbn, &job_tree) {
		job = container_of(rbn, struct job_data, job_ent);
		if (job->job_state != JOB_PAPI_RUNNING)
			continue;
		assert(job->set);
		sample_job(job);
	}
	pthread_mutex_unlock(&job_lock);
	return 0;
}

static int stream_recv_cb(ldmsd_stream_client_t c, void *ctxt,
			  ldmsd_stream_type_t stream_type,
			  const char *msg, size_t msg_len,
			  json_entity_t entity);

static int handle_job_init(job_data_t job, json_entity_t e)
{
	int int_v;
	json_entity_t attr, data, dict;

	attr = json_attr_find(e, "timestamp");
	if (!attr) {
		msglog(LDMSD_LERROR, "papi_sampler: Missing 'timestamp' attribute "
		       "in 'init' event.\n");
		return EINVAL;
	}
	job->job_state = JOB_PAPI_INIT;
	job->job_start = json_value_int(json_attr_value(attr));
	job->job_end = 0;

	data = json_attr_find(e, "data");
	if (!data) {
		msglog(LDMSD_LERROR, "papi_sampler: Missing 'data' attribute "
		       "in 'init' event.\n");
		return EINVAL;
	}
	dict = json_attr_value(data);

	attr = json_attr_find(dict, "local_tasks");
	if (attr) {
		int_v = json_value_int(json_attr_value(attr));
	} else {
		msglog(LDMSD_LERROR, "papi_sampler: Missing 'local_tasks' attribute "
		       "in 'init' event.\n");
		return EINVAL;
	}
	int_v = json_value_int(json_attr_value(attr));
	job->task_count = int_v;
	return 0;
}

static void handle_papi_error(job_data_t job)
{
	long long values[64];
	job_task_t t;
	LIST_FOREACH(t, &job->task_list, entry) {
		if (t->papi_start) {
			PAPI_stop(t->event_set, values);
			t->papi_start = 0;
		}
		if (t->papi_init) {
			PAPI_cleanup_eventset(t->event_set);
			PAPI_destroy_eventset(&t->event_set);
			t->papi_init = 0;
		}
	}
	job->job_state = JOB_PAPI_ERROR;
}

static void handle_task_init(job_data_t job, json_entity_t e)
{
	json_entity_t attr;
	json_entity_t data, dict;
	job_task_t t;
	int rc, task_pid, task_rank;
	papi_event_t ev;

	if (job->job_state != JOB_PAPI_INIT)
		return;
	data = json_attr_find(e, "data");
	if (!data) {
		msglog(LDMSD_LERROR, "papi_sampler: Missing 'data' attribute "
		       "in 'task_init' event.\n");
		return;
	}
	dict = json_attr_value(data);

	attr = json_attr_find(dict, "task_pid");
	if (!attr) {
		msglog(LDMSD_LERROR, "papi_sampler: Missing 'task_pid' attribute "
		       "in 'task_init' event.\n");
		return;
	}
	task_pid = json_value_int(json_attr_value(attr));

	attr = json_attr_find(dict, "task_global_id");
	if (!attr) {
		msglog(LDMSD_LERROR, "papi_sampler: Missing 'task_global_id' attribute "
		       "in 'task_init' event.\n");
		return;
	}
	task_rank = json_value_int(json_attr_value(attr));

	t = malloc(sizeof *t);
	if (!t) {
		msglog(LDMSD_LERROR,
		       "papi_sampler[%d]: Memory allocation failure.\n",
		       __LINE__);
		return;
	}
	t->papi_init = 0;
	t->papi_start = 0;
	t->pid = task_pid;
	t->rank = task_rank;
	t->event_set = PAPI_NULL;
	LIST_INSERT_HEAD(&job->task_list, t, entry);

	job->task_init_count += 1;
	if (job->task_init_count < job->task_count)
		return;

	if (create_metric_set(job))
		return;

	LIST_FOREACH(t, &job->task_list, entry) {
		rc = PAPI_create_eventset(&t->event_set);
		if (rc != PAPI_OK) {
			msglog(LDMSD_LERROR, "papi_sampler [%d]: PAPI error '%s' "
			       "creating EventSet.\n", __LINE__, PAPI_strerror(rc));
			job->job_state = JOB_PAPI_ERROR;
			goto err;
		}
		rc = PAPI_assign_eventset_component(t->event_set, 0);
		if (rc != PAPI_OK) {
			msglog(LDMSD_LERROR, "papi_sampler [%d]: PAPI error '%s' "
			       "assign EventSet to CPU.\n", __LINE__, PAPI_strerror(rc));
			job->job_state = JOB_PAPI_ERROR;
			goto err;
		}
		rc = PAPI_set_multiplex(t->event_set);
		if (rc != PAPI_OK) {
			msglog(LDMSD_LERROR, "papi_sampler [%d]: PAPI error '%s' "
			       "setting multiplex.\n", __LINE__, PAPI_strerror(rc));
			job->job_state = JOB_PAPI_ERROR;
			goto err;
		}
		t->papi_init = 1;
		TAILQ_FOREACH(ev, &job->event_list, entry) {
			rc = PAPI_add_event(t->event_set, ev->event_code);
			if (rc != PAPI_OK) {
				msglog(LDMSD_LERROR, "papi_sampler [%d]: PAPI error '%s' "
				       "adding event '%s'.\n", __LINE__, PAPI_strerror(rc),
				       ev->event_name);
				job->job_state = JOB_PAPI_ERROR;
				goto err;
			}
		}
		rc = PAPI_attach(t->event_set, t->pid);
		if (rc != PAPI_OK) {
			msglog(LDMSD_LERROR, "papi_sampler [%d]: PAPI error '%s' "
			       "attaching EventSet to pid %d.\n", __LINE__,
			       PAPI_strerror(rc), t->pid);
			job->job_state = JOB_PAPI_ERROR;
			goto err;
		}
	}
	LIST_FOREACH(t, &job->task_list, entry) {
		rc = PAPI_start(t->event_set);
		if (rc != PAPI_OK) {
			msglog(LDMSD_LERROR, "papi_sampler [%d]: PAPI error '%s' "
			       "starting EventSet for pid %d.\n", __LINE__,
			       PAPI_strerror(rc), t->pid);
			job->job_state = JOB_PAPI_ERROR;
			goto err;
		}
		t->papi_start = 1;
	}
	job->job_state = JOB_PAPI_RUNNING;
	return;
 err:
	handle_papi_error(job);
}

static void handle_task_exit(job_data_t job, json_entity_t e)
{
	long long values[64];
	json_entity_t attr;
	json_entity_t data = json_attr_find(e, "data");
	json_entity_t dict = json_attr_value(data);
	int rc;
	int task_pid;
	job_task_t t;

	/* Tell sampler to stop sampling */
	job->job_state = JOB_PAPI_STOPPING;

	attr = json_attr_find(dict, "task_pid");
	task_pid = json_value_int(json_attr_value(attr));

	LIST_FOREACH(t, &job->task_list, entry) {
		if (t->pid != task_pid)
			continue;
		if (!t->papi_init)
			return;
		rc = PAPI_stop(t->event_set, values);
		if (rc != PAPI_OK) {
			msglog(LDMSD_LERROR, "papi_sampler [%d]: PAPI error '%s' "
			       "stopping EventSet for pid %d.\n", __LINE__,
			       PAPI_strerror(rc), t->pid);
		}
		t->papi_start = 0;
		rc = PAPI_detach(t->event_set);
		if (rc != PAPI_OK) {
			msglog(LDMSD_LERROR, "papi_sampler[%d]: Error '%s' de-attaching from "
			       "process pid= %d. rc= %d\n", __LINE__,
			       PAPI_strerror(rc), t->pid, rc);
		}
		PAPI_cleanup_eventset(t->event_set);
		PAPI_destroy_eventset(&t->event_set);
		break;
	}
}

static void handle_job_exit(job_data_t job, json_entity_t e)
{
	json_entity_t attr = json_attr_find(e, "timestamp");
	uint64_t timestamp = json_value_int(json_attr_value(attr));

	job->job_state = JOB_PAPI_COMPLETE;
	job->job_end = timestamp;
	release_job_data(job);
}

static int stream_recv_cb(ldmsd_stream_client_t c, void *ctxt,
			  ldmsd_stream_type_t stream_type,
			  const char *msg, size_t msg_len,
			  json_entity_t entity)
{
	int rc;
	json_entity_t event, data, dict, attr;

	if (stream_type != LDMSD_STREAM_JSON) {
		msglog(LDMSD_LDEBUG, "papi_sampler: Unexpected stream type data...ignoring\n");
		msglog(LDMSD_LDEBUG, "papi_sampler:" "%s\n", msg);
		return EINVAL;
	}

	event = json_attr_find(entity, "event");
	if (!event) {
		msglog(LDMSD_LERROR, "papi_sampler: 'event' attribute missing\n");
		goto out_0;
	}

	json_str_t event_name = json_value_str(json_attr_value(event));
	data = json_attr_find(entity, "data");
	if (!data) {
		msglog(LDMSD_LERROR, "papi_sampler: '%s' event is missing "
		       "the 'data' attribute\n", event_name->str);
		goto out_0;
	}
	dict = json_attr_value(data);
	attr = json_attr_find(dict, "job_id");
	if (!attr) {
		msglog(LDMSD_LERROR, "papi_sampler: The event is missing the "
		       "'job_id' attribute.\n");
		goto out_0;
	}

	uint64_t job_id = json_value_int(json_attr_value(attr));
	job_data_t job;

	pthread_mutex_lock(&job_lock);
	if (0 == strncmp(event_name->str, "init", 4)) {
		job = get_job_data(job_id); /* protect against duplicate entries */
		if (!job) {
			job = alloc_job_data(job_id);
			if (!job) {
				msglog(LDMSD_LERROR,
				       "papi_sampler[%d]: Memory allocation failure.\n",
				       __LINE__);
				goto out_1;
			}
			rc = papi_process_config_file(job, papi_config_path, msglog);
			rc = handle_job_init(job, entity);
		}
	} else if (0 == strncmp(event_name->str, "task_init_priv", 14)) {
		job = get_job_data(job_id);
		if (!job) {
			msglog(LDMSD_LERROR, "papi_sampler: '%s' event "
			       "was received for job %d with no job_data\n",
			       event_name->str, job_id);
			goto out_1;
		}
		handle_task_init(job, entity);
	} else if (0 == strncmp(event_name->str, "task_exit", 9)) {
		job = get_job_data(job_id);
		if (!job) {
			msglog(LDMSD_LERROR, "papi_sampler: '%s' event "
			       "was received for job %d with no job_data\n",
			       event_name->str, job_id);
			goto out_1;
		}
		handle_task_exit(job, entity);
	} else if (0 == strncmp(event_name->str, "exit", 4)) {
		job = get_job_data(job_id);
		if (!job) {
			msglog(LDMSD_LERROR, "papi_sampler: '%s' event "
			       "was received for job %d with no job_data\n",
			       event_name->str, job_id);
			goto out_1;
		}
		handle_job_exit(job, entity);
	} else {
		msglog(LDMSD_LDEBUG,
		       "papi_sampler: ignoring event '%s'\n", event_name->str);
	}
 out_1:
	pthread_mutex_unlock(&job_lock);
 out_0:
	return rc;
}

static int config(struct ldmsd_plugin *self, struct attr_value_list *kwl, struct attr_value_list *avl)
{
	char *value;
	value = av_value(avl, "config_path");
	if (!value) {
		msglog(LDMSD_LERROR, "papi_sampler: The 'config_path' option "
		       "must be speciifed.\n", __LINE__);
		return EINVAL;
	}
	papi_config_path = strdup(value);
	if (!papi_config_path) {
		msglog(LDMSD_LERROR, "papi_sampler[%d]: Memory allocation failure.\n", __LINE__);
		return ENOMEM;
	}

	value = av_value(avl, "job_expiry");
	if (value)
		papi_job_expiry = strtol(value, NULL, 0);

	papi_base = base_config(avl, "papi_sampler", "papi-events", msglog);
	if (!papi_base)
		return errno;
	value = av_value(avl, "stream");
	if (!value) {
		papi_stream_name = "slurm";
	} else {
		papi_stream_name = strdup(value);
		if (!papi_stream_name) {
			msglog(LDMSD_LERROR, "papi_sampler[%d]: Memory allocation failure.\n", __LINE__);
			base_del(papi_base);
			papi_base = NULL;
			return EINVAL;
		}
	}
	ldmsd_stream_subscribe(papi_stream_name, stream_recv_cb, self);
	return 0;
}

static void term(struct ldmsd_plugin *self)
{
}

static struct ldmsd_sampler papi_sampler = {
	.base = {
		.name = "papi_sampler",
		.type = LDMSD_PLUGIN_SAMPLER,
		.term = term,
		.config = config,
		.usage = usage,
	},
	.get_set = get_set,
	.sample = sample
};

struct ldmsd_plugin *get_plugin(ldmsd_msg_log_f pf)
{
	msglog = pf;
	return &papi_sampler.base;
}

static int cmp_job_id(void *a, const void *b)
{
	uint64_t a_ = *(uint64_t *)a;
	uint64_t b_ = *(uint64_t *)b;
	if (a_ < b_)
		return -1;	if (a_ > b_)
		return 1;
	return 0;
}

static void __attribute__ ((constructor)) papi_sampler_init(void)
{
	pthread_t cleanup_thread;
	PAPI_library_init(PAPI_VER_CURRENT);
	rbt_init(&job_tree, cmp_job_id);
	LIST_INIT(&job_expiry_list);

	(void)pthread_create(&cleanup_thread, NULL, cleanup_proc, NULL);
}

static void __attribute__ ((destructor)) papi_sampler_term(void)
{
}
