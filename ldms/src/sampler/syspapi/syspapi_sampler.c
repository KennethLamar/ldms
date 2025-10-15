/* -*- c-basic-offset: 8 -*-
 * Copyright (c) 2019,2023,2025 Open Grid Computing, Inc. All rights reserved.
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
/**
 * \file syspapi_sampler.c
 */
#define _GNU_SOURCE
#include <inttypes.h>
#include <unistd.h>
#include <sys/errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <pthread.h>
#include <sys/fcntl.h>
#include <assert.h>

#include "papi.h"
#include "perfmon/pfmlib_perf_event.h"

#include "ldms.h"
#include "ldmsd.h"
#include "ldmsd_plug_api.h"
#include "ldmsd_stream.h"
#include "../sampler_base.h"
#include "../papi/papi_hook.h"

#include "ovis_json/ovis_json.h"

// Define the sampler name
#define SAMP "syspapi_sampler"

// Global variables for the sampler
static int NCPU = 0;              // Number of CPUs on the system
static ldms_set_t set = NULL;     // The LDMS metric set to store data
static int metric_offset;         // Offset for metric indices in the set
static base_data_t base;          // Base sampler data structure
static int cumulative = 0;        // Flag to indicate if counters should be cumulative (1) or reset after read (0)
static int auto_pause = 1;        // Flag to automatically pause/resume sampling based on PAPI task notifications

static ldmsd_stream_client_t syspapi_client = NULL; // Client for subscribing to LDMSD streams
static ovis_log_t mylog;                              // Logger for the sampler

// The event type is determined during initialization
typedef enum {
	PERF_CORE,
	PERF_UNCORE
} perf_event_type;

// Structure to hold information about a PAPI metric
typedef struct syspapi_metric_s {
	TAILQ_ENTRY(syspapi_metric_s) entry;  // Entry for the tail queue
	int midx;                             // Metric index in the LDMS set
	int init_rc;                          // Initialization return code (0 if successful)
	struct perf_event_attr attr;          // perf_event attributes for the metric
	char papi_name[256];                  // PAPI event name (e.g., "PAPI_TOT_CYC")
	char pfm_name[1024];                  // Perfmon event name (PAPI native name)
	perf_event_type type;                 // type of event (core or uncore)
	int pfd[];                            // Array of perf_event file descriptors, one for each CPU
} *syspapi_metric_t;

TAILQ_HEAD(syspapi_metric_list, syspapi_metric_s);
static struct syspapi_metric_list mlist = TAILQ_HEAD_INITIALIZER(mlist);

// Mutex for protecting access to shared data, like the metric list and sampling state.
pthread_mutex_t syspapi_mutex = PTHREAD_MUTEX_INITIALIZER;

// Flags to manage the sampler's state
#define  SYSPAPI_PAUSED      0x1
#define  SYSPAPI_OPENED      0x2
#define  SYSPAPI_CONFIGURED  0x4

int syspapi_flags = 0;

// Helper macros for managing flags
#define FLAG_ON(var, flag)    (var) |= (flag)
#define FLAG_OFF(var, flag)   (var) &= (~flag)
#define FLAG_CHECK(var, flag) ((var) & (flag))

/*
 * create_metric_set: Create a new LDMS metric set based on the metrics
 * in the global `mlist`.
 *
 * @param base: The base sampler data structure.
 * @return 0 on success, or an errno value on failure.
 */
static int
create_metric_set(base_data_t base)
{
	ldms_schema_t schema;
	int rc;
	syspapi_metric_t m;

	// Create the base schema
	schema = base_schema_new(base);
	if (!schema) {
		ovis_log(mylog, OVIS_LERROR,
		       "%s: The schema '%s' could not be created, errno=%d.\n",
		       __FILE__, base->schema_name, errno);
		rc = errno;
		goto err;
	}

	// Get the current number of metrics in the schema. New metrics will be added from this offset.
	metric_offset = ldms_schema_metric_count_get(schema);
	// Iterate through the list of PAPI metrics
	TAILQ_FOREACH(m, &mlist, entry) {
		// Add an array of unsigned 64-bit integers to the schema for each metric.
		// The array size is NCPU, to store a value for each CPU.
		// Use the PAPI metric name as the metric name in LDMS.
		// The array size depends on the event type (NCPU for core, 1 for uncore)
		int arr_size = (m->type == PERF_CORE) ? NCPU : 1;
		rc = ldms_schema_metric_array_add(schema, m->papi_name,
						  LDMS_V_U64_ARRAY, arr_size);
		if (rc < 0) {
			rc = -rc; // rc == -errno
			goto err;
		}
		// Store the index of the newly added metric array
		m->midx = rc;
	}

	// Create the actual metric set from the schema
	set = base_set_new(base);
	if (!set) {
		rc = errno;
		goto err;
	}

	return 0;

 err:
	return rc;
}

/*
 * usage: Returns a string describing the usage of the sampler.
 *
 * @param handle: The LDMSD plugin handle.
 * @return A string containing the usage information.
 */
static const char *
usage(ldmsd_plug_handle_t handle)
{
	return  "config name=" SAMP BASE_CONFIG_SYNOPSIS
		"        cfg_file=CFG_PATH [cumulative=0|1]\n"
		BASE_CONFIG_DESC
		"    cfg_file     The path to configuration file.\n"
		"    cumulative   0 (default) for non-cumulative counters \n"
		"                   (reset after read),\n"
		"                 1 for cumulative counters,\n"
		"    auto_pause   1 (default) to enable pausing when \n"
		"                   getting a notification from papi_sampler,\n"
		"                 0 to ignore papi_sampler notification.\n"
		;
}

/*
 * syspapi_metric_init: Initializes a single PAPI metric by converting its PAPI name
 * to a perfmon name and then to a `perf_event_attr` structure.
 *
 * @param m: A pointer to the `syspapi_metric_s` structure to initialize.
 * @param papi_name: The PAPI event name string.
 * @return 0 on success, or a PAPI error code or errno value on failure.
 */
static int
syspapi_metric_init(syspapi_metric_t m, const char *papi_name)
{
	int len;
	int i, rc, papi_code;
	PAPI_event_info_t papi_info;
	const PAPI_component_info_t *comp_info;
	const char *pfm_name;

	// Copy the PAPI name into the metric structure
	len = snprintf(m->papi_name, sizeof(m->papi_name), "%s", papi_name);
	if (len >= sizeof(m->papi_name)) {
		ovis_log(mylog, OVIS_LERROR, "event name too long: %s\n", papi_name);
		return ENAMETOOLONG;
	}
	m->midx = -1;
	// Initialize perf_event file descriptors to -1
	for (i = 0; i < NCPU; i++) {
		m->pfd[i] = -1;
	}

	// Get the perfmon name from the PAPI name
	rc = PAPI_event_name_to_code((char*)papi_name, &papi_code);
	if (rc != PAPI_OK) {
		ovis_log(mylog, OVIS_LERROR, "PAPI_event_name_to_code for %s failed, "
				 "error: %d\n", papi_name, rc);
		return -1;
	}
	rc = PAPI_get_event_info(papi_code, &papi_info);
	if (rc != PAPI_OK) {
		ovis_log(mylog, OVIS_LERROR, "PAPI_get_event_info for %s failed, "
				 "error: %d\n", papi_name, rc);
		return -1;
	}
	comp_info = PAPI_get_component_info(papi_info.component_index);
	// Check if the event belongs to the `perf_event` or `perf_event_uncore` components
	if (strcmp("perf_event", comp_info->name) == 0) {
		m->type = PERF_CORE;
	} else if (strcmp("perf_event_uncore", comp_info->name) == 0) {
		m->type = PERF_UNCORE;
	} else {
		ovis_log(mylog, OVIS_LERROR, "event %s not supported, "
			"only events in perf_event and perf_event_uncore are supported.\n",
			m->papi_name);
		return EINVAL;
	}
	// Check if the component is disabled
	if (comp_info->disabled) {
		ovis_log(mylog, OVIS_LERROR, "cannot initialize event %s, "
			"PAPI component `%s` disabled, "
			"reason: %s\n",
			m->papi_name, comp_info->name, comp_info->disabled_reason);
		return ENODATA;
	}
	// Handle PAPI preset events
	if (IS_PRESET(papi_code)) {
		// Check for derived events, which are not supported
		if (strcmp(papi_info.derived, "NOT_DERIVED")) {
			/* not NOT_DERIVED ==> this is a derived preset */
			ovis_log(mylog, OVIS_LERROR, "Unsupported PAPI derived "
					 "event: %s\n", m->papi_name);
			return ENOTSUP;
		}
		// Get the native event name
		switch (papi_info.count) {
		case 0:
			/* unavailable */
			ovis_log(mylog, OVIS_LERROR, "no native event describing "
				"papi event %s\n", m->papi_name);
			return ENODATA;
		case 1:
			/* good */
			pfm_name = papi_info.name[0];
			break;
		default:
			/* unsupported */
			ovis_log(mylog, OVIS_LERROR, "%s not supported: the event "
				"contains multiple native events.\n",
				m->papi_name);
			return ENOTSUP;
		}
	// Handle native PAPI events
	} else if (IS_NATIVE(papi_code)) {
		pfm_name = papi_info.symbol;
	} else {
		/* invalid */
		ovis_log(mylog, OVIS_LERROR, "%s is neither a PAPI-preset event "
				"nor a native event.\n", m->papi_name);
		return EINVAL;
	}
	// Copy the perfmon name into the metric structure
	snprintf(m->pfm_name, sizeof(m->pfm_name), "%s", pfm_name);

	// Get the perf_event_attr from the perfmon name
	bzero(&m->attr, sizeof(m->attr));
	m->attr.size = sizeof(m->attr);
	pfm_perf_encode_arg_t pfm_arg = { .attr = &m->attr,
					  .size = sizeof(pfm_arg) };
	// Populate the perf_event_attr using the perfmon library
	rc = pfm_get_os_event_encoding(pfm_name, PFM_PLM0|PFM_PLM3,
				       PFM_OS_PERF_EVENT, &pfm_arg);
	if (rc) {
		ovis_log(mylog, OVIS_LERROR, "pfm_get_os_event_encoding for %s failed, "
				 "error: %d\n", m->papi_name, rc);
	}
	return rc;
}

/*
 * syspapi_metric_add: Creates and adds a metric (by name) to the metric list.
 *
 * @param name: The PAPI event name.
 * @param mlist: The tail queue list to add the metric to.
 * @return 0 on success, or ENOMEM on allocation failure.
 */
static int
syspapi_metric_add(const char *name, struct syspapi_metric_list *mlist)
{
	syspapi_metric_t m;
	// Allocate memory for the metric structure, including the pfd array, based on NCPU. This is the max possible size.
	m = calloc(1, sizeof(*m) + NCPU*sizeof(int));
	if (!m)
		return ENOMEM;
	// Initialize the metric
	m->init_rc = syspapi_metric_init(m, name);
	// Add the new metric to the tail queue
	TAILQ_INSERT_TAIL(mlist, m, entry);
	return 0;
}

/*
 * populate_mlist: Populates the metric list from a comma-separated string of event names.
 *
 * @param events: A comma-separated string of PAPI event names.
 * @param mlist: The tail queue list to populate.
 * @return 0 on success, or an errno value on failure.
 */
static int
populate_mlist(char *events, struct syspapi_metric_list *mlist)
{
	int rc;
	char *tkn, *ptr;
	// Tokenize the string by commas
	tkn = strtok_r(events, ",", &ptr);
	while (tkn) {
		// Add each tokenized event name to the metric list
		rc = syspapi_metric_add(tkn, mlist);
		if (rc)
			return rc;
		tkn = strtok_r(NULL, ",", &ptr);
	}
	return 0;
}

/*
 * purge_mlist: Frees all resources and memory associated with the metric list.
 *
 * @param mlist: The tail queue list to purge.
 */
static void
purge_mlist(struct syspapi_metric_list *mlist)
{
	int i;
	syspapi_metric_t m;
	// Iterate through the list and free each metric
	while ((m = TAILQ_FIRST(mlist))) {
		TAILQ_REMOVE(mlist, m, entry);
		// The loop needs to be NCPU here because the pfd array size is NCPU
		for (i = 0; i < NCPU; i++) {
			if (m->pfd[i] < 0)
				continue;
			// Close any open file descriptors
			close(m->pfd[i]);
		}
		free(m);
	}
}

/*
 * syspapi_close: Disables and closes all perf_event file descriptors for all metrics.
 *
 * @param mlist: The tail queue list of metrics.
 */
static void
syspapi_close(struct syspapi_metric_list *mlist)
{
	syspapi_metric_t m;
	int i;
	TAILQ_FOREACH(m, mlist, entry) {
		// Loop over NCPU for all events to close open FDs
		for (i = 0; i < NCPU; i++) {
			if (m->pfd[i] < 0)
				continue;
			// Disable and close the perf_event file descriptor
			ioctl(m->pfd[i], PERF_EVENT_IOC_DISABLE, 0);
			close(m->pfd[i]);
			m->pfd[i] = -1;
		}
	}
}

/*
 * syspapi_open_error: Logs a user-friendly error message for perf_event_open failures.
 *
 * @param m: The metric that failed to open.
 * @param rc: The errno value from the failed call.
 */
static void
syspapi_open_error(syspapi_metric_t m, int rc)
{
	switch (rc) {
	case EACCES:
	case EPERM:
		ovis_log(mylog, OVIS_LERROR, "perf_event_open() failed (Permission "
			"denied) for %s. Please make sure that ldmsd has "
			"CAP_SYS_ADMIN or /proc/sys/kernel/perf_event_paranoid "
			"is permissive (e.g. -1, see "
			"https://www.kernel.org/doc/Documentation/"
			"sysctl/kernel.txt for more info).\n", m->papi_name);
		break;
	case EBUSY:
		ovis_log(mylog, OVIS_LERROR, "perf_event_open() failed (EBUSY) for %s, "
			"another event already has exclusive access to the "
			"PMU.\n", m->papi_name);
		break;
	case EINVAL:
		ovis_log(mylog, OVIS_LERROR, "perf_event_open() failed (EINVAL) for %s, "
			"invalid event\n", m->papi_name);
		break;
	case EMFILE:
		ovis_log(mylog, OVIS_LERROR, "perf_event_open() failed (EMFILE) for %s, "
			"too many open file descriptors.\n", m->papi_name);
		break;
	case ENODEV:
	case ENOENT:
	case ENOSYS:
	case EOPNOTSUPP:
		ovis_log(mylog, OVIS_LERROR, "perf_event_open() failed (%d) for %s, "
			"event not supported.\n", rc, m->papi_name);
		break;
	case ENOSPC:
		ovis_log(mylog, OVIS_LERROR, "perf_event_open() failed (%d) for %s, "
			"too many events.\n", rc, m->papi_name);
		break;
	default:
		ovis_log(mylog, OVIS_LERROR, "perf_event_open() failed for %s, "
				 "errno: %d\n", m->papi_name, rc);
		break;
	}
}

/*
 * syspapi_open: Opens perf_event file descriptors for all metrics in the list,
 * one per CPU.
 *
 * @param mlist: The tail queue list of metrics.
 * @return 0 on success, or an errno value on a critical failure (e.g., EMFILE).
 */
static int
syspapi_open(struct syspapi_metric_list *mlist)
{
	int i, rc = 0;
	syspapi_metric_t m;

	TAILQ_FOREACH(m, mlist, entry) {
		if (m->init_rc) // Skip metrics that failed to initialize
			continue;

		if (m->type == PERF_CORE) {
			for (i = 0; i < NCPU; i++) {
				// Open the perf_event file descriptor for the specific CPU
				m->pfd[i] = perf_event_open(&m->attr, -1, i, -1, 0);
				if (m->pfd[i] < 0) {
					rc = errno;
					syspapi_open_error(m, rc);
					// A critical error like EMFILE requires a full stop.
					if (rc == EMFILE) {
						syspapi_close(mlist);
						return rc;
					}
				} else {
					ovis_log(mylog, OVIS_LINFO, "%s on CPU %d successfully added\n", m->papi_name, i);
				}
			}
		} else { // PERF_UNCORE
			// For UNCORE, we open a single event for the entire system (0 CPU)
			// The event will be a "group leader" and collect system-wide data.
			m->pfd[0] = perf_event_open(&m->attr, -1, 0, -1, 0);
			if (m->pfd[0] < 0) {
				rc = errno;
				syspapi_open_error(m, rc);
				// A critical error like EMFILE requires a full stop.
				if (rc == EMFILE) {
					syspapi_close(mlist);
					return rc;
				}
			} else {
				ovis_log(mylog, OVIS_LINFO, "%s (UNCORE) successfully added\n", m->papi_name);
			}
		}
	}
	return 0;
}

/*
 * handle_cfg_file: Parses a JSON configuration file to get event names.
 *
 * @param handle: The LDMSD plugin handle.
 * @param cfg_file: The path to the JSON configuration file.
 * @return 0 on success, or an errno value on failure.
 */
static int
handle_cfg_file(ldmsd_plug_handle_t handle, const char *cfg_file)
{
	int rc = 0, fd = -1;
	ssize_t off, rsz, sz;
	char *buff = NULL;
	json_parser_t parser = NULL;
	json_entity_t json = NULL;
	json_entity_t events;
	json_entity_t event;
	json_entity_t schema;

	// Open and read the entire file into a buffer
	fd = open(cfg_file, O_RDONLY);
	if (fd < 0) {
		rc = errno;
		ovis_log(mylog, OVIS_LERROR, "open failed on %s, "
				"errno: %d\n", cfg_file, errno);
		goto out;
	}
	sz = lseek(fd, 0, SEEK_END);
	lseek(fd, 0, SEEK_SET);
	buff = malloc(sz);
	if (!buff) {
		rc = ENOMEM;
		ovis_log(mylog, OVIS_LERROR, "out of memory\n");
		goto out;
	}
	off = 0;
	while (off < sz) {
		rsz = read(fd, buff + off, sz - off);
		if (rsz < 0) {
			rc = errno;
			ovis_log(mylog, OVIS_LERROR, "cfg_file read "
					"error: %d\n", rc);
			goto out;
		}
		if (rsz == 0) {
			rc = EIO;
			ovis_log(mylog, OVIS_LERROR, "unexpected EOF.\n");
			goto out;
		}
		off += rsz;
	}

	// Parse the JSON buffer
	parser = json_parser_new(0);
	if (!parser) {
		rc = ENOMEM;
		goto out;
	}

	rc = json_parse_buffer(parser, buff, sz, &json);
	if (rc) {
		ovis_log(mylog, OVIS_LERROR, "`%s` JSON parse error.\n",
				cfg_file);
		goto out;
	}

	// Look for an optional "schema" key to override the schema name
	schema = json_attr_find(json, "schema");
	if (schema) {
		schema = json_attr_value(schema);
		if (json_entity_type(schema) != JSON_STRING_VALUE) {
			ovis_log(mylog, OVIS_LERROR, "cfg_file error, `schema` "
				"attribute must be a string.\n");
			rc = EINVAL;
			goto out;
		}
		if (base->schema_name)
			free(base->schema_name);
		base->schema_name = strdup(json_value_str(schema)->str);
		if (!base->schema_name) {
			ovis_log(mylog, OVIS_LERROR, "out of memory.\n");
			rc = ENOMEM;
			goto out;
		}
	}

	// Get the "events" list from the JSON file
	events = json_attr_find(json, "events");
	if (!events) {
		ovis_log(mylog, OVIS_LERROR, "cfg_file parse error: `events` "
				"attribute not found.\n");
		rc = ENOENT;
		goto out;
	}
	events = json_attr_value(events);
	if (json_entity_type(events) != JSON_LIST_VALUE) {
		rc = EINVAL;
		ovis_log(mylog, OVIS_LERROR, "cfg_file error: `events` must "
				"be a list of strings.\n");
		goto out;
	}

	// Iterate through the list of event names and add them to the metric list
	event = json_item_first(events);
	while (event) {
		if (json_entity_type(event) != JSON_STRING_VALUE) {
			rc = EINVAL;
			ovis_log(mylog, OVIS_LERROR, "cfg_file error: "
					"entries in `events` list must be "
					"strings.\n");
			goto out;
		}
		rc = syspapi_metric_add(json_value_str(event)->str, &mlist);
		if (rc)
			goto out;
		event = json_item_next(event);
	}

out:
	// Cleanup resources
	if (fd > -1)
		close(fd);
	if (buff)
		free(buff);
	if (parser)
		json_parser_free(parser);
	if (json)
		json_entity_free(json);
	return rc;
}

/*
 * config: Configures the syspapi sampler. This is the main configuration function
 * called by the LDMS daemon.
 *
 * @param handle: The LDMSD plugin handle.
 * @param kwl: The keyword-value list (not used here).
 * @param avl: The attribute-value list containing configuration parameters.
 * @return 0 on success, or an errno value on failure.
 */
static int
config(ldmsd_plug_handle_t handle, struct attr_value_list *kwl,
       struct attr_value_list *avl)
{
	int rc;
	char *value;
	char *events;
	char *cfg_file;

	pthread_mutex_lock(&syspapi_mutex);

	if (set) {
		ovis_log(mylog, OVIS_LERROR, "Set already created.\n");
		rc = EINVAL;
		goto out;
	}

	cfg_file = av_value(avl, "cfg_file"); // Get JSON config file path
	events = av_value(avl, "events");     // Get comma-separated event list

	if (!events && !cfg_file) {
		ovis_log(mylog, OVIS_LERROR, "`events` and `cfg_file` "
					 "not specified\n");
		rc = EINVAL;
		goto out;
	}

	// Initialize base sampler data
	base = base_config(avl, ldmsd_plug_cfg_name_get(handle), SAMP, mylog);
	if (!base) {
		rc = errno;
		goto out;
	}

	// Handle events from a config file if specified
	if (cfg_file) {
		rc = handle_cfg_file(handle, cfg_file);
		if (rc)
			goto err;
	}

	// Handle events from a direct string if specified
	if (events) {
		rc = populate_mlist(events, &mlist);
		if (rc)
			goto err;
	}

	// Get optional `auto_pause` and `cumulative` flags
	value = av_value(avl, "auto_pause");
	if (value) {
		auto_pause = atoi(value);
	}

	value = av_value(avl, "cumulative");
	if (value) {
		cumulative = atoi(value);
	}

	// If not paused, open the perf_event file descriptors
	if (!FLAG_CHECK(syspapi_flags, SYSPAPI_PAUSED)) {
		/* state may be in SYSPAPI_PAUSED, and we won't open fd */
		rc = syspapi_open(&mlist);
		if (rc) /* error has already been logged */
			goto err;
		FLAG_ON(syspapi_flags, SYSPAPI_OPENED);
	}

	// Create the LDMS metric set
	rc = create_metric_set(base);
	if (rc) {
		ovis_log(mylog, OVIS_LERROR, "failed to create a metric set.\n");
		goto err;
	}
	// Mark the sampler as configured
	FLAG_ON(syspapi_flags, SYSPAPI_CONFIGURED);
	rc = 0;
	goto out;
 err:
	// Cleanup on error
	FLAG_OFF(syspapi_flags, SYSPAPI_CONFIGURED);
	FLAG_OFF(syspapi_flags, SYSPAPI_OPENED);
	purge_mlist(&mlist);
	if (base)
		base_del(base);
 out:
	pthread_mutex_unlock(&syspapi_mutex);
	return rc;
}

/*
 * sample: The main sampling function called by the LDMS daemon at each
 * sampling interval.
 *
 * @param handle: The LDMSD plugin handle.
 * @return 0 on success, or an errno value on failure.
 */
static int
sample(ldmsd_plug_handle_t handle)
{
	uint64_t v;
	int i, rc;
	syspapi_metric_t m;

	if (!set) {
		ovis_log(mylog, OVIS_LDEBUG, "plugin not initialized\n");
		return EINVAL;
	}

	pthread_mutex_lock(&syspapi_mutex);
	// Only sample if the perf_event file descriptors are open
	if (!FLAG_CHECK(syspapi_flags, SYSPAPI_OPENED))
		goto out;
	// Start the sampling process
	base_sample_begin(base);

	// Iterate through all metrics
	TAILQ_FOREACH(m, &mlist, entry) {
		if (m->type == PERF_CORE) {
			for (i = 0; i < NCPU; i++) {
				v = 0;
				if (m->pfd[i] >= 0) {
					// Read the counter value from the perf_event file descriptor
					rc = read(m->pfd[i], &v, sizeof(v));
					if (rc <= 0)
						continue;
					// If not cumulative, reset the counter after reading
					if (!cumulative) {
						ioctl(m->pfd[i], PERF_EVENT_IOC_RESET, 0);
					}
				}
				// Store the value in the LDMS metric set
				ldms_metric_array_set_u64(set, m->midx, i, v);
			}
		} else { // PERF_UNCORE
			v = 0;
			if (m->pfd[0] >= 0) {
				// Read the counter value from the perf_event file descriptor
				rc = read(m->pfd[0], &v, sizeof(v));
				if (rc <= 0)
					continue;
				// If not cumulative, reset the counter after reading
				if (!cumulative) {
					ioctl(m->pfd[0], PERF_EVENT_IOC_RESET, 0);
				}
			}
			// Store the value in the LDMS metric set
			// For UNCORE, we set the single value at index 0 of the array.
			// The metric set size is already configured for size 1.
			ldms_metric_array_set_u64(set, m->midx, 0, v);
		}
	}

	base_sample_end(base);
 out:
	pthread_mutex_unlock(&syspapi_mutex);
	return 0;
}

/*
 * __pause: Pauses the sampler by closing all perf_event file descriptors.
 * Assumes the `syspapi_mutex` is already held.
 */
static void
__pause()
{
	if (FLAG_CHECK(syspapi_flags, SYSPAPI_PAUSED))
		return; // Already paused, do nothing
	FLAG_ON(syspapi_flags, SYSPAPI_PAUSED);
	if (FLAG_CHECK(syspapi_flags, SYSPAPI_OPENED)) {
		syspapi_close(&mlist);
		FLAG_OFF(syspapi_flags, SYSPAPI_OPENED);
	}
}

/*
 * __resume: Resumes the sampler by opening all perf_event file descriptors.
 * Assumes the `syspapi_mutex` is already held.
 */
static void
__resume()
{
	if (!FLAG_CHECK(syspapi_flags, SYSPAPI_PAUSED))
		return; // Not paused, do nothing
	FLAG_OFF(syspapi_flags, SYSPAPI_PAUSED);
	// Only resume if the sampler was configured successfully
	if (FLAG_CHECK(syspapi_flags, SYSPAPI_CONFIGURED)) {
		assert(0 == FLAG_CHECK(syspapi_flags, SYSPAPI_OPENED));
		syspapi_open(&mlist);
		FLAG_ON(syspapi_flags, SYSPAPI_OPENED);
	}
}

/*
 * __on_task_init: A hook function called when a new task is initialized.
 * If auto_pause is enabled, it pauses the sampler.
 */
void
__on_task_init()
{
	if (!auto_pause)
		return;
	pthread_mutex_lock(&syspapi_mutex);
	__pause();
	pthread_mutex_unlock(&syspapi_mutex);
}

/*
 * __on_task_empty: A hook function called when a task becomes empty.
 * If auto_pause is enabled, it resumes the sampler.
 */
void
__on_task_empty()
{
	if (!auto_pause)
		return;
	pthread_mutex_lock(&syspapi_mutex);
	__resume();
	pthread_mutex_unlock(&syspapi_mutex);
}

/*
 * __stream_cb: A callback function for the LDMSD stream.
 * It handles "pause" and "resume" commands received over the stream.
 *
 * @param c: The LDMSD stream client.
 * @param ctxt: The context pointer (not used).
 * @param stream_type: The type of data in the stream.
 * @param data: The data received.
 * @param data_len: The length of the data.
 * @param entity: The JSON entity if the data is JSON.
 * @return 0 to continue processing.
 */
static int
__stream_cb(ldmsd_stream_client_t c, void *ctxt,
		ldmsd_stream_type_t stream_type,
		const char *data, size_t data_len,
		json_entity_t entity)
{
	if (stream_type != LDMSD_STREAM_STRING)
		return 0;
	pthread_mutex_lock(&syspapi_mutex);
	if (strncmp("pause", data, 5)  == 0) {
		/* "pause\n" or "pausefoo" would pause too */
		__pause();
	}
	if (strncmp("resume", data, 6)  == 0) {
		/* "resume\n" or "resumebar" would resume too */
		__resume();
	}
	pthread_mutex_unlock(&syspapi_mutex);
	return 0;
}

/*
 * constructor: The sampler's constructor. Initializes PAPI, gets the number of CPUs,
 * subscribes to a stream for remote control, and registers hooks for
 * task-based pausing.
 *
 * @param handle: The LDMSD plugin handle.
 * @return 0 on success, or a PAPI error code on failure.
 */
static int constructor(ldmsd_plug_handle_t handle)
{
	int rc;
	mylog = ldmsd_plug_log_get(handle);
	// Initialize the PAPI library
	rc = PAPI_library_init(PAPI_VER_CURRENT);
	if (rc < 0) {
		ovis_log(mylog, OVIS_LERROR, "Error %d attempting to initialize "
			     "the PAPI library.\n", rc);
	}
	// Get the number of online CPUs
	NCPU = sysconf(_SC_NPROCESSORS_CONF);
	// Subscribe to a stream for "pause" and "resume" commands
	syspapi_client = ldmsd_stream_subscribe("syspapi_stream", __stream_cb, NULL);
	if (!syspapi_client) {
		ovis_log(mylog, OVIS_LERROR, "failed to subscribe to 'syspapi_stream' "
			     "stream, errno: %d\n", errno);
	}
	// Register hooks for automatic pausing/resuming
	register_task_init_hook(__on_task_init);
	register_task_empty_hook(__on_task_empty);

    return 0;
}

/*
 * destructor: The sampler's destructor. Cleans up all resources.
 *
 * @param handle: The LDMSD plugin handle.
 */
static void destructor(ldmsd_plug_handle_t handle)
{
	pthread_mutex_lock(&syspapi_mutex);
	if (base)
		base_del(base);
	if (set)
		ldms_set_delete(set);
	set = NULL;
	// Purge the metric list and close all perf_event file descriptors
	purge_mlist(&mlist);
	FLAG_OFF(syspapi_flags, SYSPAPI_CONFIGURED);
	FLAG_OFF(syspapi_flags, SYSPAPI_OPENED);
	pthread_mutex_unlock(&syspapi_mutex);
	// Close the LDMS stream client
	if (syspapi_client) {
		ldmsd_stream_close(syspapi_client);
		syspapi_client = NULL;
	}
	// Shutdown the PAPI library
	PAPI_shutdown();
}

// The main plugin interface structure, linking the sampler functions to LDMSD.
struct ldmsd_sampler ldmsd_plugin_interface = {
	.base = {
		.type = LDMSD_PLUGIN_SAMPLER,
		.config = config,
		.usage = usage,
		.constructor = constructor,
		.destructor = destructor,
	},
	.sample = sample,
};