/* -*- c-basic-offset: 8 -*-
 * Copyright (c) 2015-2016 Open Grid Computing, Inc. All rights reserved.
 * Copyright (c) 2015-2016 Sandia Corporation. All rights reserved.
 *
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

#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <coll/rbt.h>
#include <ovis_util/util.h>
#include "event.h"
#include "ldms.h"
#include "ldmsd.h"
#include "ldms_xprt.h"
#include "config.h"

void ldmsd_updtr___del(ldmsd_cfgobj_t obj)
{
	ldmsd_updtr_t updtr = (ldmsd_updtr_t)obj;
	ldmsd_name_match_t match;
	while (!LIST_EMPTY(&updtr->match_list) ) {
		match = LIST_FIRST(&updtr->match_list);
		if (match->regex_str)
			free(match->regex_str);
		regfree(&match->regex);
		LIST_REMOVE(match, entry);
		free(match);
	}
	ldmsd_prdcr_ref_t prdcr_ref;
	while (!LIST_EMPTY(&updtr->prdcr_list)) {
		prdcr_ref = LIST_FIRST(&updtr->prdcr_list);
		ldmsd_cfgobj_put(&prdcr_ref->prdcr->obj);
		LIST_REMOVE(prdcr_ref, entry);
		free(prdcr_ref);
	}
	ldmsd_cfgobj___del(obj);
}

#ifdef LDMSD_UPDATE_TIME
void __updt_time_get(struct ldmsd_updt_time *updt_time)
{
	__sync_fetch_and_add(&updt_time->ref, 1);
}

void __updt_time_put(struct ldmsd_updt_time *updt_time)
{
	if (0 == __sync_sub_and_fetch(&updt_time->ref, 1)) {
		if (updt_time->update_start.tv_sec != 0) {
			struct timeval end;
			gettimeofday(&end, NULL);
			updt_time->updtr->duration =
				ldmsd_timeval_diff(&updt_time->update_start,
						&end);
		} else {
			updt_time->updtr->duration = -1;
		}
		free(updt_time);
	}
}
#endif /* LDMSD_UDPATE_TIME */


static void updtr_update_cb(ldms_t t, ldms_set_t set, int status, void *arg)
{
	uint64_t gn;
	ldmsd_prdcr_set_t prd_set = arg;
	pthread_mutex_lock(&prd_set->lock);
#ifdef LDMSD_UPDATE_TIME
	struct timeval end;
	gettimeofday(&end, NULL);
	prd_set->updt_duration = ldmsd_timeval_diff(&prd_set->updt_start, &end);
	__updt_time_put(prd_set->updt_time);
#endif /* LDMSD_UPDATE_TIME */
	ldmsd_log(LDMSD_LDEBUG, "Update complete for Set %s with status %d\n",
					prd_set->inst_name, status);
	if (status) {
		goto out;
	}

	if (!ldms_set_is_consistent(set)) {
		ldmsd_log(LDMSD_LINFO, "Set %s is inconsistent.\n", prd_set->inst_name);
		goto set_ready;
	}

	gn = ldms_set_data_gn_get(set);
	if (prd_set->last_gn == gn) {
		ldmsd_log(LDMSD_LINFO, "Set %s oversampled.\n", prd_set->inst_name, prd_set->last_gn);
		goto set_ready;
	}
	prd_set->last_gn = gn;

	ldmsd_strgp_ref_t str_ref;
	LIST_FOREACH(str_ref, &prd_set->strgp_list, entry) {
		ldmsd_strgp_t strgp = str_ref->strgp;

		ldmsd_strgp_lock(strgp);
		strgp->update_fn(strgp, prd_set);
		ldmsd_strgp_unlock(strgp);
	}
set_ready:
	prd_set->state = LDMSD_PRDCR_SET_STATE_READY;
out:
	pthread_mutex_unlock(&prd_set->lock);
	ldmsd_prdcr_set_ref_put(prd_set); /* The ref was taken before update */
	return;
}

static int schedule_set_updates(ldmsd_prdcr_set_t prd_set, ldmsd_updtr_t updtr)
{
	int rc;
	/* The reference will be put back in update_cb */
	ldmsd_log(LDMSD_LDEBUG, "Schedule an update for set %s\n",
					prd_set->inst_name);
	ldmsd_prdcr_set_ref_get(prd_set);
	prd_set->state = LDMSD_PRDCR_SET_STATE_UPDATING;
#ifdef LDMSD_UPDATE_TIME
	prd_set->updt_time = updtr->curr_updt_time;
	__updt_time_get(prd_set->updt_time);
	gettimeofday(&prd_set->updt_start, NULL);
	if (prd_set->updt_time->update_start.tv_sec == 0)
		prd_set->updt_time->update_start = prd_set->updt_start;
	rc = ldms_xprt_update(prd_set->set, updtr_update_cb, prd_set);
	if (rc) {
		__updt_time_put(prd_set->updt_time);
		ldmsd_log(LDMSD_LINFO, "Synchronous error %d: Updating Set %s\n",
						rc, prd_set->inst_name);
		ldmsd_prdcr_set_ref_put(prd_set);
	}
#else /* LDMSD_UPDATE_TIME */
	rc = ldms_xprt_update(prd_set->set, updtr_update_cb, prd_set);
	if (rc) {
		ldmsd_log(LDMSD_LINFO, "Synchronous error %d: Updating Set %s\n",
						rc, prd_set->inst_name);
		ldmsd_prdcr_set_ref_put(prd_set);
	}
#endif /* LDMSD_UPDATE_TIME */
	return rc;
}

static void schedule_prdcr_updates(ldmsd_updtr_t updtr,
				   ldmsd_prdcr_t prdcr, ldmsd_name_match_t match)
{
#ifdef LDMSD_UPDATE_TIME
	struct timeval start, end;
	gettimeofday(&start, NULL);
#endif /* LDMSD_UPDATE_TIME */
	ldmsd_prdcr_lock(prdcr);
	if (prdcr->conn_state != LDMSD_PRDCR_STATE_CONNECTED)
		goto out;
	ldmsd_prdcr_set_t prd_set;
	for (prd_set = ldmsd_prdcr_set_first(prdcr); prd_set;
	     prd_set = ldmsd_prdcr_set_next(prd_set)) {
		const char *str;
		if (prd_set->state == LDMSD_PRDCR_SET_STATE_UPDATING) {
			ldmsd_log(LDMSD_LINFO, "%s: Set %s: "
				"there is an outstanding update.\n",
				__func__, prd_set->inst_name);
		}
		if (prd_set->state != LDMSD_PRDCR_SET_STATE_READY)
			continue;
		/* If a match condition is not specified, everything matches */
		if (!match) {
			schedule_set_updates(prd_set, updtr);
			continue;
		}
		if (match->selector == LDMSD_NAME_MATCH_INST_NAME)
			str = prd_set->inst_name;
		else
			str = prd_set->schema_name;
		int rc = regexec(&match->regex, str, 0, NULL, 0);
		if (!rc) {
			schedule_set_updates(prd_set, updtr);
		}
	}
out:
	ldmsd_prdcr_unlock(prdcr);
#ifdef LDMSD_UPDATE_TIME
	gettimeofday(&end, NULL);
	prdcr->sched_update_time = ldmsd_timeval_diff(&start, &end);
#endif /* LDMSD_UPDATE_tIME */
}

static void schedule_updates(ldmsd_updtr_t updtr)
{
	ldmsd_name_match_t match;

#ifdef LDMSD_UPDATE_TIME
	ldmsd_log(LDMSD_LDEBUG, "Updater %s: schedule an update\n",
						updtr->obj.name);
	struct timeval start;
	struct ldmsd_updt_time *updt_time = calloc(1, sizeof(*updt_time));
	__updt_time_get(updt_time);
	updt_time->updtr = updtr;
	updtr->curr_updt_time = updt_time;
	updtr->duration = -1;
	updtr->sched_duration = -1;
	gettimeofday(&start, NULL);
	updt_time->sched_start = start;
#endif /* LDMSD_UPDATE_TIME */

	if (!LIST_EMPTY(&updtr->match_list)) {
		LIST_FOREACH(match, &updtr->match_list, entry) {
			ldmsd_prdcr_ref_t ref;
			LIST_FOREACH(ref, &updtr->prdcr_list, entry)
				schedule_prdcr_updates(updtr, ref->prdcr, match);
		}
	} else {
		ldmsd_prdcr_ref_t ref;
		LIST_FOREACH(ref, &updtr->prdcr_list, entry)
			schedule_prdcr_updates(updtr, ref->prdcr, NULL);
	}
#ifdef LDMSD_UPDATE_TIME
	struct timeval end;
	gettimeofday(&end, NULL);
	updtr->sched_duration = ldmsd_timeval_diff(&start, &end);
	updtr->curr_updt_time = NULL;
	__updt_time_put(updt_time);
#endif /* LDMSD_UPDATE_TIME */
}

static void updtr_task_cb(ldmsd_task_t task, void *arg)
{
	ldmsd_updtr_t updtr = arg;

	ldmsd_updtr_lock(updtr);
	switch (updtr->state) {
	case LDMSD_UPDTR_STATE_STOPPED:
		break;
	case LDMSD_UPDTR_STATE_RUNNING:
		schedule_updates(updtr);
		break;
	}
	ldmsd_updtr_unlock(updtr);
}

ldmsd_updtr_t
ldmsd_updtr_new(const char *name)
{
	struct ldmsd_updtr *updtr;

	updtr = (struct ldmsd_updtr *)
		ldmsd_cfgobj_new(name, LDMSD_CFGOBJ_UPDTR,
				 sizeof *updtr, ldmsd_updtr___del);
	if (!updtr)
		return NULL;

	updtr->state = LDMSD_UPDTR_STATE_STOPPED;
	ldmsd_task_init(&updtr->task);
	LIST_INIT(&updtr->prdcr_list);
	LIST_INIT(&updtr->match_list);
	ldmsd_cfgobj_unlock(&updtr->obj);
	return updtr;
}

int ldmsd_updtr_del(const char *updtr_name)
{
	int rc = 0;
	ldmsd_updtr_t updtr = ldmsd_updtr_find(updtr_name);
	if (!updtr) {
		return ENOENT;
	}
	ldmsd_updtr_lock(updtr);
	if (updtr->state != LDMSD_UPDTR_STATE_STOPPED) {
		rc = EBUSY;
		goto out;
	}
	if (ldmsd_cfgobj_refcount(&updtr->obj) > 2) {
		rc = EBUSY;
		goto out;
	}
	/* Make sure any outstanding callbacks are complete */
	ldmsd_task_join(&updtr->task);
	/* Put the find reference */
	ldmsd_updtr_put(updtr);
	/* Drop the lock and drop the create reference */
	ldmsd_updtr_unlock(updtr);
	ldmsd_updtr_put(updtr);
	return 0;
out:
	ldmsd_updtr_put(updtr);
	ldmsd_updtr_unlock(updtr);
	return rc;
}

int ldmsd_updtr_start(const char *updtr_name, const char *interval_str,
						const char *offset_str)
{
	int rc = 0;
	ldmsd_updtr_t updtr = ldmsd_updtr_find(updtr_name);
	if (!updtr)
		return ENOENT;

	ldmsd_updtr_lock(updtr);
	if (updtr->state != LDMSD_UPDTR_STATE_STOPPED) {
		rc = EBUSY;
		goto out_1;
	}
	updtr->state = LDMSD_UPDTR_STATE_RUNNING;
	if (interval_str)
		updtr->updt_intrvl_us = strtol(interval_str, NULL, 0);
	if (offset_str) {
		updtr->updt_offset_us = strtol(offset_str, NULL, 0);
		updtr->updt_task_flags = LDMSD_TASK_F_SYNCHRONOUS;
	}
	if ( updtr->updt_intrvl_us == 0) {
		ldmsd_log(LDMSD_LERROR, "updtr_start fails for interval = 0\n");
		rc = EINVAL;
		goto out_1;
	}

	ldmsd_task_start(&updtr->task, updtr_task_cb, updtr,
			 updtr->updt_task_flags,
			 updtr->updt_intrvl_us, updtr->updt_offset_us);
out_1:
	ldmsd_updtr_unlock(updtr);
	ldmsd_updtr_put(updtr);
	return rc;
}

int ldmsd_updtr_stop(const char *updtr_name)
{
	int rc = 0;
	ldmsd_updtr_t updtr = ldmsd_updtr_find(updtr_name);
	if (!updtr)
		return ENOENT;

	ldmsd_updtr_lock(updtr);
	if (updtr->state != LDMSD_UPDTR_STATE_RUNNING) {
		rc = EBUSY;
		goto out_1;

	}
	updtr->state = LDMSD_UPDTR_STATE_STOPPED;
	ldmsd_task_stop(&updtr->task);
	ldmsd_task_join(&updtr->task);
out_1:
	ldmsd_updtr_unlock(updtr);
	ldmsd_updtr_put(updtr);
	return rc;
}

ldmsd_updtr_t ldmsd_updtr_first()
{
	return (ldmsd_updtr_t)ldmsd_cfgobj_first(LDMSD_CFGOBJ_UPDTR);
}

ldmsd_updtr_t ldmsd_updtr_next(struct ldmsd_updtr *updtr)
{
	return (ldmsd_updtr_t)ldmsd_cfgobj_next(&updtr->obj);
}

ldmsd_name_match_t ldmsd_updtr_match_first(ldmsd_updtr_t updtr)
{
	return LIST_FIRST(&updtr->match_list);
}

ldmsd_name_match_t ldmsd_updtr_match_next(ldmsd_name_match_t cmp)
{
	return LIST_NEXT(cmp, entry);
}

ldmsd_prdcr_ref_t ldmsd_updtr_prdcr_first(ldmsd_updtr_t updtr)
{
	return LIST_FIRST(&updtr->prdcr_list);
}

ldmsd_prdcr_ref_t ldmsd_updtr_prdcr_next(ldmsd_prdcr_ref_t ref)
{
	return LIST_NEXT(ref, entry);
}

int cmd_updtr_add(char *replybuf, struct attr_value_list *avl, struct attr_value_list *kwl)
{
	char *attr, *name, *interval, *offset;

	attr = "name";
	name = av_value(avl, attr);
	if (!name)
		goto einval;

	attr = "interval";
	interval = av_value(avl, attr);
	if (!interval)
		goto einval;

	/* Optional attributes */
	offset = av_value(avl, "offset");

	ldmsd_updtr_t updtr = ldmsd_updtr_new(name);
	if (!updtr) {
		if (errno == EEXIST)
			goto eexist;
		else if (errno == ENOMEM)
			goto enomem;
		goto out;
	}
	updtr->updt_intrvl_us = strtol(interval, NULL, 0);
	if ( updtr->updt_intrvl_us == 0) {
		ldmsd_log(LDMSD_LERROR, "updtr_add fails for interval = 0\n");
		goto out;
	}

	if (offset) {
		updtr->updt_offset_us = strtol(offset, NULL, 0);
		updtr->updt_task_flags = LDMSD_TASK_F_SYNCHRONOUS;
	} else
		updtr->updt_task_flags = 0;
	sprintf(replybuf, "0\n");
	goto out;
enomem:
	sprintf(replybuf, "%dOut of memory.\n", ENOMEM);
	goto out;
eexist:
	sprintf(replybuf, "%dThe updtr %s already exists.\n", EEXIST, name);
	goto out;
einval:
	sprintf(replybuf, "%dThe attribute '%s' is required.\n", EINVAL, attr);
out:
	return 0;
}

ldmsd_name_match_t updtr_find_match_ex(ldmsd_updtr_t updtr,
				       enum ldmsd_name_match_sel sel,
				       const char *ex)
{
	ldmsd_name_match_t match;
	LIST_FOREACH(match, &updtr->match_list, entry) {
		if (match->selector != sel)
			continue;
		if (0 == strcmp(match->regex_str, ex))
			return match;
	}
	return NULL;
}

int ldmsd_updtr_match_add(const char *updtr_name, const char *regex_str,
		const char *selector_str, char *rep_buf, size_t rep_len)
{
	int rc = 0;
	ldmsd_updtr_t updtr = ldmsd_updtr_find(updtr_name);
	if (!updtr)
		return ENOENT;

	ldmsd_updtr_lock(updtr);
	if (updtr->state != LDMSD_UPDTR_STATE_STOPPED) {
		rc = EBUSY;
		goto out_1;
	}
	ldmsd_name_match_t match = calloc(1, sizeof *match);
	if (!match) {
		rc = ENOMEM;
		goto out_1;
	}
	match->regex_str = strdup(regex_str);
	if (!match->regex_str) {
		rc = ENOMEM;
		goto out_2;
	}

	if (!selector_str)
		match->selector = LDMSD_NAME_MATCH_INST_NAME;
	else if (0 == strcasecmp(selector_str, "schema"))
		match->selector = LDMSD_NAME_MATCH_SCHEMA_NAME;
	else if (0 == strcasecmp(selector_str, "inst"))
		match->selector = LDMSD_NAME_MATCH_INST_NAME;
	else {
		rc = EINVAL;
		goto out_3;
	}

	if (ldmsd_compile_regex(&match->regex, regex_str, rep_buf, rep_len))
		goto out_3;

	LIST_INSERT_HEAD(&updtr->match_list, match, entry);
	goto out_1;

out_3:
	free(match->regex_str);
out_2:
	free(match);
out_1:
	ldmsd_updtr_unlock(updtr);
	ldmsd_updtr_put(updtr);
	return rc;
}

int cmd_updtr_match_add(char *replybuf, struct attr_value_list *avl, struct attr_value_list *kwl)
{
	char *attr, *updtr_name, *regex_str, *selector_str;

	attr = "name";
	updtr_name = av_value(avl, attr);
	if (!updtr_name) {
		sprintf(replybuf, "%dThe updater name must be specified\n", EINVAL);
		goto out_0;
	}
	regex_str = av_value(avl, "regex");
	if (!regex_str) {
		sprintf(replybuf,
			"%dThe regular expression must be specified.\n",
			EINVAL);
		goto out_0;
	}
	selector_str = av_value(avl, "match"); /* Can be null, defaults to INST_NAME */
	int rc = ldmsd_updtr_match_add(updtr_name, regex_str, selector_str,
						replybuf, sizeof(replybuf));
	if (!rc) {
		strcpy(replybuf, "0\n");
	} else if (rc == ENOENT) {
		sprintf(replybuf, "%dThe updater specified does not exist\n", ENOENT);
	} else if (rc == EBUSY) {
		sprintf(replybuf, "%dConfiguration changes cannot be made "
			"while the updater is running\n", EBUSY);
	} else if (rc == ENOMEM) {
		sprintf(replybuf, "%dOut of memory.\n", ENOMEM);
	} else if (rc == EINVAL) {
		sprintf(replybuf, "%dThe value '%s' for match= is invalid.\n",
			EINVAL, selector_str);
	}
out_0:
	return 0;
}

int ldmsd_updtr_match_del(const char *updtr_name, const char *regex_str,
						const char *selector_str)
{
	int rc = 0;
	enum ldmsd_name_match_sel sel;
	if (!selector_str)
		sel = LDMSD_NAME_MATCH_INST_NAME;
	else if (0 == strcasecmp(selector_str, "inst"))
		sel = LDMSD_NAME_MATCH_INST_NAME;
	else if (0 == strcasecmp(selector_str, "schema"))
		sel = LDMSD_NAME_MATCH_SCHEMA_NAME;
	else {
		return EINVAL;
	}

	ldmsd_updtr_t updtr = ldmsd_updtr_find(updtr_name);
	if (!updtr)
		return ENOENT;

	ldmsd_updtr_lock(updtr);
	if (updtr->state != LDMSD_UPDTR_STATE_STOPPED) {
		rc = EBUSY;
		goto out_1;
	}
	ldmsd_name_match_t match = updtr_find_match_ex(updtr, sel, regex_str);
	if (!match) {
		rc = -ENOENT;
		goto out_1;
	}
	LIST_REMOVE(match, entry);
	regfree(&match->regex);
	free(match->regex_str);
	free(match);
out_1:
	ldmsd_updtr_unlock(updtr);
	ldmsd_updtr_put(updtr);
	return rc;
}

int cmd_updtr_match_del(char *replybuf, struct attr_value_list *avl, struct attr_value_list *kwl)
{
	char *updtr_name, *regex_str, *selector_str;

	updtr_name = av_value(avl, "name");
	if (!updtr_name) {
		sprintf(replybuf, "%dThe updater name must be specified\n", EINVAL);
		goto out_0;
	}
	regex_str = av_value(avl, "regex");
	if (!regex_str) {
		sprintf(replybuf,
			"%dThe regular expression must be specified.\n",
			EINVAL);
		goto out_0;
	}
	selector_str = av_value(avl, "match"); /* Can be null, defaults to INST_NAME */

	int rc = ldmsd_updtr_match_del(updtr_name, regex_str, selector_str);
	if (rc == ENOENT) {
		sprintf(replybuf, "%dThe updater specified does not exist\n", ENOENT);
	} else if (rc == EBUSY) {
		sprintf(replybuf, "%dConfiguration changes cannot be made "
			"while the updater is running\n", EBUSY);
	} else if (rc == -ENOENT) {
		sprintf(replybuf,
			"%dThe specified regex does not match any condition\n",
			ENOENT);
	} else {
		strcpy(replybuf, "0\n");
	}
out_0:
	return 0;
}

int cmd_updtr_start(char *replybuf, struct attr_value_list *avl, struct attr_value_list *kwl)
{
	char *updtr_name, *interval_str, *offset_str;
	updtr_name = av_value(avl, "name");
	if (!updtr_name) {
		sprintf(replybuf, "%dThe updater name must be specified\n", EINVAL);
		return 0;
	}
	interval_str = av_value(avl, "interval"); /* Can be null if we're not changing it */
	offset_str = av_value(avl, "offset"); /* Can be null if we're not changing it */

	int rc = ldmsd_updtr_start(updtr_name, interval_str, offset_str);
	if (rc == ENOENT) {
		sprintf(replybuf, "%dThe updater specified does "
				"not exist\n", ENOENT);
	} else if (rc == EBUSY) {
		sprintf(replybuf, "%dThe updater is already running\n", EBUSY);
	} else if (rc == EINVAL) {
		sprintf(replybuf, "%dThe updater interval is 0\n", EINVAL);
	} else {
		sprintf(replybuf, "0\n");
	}
	return 0;
}

int cmd_updtr_stop(char *replybuf, struct attr_value_list *avl, struct attr_value_list *kwl)
{
	char *updtr_name;

	updtr_name = av_value(avl, "name");
	if (!updtr_name) {
		sprintf(replybuf, "%dThe updater name must be specified\n", EINVAL);
		goto out_0;
	}

	int rc = ldmsd_updtr_stop(updtr_name);
	if (rc == ENOENT) {
		sprintf(replybuf, "%dThe updater specified does not exist\n", ENOENT);
	} else if (rc == EBUSY) {
		sprintf(replybuf, "%dThe updater is already stopped\n", EBUSY);
	} else {
		sprintf(replybuf, "0\n");
	}

out_0:
	return 0;
}

int cmd_updtr_del(char *replybuf, struct attr_value_list *avl, struct attr_value_list *kwl)
{
	char *updtr_name;
	updtr_name = av_value(avl, "name");
	if (!updtr_name) {
		sprintf(replybuf, "%dThe updater name must be specified\n", EINVAL);
		goto out_0;
	}
	int rc = ldmsd_updtr_del(updtr_name);
	if (rc == ENOENT) {
		sprintf(replybuf, "%dThe updater specified does not exist\n", ENOENT);
	} else if (rc == EBUSY) {
		sprintf(replybuf, "%dThe updater is in use.\n", EBUSY);
	} else {
		sprintf(replybuf, "0\n");
	}

out_0:
	return 0;
}

ldmsd_prdcr_ref_t prdcr_ref_new(ldmsd_prdcr_t prdcr)
{
	ldmsd_prdcr_ref_t ref = calloc(1, sizeof *ref);
	if (ref)
		ref->prdcr = ldmsd_prdcr_get(prdcr);
	return ref;
}

ldmsd_prdcr_ref_t prdcr_ref_find(ldmsd_updtr_t updtr, const char *name)
{
	ldmsd_prdcr_ref_t ref;
	LIST_FOREACH(ref, &updtr->prdcr_list, entry)
		if (0 == strcmp(name, ref->prdcr->obj.name))
			return ref;
	return NULL;
}

ldmsd_prdcr_ref_t prdcr_ref_find_regex(ldmsd_updtr_t updtr, regex_t *regex)
{
	ldmsd_prdcr_ref_t ref;
	LIST_FOREACH(ref, &updtr->prdcr_list, entry)
		if (0 == regexec(regex, ref->prdcr->obj.name, 0, NULL, 0))
			return ref;
	return NULL;
}

int ldmsd_updtr_prdcr_add(const char *updtr_name, const char *prdcr_regex,
						char *rep_buf, size_t rep_len)
{
	regex_t regex;
	ldmsd_updtr_t updtr;
	ldmsd_prdcr_t prdcr;
	int rc;

	rc = ldmsd_compile_regex(&regex, prdcr_regex, rep_buf, rep_len);
	if (rc)
		return rc;

	updtr = ldmsd_updtr_find(updtr_name);
	if (!updtr) {
		sprintf(rep_buf, "%dThe updater specified does not "
						"exist\n", ENOENT);
		regfree(&regex);
		return ENOENT;
	}

	ldmsd_updtr_lock(updtr);
	if (updtr->state != LDMSD_UPDTR_STATE_STOPPED) {
		sprintf(rep_buf, "%dConfiguration changes cannot be made "
				"while the updater is running\n", EBUSY);
		rc = EBUSY;
		goto out_1;
	}
	ldmsd_cfg_lock(LDMSD_CFGOBJ_PRDCR);
	for (prdcr = ldmsd_prdcr_first(); prdcr; prdcr = ldmsd_prdcr_next(prdcr)) {
		if (regexec(&regex, prdcr->obj.name, 0, NULL, 0))
			continue;
		/* See if this match is already in the list */
		ldmsd_prdcr_ref_t ref = prdcr_ref_find(updtr, prdcr->obj.name);
		if (ref)
			continue;
		ref = prdcr_ref_new(prdcr);
		if (!ref) {
			rc = ENOMEM;
			sprintf(rep_buf, "%dMemory allocation failure.\n", ENOMEM);
			ldmsd_prdcr_put(prdcr);
			ldmsd_cfg_unlock(LDMSD_CFGOBJ_PRDCR);
			goto out_1;
		}
		LIST_INSERT_HEAD(&updtr->prdcr_list, ref, entry);
	}
	ldmsd_cfg_unlock(LDMSD_CFGOBJ_PRDCR);
	sprintf(rep_buf, "0\n");
out_1:
	regfree(&regex);
	ldmsd_updtr_unlock(updtr);
	ldmsd_updtr_put(updtr);
	return rc;
}

int ldmsd_updtr_prdcr_del(const char *updtr_name, const char *prdcr_regex,
						char *rep_buf, size_t rep_len)
{
	int rc = 0;
	regex_t regex;
	ldmsd_prdcr_ref_t ref;

	rc = ldmsd_compile_regex(&regex, prdcr_regex, rep_buf, rep_len);
	if (rc)
		goto out_0;

	ldmsd_updtr_t updtr = ldmsd_updtr_find(updtr_name);
	if (!updtr) {
		rc = ENOENT;
		regfree(&regex);
		goto out_0;
	}
	ldmsd_updtr_lock(updtr);
	if (updtr->state != LDMSD_UPDTR_STATE_STOPPED) {
		rc = EBUSY;
		goto out_1;
	}
	for (ref = prdcr_ref_find_regex(updtr, &regex);
	     ref; ref = prdcr_ref_find_regex(updtr, &regex)) {
		LIST_REMOVE(ref, entry);
		ldmsd_prdcr_put(ref->prdcr);
		free(ref);
	}
out_1:
	regfree(&regex);
	ldmsd_updtr_unlock(updtr);
	ldmsd_updtr_put(updtr);
out_0:
	return rc;
}

int cmd_updtr_prdcr_del(char *replybuf, struct attr_value_list *avl, struct attr_value_list *kwl)
{
	char *updtr_name, *prdcr_regex;
	updtr_name = av_value(avl, "name");
	if (!updtr_name) {
		sprintf(replybuf, "%dThe updater name must be specified\n", EINVAL);
		goto out_0;
	}
	prdcr_regex = av_value(avl, "regex");
	if (!prdcr_regex) {
		sprintf(replybuf, "%dA producer regular expression must be specified\n", EINVAL);
		goto out_0;
	}

	int rc = ldmsd_updtr_prdcr_del(updtr_name, prdcr_regex,
					replybuf, sizeof(replybuf));
	if (rc) {
		if (rc == ENOMEM) {
			sprintf(replybuf, "%dThe updater specified does not "
								"exist\n", ENOENT);
		} else if (rc == EBUSY) {
			sprintf(replybuf, "%dConfiguration changes cannot be "
				"made while the updater is running\n", EBUSY);
		} else if (rc == ENOENT) {
			sprintf(replybuf, "%dThe updater specified does not "
							"exist\n", ENOENT);
		}
	} else {
		sprintf(replybuf, "0\n");
	}

out_0:
	return 0;
}

int cmd_updtr_prdcr_add(char *replybuf, struct attr_value_list *avl, struct attr_value_list *kwl)
{
	char *updtr_name, *prdcr_regex;
	int rc;

	updtr_name = av_value(avl, "name");
	if (!updtr_name) {
		sprintf(replybuf, "%dThe updater name must be specified\n", EINVAL);
		goto out_0;
	}
	prdcr_regex = av_value(avl, "regex");
	if (!prdcr_regex) {
		sprintf(replybuf, "%dA producer regular expression must be specified\n", EINVAL);
		goto out_0;
	}

	rc = ldmsd_updtr_prdcr_add(updtr_name, prdcr_regex,
				replybuf, sizeof(replybuf));
	if (rc) {
		if (rc == ENOENT) {
			sprintf(replybuf, "%dThe updater specified does not "
							"exist\n", ENOENT);
		} else if (rc == EBUSY) {
			sprintf(replybuf, "%dConfiguration changes cannot be "
					"made while the updater is running\n",
					EBUSY);
		} else if (rc == ENOMEM) {
			sprintf(replybuf, "%dMemory allocation failure.\n",
					ENOMEM);
		}
	} else {
		sprintf(replybuf, "0\n");
	}

out_0:
	return 0;
}
