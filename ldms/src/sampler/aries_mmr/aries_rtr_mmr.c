/* -*- c-basic-offset: 8 -*-
 * Copyright (c) 2015-2016 Open Grid Computing, Inc. All rights reserved.
 * Copyright (c) 2015-2016 Sandia Corporation. All rights reserved.
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

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <ctype.h>
#include <limits.h>
#include <inttypes.h>
#include <unistd.h>
#include <string.h>
#include <sys/errno.h>
#include <sys/types.h>
#include <pthread.h>
#include <linux/limits.h>
#include "gpcd_pub.h"
#include "gpcd_lib.h"
#include "ldms.h"
#include "ldmsd.h"

/**
 * \file aries_rtr_mmr.c
 * \brief aries network metric provider (reads gpcd mmr)
 *
 * parses 1 config file. All names go in as is.
 */

#define AR_MAX_LEN 256

//#define NICFILTER
#define ROUTERFILTER

//list types
enum {REQ_T, RC_T, PTILE_T, NIC_T, OTHER_T, END_T};
//matching string types
enum {REQ_LMT, RSP_LMT, RTR_LMT, PTILE_LMT, NIC_LMT, END_LMT};

struct listmatch_t{
	char* header;
	int len;
};

static struct listmatch_t LMT[END_LMT] = {
	{"AR_NL_PRF_REQ_", 14},
	{"AR_NL_PRF_RSP_", 14},
	{"AR_RTR_", 7},
	{"AR_NL_PRF_PTILE_", 16},
	{"AR_NIC_", 7}
};


struct met{ //for the XXX_T
	int metric_index;
	LIST_ENTRY(met) entry;
};

struct mstruct{ //for the XXX_T
	int num_metrics;
	gpcd_context_t* ctx;
};

static struct mstruct mvals[END_T];
static char** rawlist;
static int numraw;

//want to be able to create/declare these in the mstruct.
//these allow the metric_ids to be in any order
static LIST_HEAD(req_list, met) req_list;
static LIST_HEAD(rc_list, met) rc_list;
static LIST_HEAD(nic_list, met) nic_list;
static LIST_HEAD(ptile_list, met) ptile_list;
static LIST_HEAD(other_list, met) other_list;

static gpcd_mmr_list_t *listp = NULL;

static ldms_set_t set = NULL;
static ldmsd_msg_log_f msglog;
static char *producer_name;
static ldms_schema_t schema;
static char *default_schema_name = "aries_rtr_mmr";
static uint64_t compid;
static char* rtrid = NULL;
static uint64_t jobid;


static int filterConfig(char* tmpname){
//return val of 1 means keep this
#ifdef NICFILTER
	if (strncmp(LMT[NIC_LMT].header, tmpname, LMT[NIC_LMT].len) == 0)
		return 1;
	else
		return 0;
#endif

#ifdef ROUTERFILTER
	if (strncmp(LMT[NIC_LMT].header, tmpname, LMT[NIC_LMT].len) == 0)
		return 0;
	else
		return 1;
#endif

	return 1;
}



static int parseConfig(char* fname){
	FILE *mf;
	char *s;
	char name[AR_MAX_LEN];
	char lbuf[AR_MAX_LEN];
	int countA = 0;
	int rc;

	rawlist = NULL;
	numraw = 0;

	mf = fopen(fname, "r");
	if (!mf){
		msglog(LDMSD_LERROR, " aries_rtr_mmr: Cannot open file <%s>\n", fname);
		return EINVAL;
	}

	fseek(mf, 0, SEEK_SET);
	//parse once to get the number of metrics
	do {
		s = fgets(lbuf, sizeof(lbuf), mf);
		if (!s)
			break;
		rc = sscanf(lbuf," %s", name);
		if ((rc != 1) || (strlen(name) == 0) || (name[0] == '#')){
			msglog(LDMSD_LDEBUG, "aries_rtr_mmr: skipping input <%s>\n", lbuf);
			continue;
		}
		rc = filterConfig(name);
		if (!rc) {
			msglog(LDMSD_LDEBUG, "aries_rtr_mmr: filtering input <%s>\n", lbuf);
			continue;
		}
		countA++;
	} while(s);

	if (countA > 0){
		//parse again to populate the metrics
		fseek(mf, 0, SEEK_SET);
		rawlist = calloc(countA, sizeof(*rawlist));
		if (!rawlist)
			return ENOMEM;
		do {
			s = fgets(lbuf, sizeof(lbuf), mf);
			if (!s)
				break;
			rc = sscanf(lbuf," %s", name);
			if ((rc != 1) || (strlen(name) == 0) || (name[0] == '#')){
				continue;
			}
			rc = filterConfig(name);
			if (!rc)
				continue;
			msglog(LDMSD_LDEBUG, "aries_rtr_mmr: config read <%s>\n", lbuf);
			rawlist[numraw] = strdup(name);
			if (numraw == countA)
				break;
			numraw++;
		} while(s);
	}

	rc = 0;

	fclose(mf);
	return rc;
}


/**
 * Build linked list of tile performance counters we wish to get values for.
 * No aggregation.
 */
int addMetricToContext(gpcd_context_t* lctx, char* met)
{
	gpcd_mmr_desc_t *desc;
	int status;
	int i;

	if (lctx == NULL){
		msglog(LDMSD_LERROR, "aries_rtr_mmr: NULL context\n");
		return -1;
	}

	desc = (gpcd_mmr_desc_t *)
		gpcd_lookup_mmr_byname(met);
	if (!desc) {
		msglog(LDMSD_LINFO, "aries_rtr_mmr: Could not lookup <%s>\n", met);
		return -1;
	}

	status = gpcd_context_add_mmr(lctx, desc);
	if (status != 0) {
		msglog(LDMSD_LERROR, "aries_rtr_mmr: Could not add mmr for <%s>\n", met);
                gpcd_remove_context(lctx); //some other option?
		return -1;
	}

	return 0;
}


static int addMetric(char* tmpname){

	int i;
	int mid;
	int rc;

	if (strncmp(LMT[NIC_LMT].header, tmpname, LMT[NIC_LMT].len) == 0){
		rc = addMetricToContext(mvals[NIC_T].ctx, tmpname);
		if (!rc) {
			mid = ldms_schema_metric_add(schema, tmpname, LDMS_V_U64);
			if (mid < 0)
				return ENOMEM;

			struct met* e = calloc(1, sizeof(*e));
			e->metric_index = mid;
			LIST_INSERT_HEAD(&nic_list, e, entry);
			mvals[NIC_T].num_metrics++;
		}
		return rc;
	}

	if ((strncmp(LMT[REQ_LMT].header, tmpname, LMT[REQ_LMT].len) == 0) ||
	    (strncmp(LMT[RSP_LMT].header, tmpname, LMT[RSP_LMT].len) == 0)){
		rc = addMetricToContext(mvals[REQ_T].ctx, tmpname);
		if (!rc) {
			mid = ldms_schema_metric_add(schema, tmpname, LDMS_V_U64);
			if (mid < 0)
				return ENOMEM;

			struct met* e = calloc(1, sizeof(*e));
			e->metric_index = mid;
			LIST_INSERT_HEAD(&req_list, e, entry);
			mvals[REQ_T].num_metrics++;
		}
		return rc;
	}

	if (strncmp(LMT[PTILE_LMT].header, tmpname, LMT[PTILE_LMT].len) == 0){
		rc = addMetricToContext(mvals[PTILE_T].ctx, tmpname);
		if (!rc) {
			mid = ldms_schema_metric_add(schema, tmpname, LDMS_V_U64);
			if (mid < 0)
				return ENOMEM;

			struct met* e = calloc(1, sizeof(*e));
			e->metric_index = mid;
			LIST_INSERT_HEAD(&ptile_list, e, entry);
			mvals[PTILE_T].num_metrics++;
		}
		return rc;
	}

	if (strncmp(LMT[RTR_LMT].header, tmpname, LMT[RTR_LMT].len) == 0){
		rc = addMetricToContext(mvals[RC_T].ctx, tmpname);
		if (!rc) {
			mid = ldms_schema_metric_add(schema, tmpname, LDMS_V_U64);
			if (mid < 0)
				return ENOMEM;

			struct met* e = calloc(1, sizeof(*e));
			e->metric_index = mid;
			LIST_INSERT_HEAD(&rc_list, e, entry);
			mvals[RC_T].num_metrics++;
		}
		return rc;
	}

	//put it in the other list.....
	rc = addMetricToContext(mvals[OTHER_T].ctx, tmpname);
	if (!rc) {
		mid = ldms_schema_metric_add(schema, tmpname, LDMS_V_U64);
		if (mid < 0)
			return ENOMEM;

		struct met* e = calloc(1, sizeof(*e));
		e->metric_index = mid;
		LIST_INSERT_HEAD(&other_list, e, entry);
		mvals[OTHER_T].num_metrics++;
	}
	return rc;
}

static int create_metric_set(const char *instance_name, char* schema_name)
{
	union ldms_value v;
	int rc, i;

	schema = ldms_schema_new(schema_name);
	if (!schema) {
		rc = ENOMEM;
		goto err;
	}

	rc = ldms_schema_meta_add(schema, "component_id", LDMS_V_U64);
	if (rc < 0) {
		rc = ENOMEM;
		goto err;
	}

	rc = ldms_schema_metric_add(schema, "job_id", LDMS_V_U64);
	if (rc < 0) {
		rc = ENOMEM;
		goto err;
	}

	if (rtrid)
                rc = ldms_schema_meta_array_add(schema, "aries_rtr_id", LDMS_V_CHAR_ARRAY, strlen(rtrid)+1);
        else
                rc = ldms_schema_meta_array_add(schema, "aries_rtr_id", LDMS_V_CHAR_ARRAY, 1);

        if (rc < 0) {
		rc = ENOMEM;
		goto err;
	}

	//add them in the order of the file.
	//they will come off the context and the index list in the reverse order

	for (i = 0; i < numraw; i++){
		rc = addMetric(rawlist[i]);
		if (rc == ENOMEM)
			goto err;
		else if (rc)
			msglog(LDMSD_LINFO, "aries_rtr_mmr: cannot add metric <%s>. Skipping\n",
			       rawlist[i]);
		free(rawlist[i]);
	}
	if (rawlist)
		free(rawlist);
	rawlist = NULL;
	numraw = 0;

	set = ldms_set_new(instance_name, schema);
	if (!set) {
		rc = errno;
		goto err;
	}

	//add specialized metrics
	v.v_u64 = compid;
	ldms_metric_set(set, 0, &v);
	v.v_u64 = 0;
	ldms_metric_set(set, 1, &v);
	if (rtrid)
                ldms_metric_array_set_str(set, 2, rtrid);
        else
                ldms_metric_array_set_str(set, 2, "");
	return 0;

err:
	if (schema)
		ldms_schema_delete(schema);
	schema = NULL;

	return rc;
}


static int config(struct ldmsd_plugin *self, struct attr_value_list *kwl, struct attr_value_list *avl)
{
	char *value;
	char *sname;
	char *rawf;
	void * arg = NULL;
	int i;
	int rc;


	if (set) {
		msglog(LDMSD_LERROR, "aries_rtr_mmr: Set already created.\n");
		return EINVAL;
	}

	for (i = 0; i < END_T; i++){
		mvals[i].num_metrics = 0;
		mvals[i].ctx = gpcd_create_context();
		if (!mvals[i].ctx){
			printf("Could not create context\n");
			return EINVAL;
		}
	}

	producer_name = av_value(avl, "producer");
	if (!producer_name) {
		msglog(LDMSD_LERROR, "aries_rtr_mmr: missing producer\n");
		return ENOENT;
	}

	value = av_value(avl, "component_id");
	if (value)
		compid = (uint64_t)(atoi(value));
	else
		compid = 0;

	value = av_value(avl, "aries_rtr_id");
        if (value)
                rtrid = strdup(value);
        else
		rtrid = NULL;

	value = av_value(avl, "instance");
	if (!value) {
		msglog(LDMSD_LERROR, "aries_rtr_mmr: missing instance.\n");
		return ENOENT;
	}

	sname = av_value(avl, "schema");
	if (!sname)
		sname = default_schema_name;
	if (strlen(sname) == 0){
		msglog(LDMSD_LERROR, "aries_rtr_mmr: schema name invalid.\n");
		return EINVAL;
	}

	rawf = av_value(avl, "file");
	if (rawf){
		rc = parseConfig(rawf);
		if (rc){
			msglog(LDMSD_LERROR, "aries_rtr_mmr: error parsing <%s>\n", rawf);
			return EINVAL;
		}
	} else {
		msglog(LDMSD_LERROR, "aries_rtr_mmr: must specify input file\n");
		return EINVAL;
	}

	rc = create_metric_set(value, sname);
	if (rc) {
		msglog(LDMSD_LERROR, "aries_rtr_mmr: failed to create a metric set.\n");
		return rc;
	}
	ldms_set_producer_name_set(set, producer_name);
	return 0;
}


static int sample(struct ldmsd_sampler *self){

	union ldms_value v;
	int i;
	int rc;

	if (!set) {
		msglog(LDMSD_LERROR, "aries_rtr_mmr: plugin not initialized\n");
		return EINVAL;
	}

	for (i = 0; i < END_T; i++){
		if (mvals[i].num_metrics){
			rc = gpcd_context_read_mmr_vals(mvals[i].ctx);
			if (rc){
				msglog(LDMSD_LERROR, "aries_rtr_mmr: Cannot read raw mmr vals\n");
				return EINVAL;
			}
		}
	}

	ldms_transaction_begin(set);

	for (i = 0; i < END_T; i++){
		struct met *np;

		if (mvals[i].num_metrics == 0)
			continue;
		listp = mvals[i].ctx->list;
		switch(i) {
		case REQ_T:
			np = req_list.lh_first;
			break;
		case RC_T:
			np = rc_list.lh_first;
			break;
		case NIC_T:
			np = nic_list.lh_first;
			break;
		case PTILE_T:
			np = ptile_list.lh_first;
			break;
		}

		if (np == NULL){
			msglog(LDMSD_LERROR, "aries_rtr_mmr: Name/MetricID list is null\n");
			rc = EINVAL;
			goto out;
		}
		if (!listp){
			msglog(LDMSD_LERROR, "aries_rtr_mmr: Context list is null\n");
			rc = EINVAL;
			goto out;
		}

		while (listp != NULL){
			v.v_u64 = listp->value;
			ldms_metric_set(set, np->metric_index, &v);

			if (listp->next != NULL)
				listp=listp->next;
			else
				break;
			np = np->entry.le_next;
			if (np == NULL){
				msglog(LDMSD_LERROR, "aries_rtr_mmr: No metric id\n");
				       goto out;
			}
		}
	}

	rc = 0;
out:

	ldms_transaction_end(set);
	return 0;

}



static ldms_set_t get_set(struct ldmsd_sampler *self)
{
	return set;
}

static void term(struct ldmsd_plugin *self)
{

	int i;

	if (rtrid)
		free(rtrid);
	rtrid = NULL;

	for (i = 0; i < END_T; i++){
		struct met *np;
		switch(i){
		case RC_T:
			np = rc_list.lh_first;
			break;
		case NIC_T:
			np = nic_list.lh_first;
			break;
		case PTILE_T:
			np = ptile_list.lh_first;
			break;
		default:
			np = req_list.lh_first;
		}
		while (np != NULL) {
			struct met *tp = np->entry.le_next;
			LIST_REMOVE(np, entry);
			free(np);
			np = tp;
		}

		if (mvals[i].ctx)
			gpcd_remove_context(mvals[i].ctx);
		mvals[i].ctx = NULL;
		mvals[i].num_metrics = 0;
	}

	if (schema)
		ldms_schema_delete(schema);
	schema = NULL;
	if (set)
		ldms_set_delete(set);
	set = NULL;
}

static const char *usage(struct ldmsd_plugin *self)
{
	return  "config name=aries_rtr_mmr producer=<prod_name> instance=<inst_name> file=<file> [component_id=<compid> aries_rtr_id=<rtrid> schema=<sname>]\n"
		"    <prod_name>    The producer name\n"
		"    <inst_name>    The instance name\n"
		"    <file>         File with full names of metrics\n";
		"    <compid>       Optional unique number identifier. Defaults to zero.\n"
                "    <rtrid>        Optional unique rtr string identifier. Defaults to 0 length string.\n"
		"    <sname>        Optional schema name. Defaults to 'aries_rtr_mmr'\n";
}

static struct ldmsd_sampler aries_rtr_mmr_plugin = {
	.base = {
		.name = "aries_rtr_mmr",
		.term = term,
		.config = config,
		.usage = usage,
	},
	.get_set = get_set,
	.sample = sample,
};

struct ldmsd_plugin *get_plugin(ldmsd_msg_log_f pf)
{
	msglog = pf;
	set = NULL;
	return &aries_rtr_mmr_plugin.base;
}
