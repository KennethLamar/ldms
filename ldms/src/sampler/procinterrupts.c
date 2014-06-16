/* -*- c-basic-offset: 8 -*-
 * Copyright (c) 2010 Open Grid Computing, Inc. All rights reserved.
 * Copyright (c) 2010 Sandia Corporation. All rights reserved.
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
/**
 * \file procinterrupts.c
 * \brief /proc/interrupts data provider
 */
#include <inttypes.h>
#include <unistd.h>
#include <sys/errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include "ldms.h"
#include "ldmsd.h"

#define PROC_FILE "/proc/interrupts"
static char *procfile = PROC_FILE;

ldms_set_t set;
FILE *mf;
ldms_metric_t *metric_table;
ldmsd_msg_log_f msglog;
int nprocs;
uint64_t comp_id;
static uint64_t counter;

static ldms_set_t get_set()
{
	return set;
}


static int getNProcs(char buf[]){
	int nproc = 0;
	char* pch;
	pch = strtok(buf, " ");
	while (pch != NULL){
		if (pch[0] == '\n'){
			break;
		}
		if (pch[0] != ' '){
			nproc++;
		}
		pch = strtok(NULL," ");
	}

	return nproc;
}


static int create_metric_set(const char *path)
{
	size_t meta_sz, tot_meta_sz;
	size_t data_sz, tot_data_sz;
	int rc, i, metric_count;
	char *s;
	char lbuf[256];
	char metric_name[128];

	mf = fopen(procfile, "r");
	if (!mf) {
		msglog("Could not open the interrupts file '%s'...exiting\n",
				procfile);
		return ENOENT;
	}

	char beg_name[128];

	/*
	 * Process the file once first to determine the metric set size.
	 */

	tot_meta_sz = 0;
	tot_data_sz = 0;

	metric_count = 0;
	fseek(mf, 0, SEEK_SET);

	//first line is the cpu list
	s = fgets(lbuf, sizeof(lbuf), mf);
	nprocs = getNProcs(lbuf);
	if (nprocs <= 0){
		msglog("Bad number of CPU. Exiting\n");
		return EINVAL;
	}

	//the rest are data lines
	while(s) {
		s = fgets(lbuf, sizeof(lbuf), mf);
		if (!s)
			break;
		int currcol = 0;
		char* pch = strtok (lbuf," ");
		while (pch != NULL && currcol <= nprocs){
			if (pch[0] == '\n'){
				break;
			}
			if (currcol == 0){
				/* Strip the colon from metric name if present */
				i = strlen(pch);
				if (i && pch[i-1] == ':')
					pch[i-1] = '\0';
				strcpy(beg_name, pch);
			} else {
				/* the metric name will be irq.<name>#CPU_NUMBER
				 */
				snprintf(metric_name, 128, "irq.%s#%d",
						beg_name, (currcol-1));
				rc = ldms_get_metric_size(metric_name, LDMS_V_U64, &meta_sz, &data_sz);
				tot_meta_sz += meta_sz;
				tot_data_sz += data_sz;
				metric_count++;
			}
			currcol++;
			pch = strtok(NULL," ");
		} // while (strtok)
	} //while(s)

	/* Create a metric set of the required size */
	rc = ldms_create_set(path, tot_meta_sz, tot_data_sz, &set);
	if (rc)
		return rc;

	metric_table = calloc(metric_count, sizeof(ldms_metric_t));
	if (!metric_table)
		goto err;

	/*
	 * Process the file again to define all the metrics.
	 */
	int metric_no = 0;
	fseek(mf, 0, SEEK_SET);
	//first line is the cpu list
	s = fgets(lbuf, sizeof(lbuf), mf);
	while(s){
		s = fgets(lbuf, sizeof(lbuf), mf);
		if (!s)
			break;
		int currcol = 0;
		char* pch = strtok (lbuf," ");
		while (pch != NULL && currcol <= nprocs){
			if (pch[0] == '\n'){
				break;
			}
			if (currcol == 0){
				/* Strip the colon from metric name if present
				 * */
				i = strlen(pch);
				if (i && pch[i-1] == ':')
					pch[i-1] = '\0';
				strcpy(beg_name, pch);
			} else {
				snprintf(metric_name, 128, "irq.%s#%d",
						beg_name, (currcol-1));
				metric_table[metric_no] =
					ldms_add_metric(set, metric_name,
							LDMS_V_U64);
				if (!metric_table[metric_no]){
					rc = ENOMEM;
					goto err;
				}
				ldms_set_user_data(metric_table[metric_no],
						comp_id);
				metric_no++;
			}
			currcol++;
			pch = strtok(NULL," ");
		} // while (strtok)
	} //while(s)
	return 0;

err:
	ldms_destroy_set(set);
	return rc;
}

/**
 * \brief Configuration
 *
 * - config procinterrupts component_id <value>
 */
static int config(struct attr_value_list *kwl, struct attr_value_list *avl)
{

	char *value;

	value = av_value(avl, "component_id");
	if (value)
		comp_id = strtol(value, NULL, 0);

	value = av_value(avl, "set");
	if (value)
		create_metric_set(value);

	return 0;
}

static int sample(void)
{
	int rc;
	int metric_no;
	char *s;
	char lbuf[256];
	union ldms_value v;

	if (!set){
		msglog("procinterrupts: plugin not initialized\n");
		return EINVAL;
	}
	ldms_begin_transaction(set);

	metric_no = 0;
	fseek(mf, 0, SEEK_SET);
	//first line is the cpu list
	s = fgets(lbuf, sizeof(lbuf), mf);

	while(s){
		s = fgets(lbuf, sizeof(lbuf), mf);
		if (!s)
			break;

		int currcol = 0;
		char* pch = strtok(lbuf, " ");
		while (pch != NULL && currcol <= nprocs){
			if (pch[0] == '\n'){
				break;
			}
			if (pch[0] != ' '){
				if (currcol != 0){
					char* endptr;
					unsigned long long int l1;
					l1 = strtoull(pch,&endptr,10);
					if (endptr != pch){
						v.v_u64 = l1;
						ldms_set_metric(
							metric_table[metric_no],
							&v);
						metric_no++;
					} else {
						msglog("bad val <%s>\n",pch);
						rc = EINVAL;
						goto out;
					}
				}
				currcol++;
			}
			pch = strtok(NULL," ");
		} //strtok
	} while (s);
	rc = 0;
out:
	ldms_end_transaction(set);
	return rc;
}


static void term(void)
{
	if (set)
		ldms_destroy_set(set);
	set = NULL;
}


static const char *usage(void)
{
	return  "config name=procinterrupts component_id=<comp_id> set=<setname>\n"
		"    comp_id     The component id value.\n"
		"    setname     The set name.\n";
}



static struct ldmsd_sampler procinterrupts_plugin = {
	.base = {
		.name = "procinterrupts",
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
	return &procinterrupts_plugin.base;
}
