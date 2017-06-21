/*
 * Copyright (c) 2016-2017 Sandia Corporation. All rights reserved.
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


#define PLUGNAME 0
#define store_csv_common_lib
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <errno.h>
#include "store_csv_common.h"
#define DSTRING_USE_SHORT
#include "ovis_util/dstring.h"


/** parse an option of the form name=bool where bool is
 * lead with tfyn01 or uppercase versions of same or value
 * string is "", so assume user meant name=true and absence of name
 * in options defaults to false.
 */
int parse_bool(struct csv_plugin_static *cps, struct attr_value_list *avl,
		const char *name, bool *bval)
{
	if (!cps || !avl || !name || !bval)
		return EINVAL;
	const char *val = av_value(avl, name);
	if (val) {
		switch (val[0]) {
		case '1':
		case 't':
		case 'T':
		case 'y':
		case 'Y':
		case '\0':
			*bval = true;
			break;
		case '0':
		case 'f':
		case 'F':
		case 'n':
		case 'N':
			*bval = false;
			break;
		default:
			cps->msglog(LDMSD_LERROR, "%s: bad %s=%s\n",
				cps->pname, name, val);
			return EINVAL;
		}
	}
	return 0;
}

static char *bad_replacement = "/malloc/failed";

int replace_string(char **strp, const char *val)
{
	if (!strp)
		return EINVAL;
	if (!val) {
		if (*strp != bad_replacement)
			free(*strp);
		*strp = NULL;
		return 0;
	}
	if (*strp != bad_replacement)
		free(*strp);
	char *new = strdup(val);
	if (new) {
		*strp = new;
		return 0;
	}
	*strp = bad_replacement;
	return ENOMEM;
}

/* get a handle to the onp we should use. return NULL only if no notify configured anywhere. */
static struct ovis_notification **get_onph(struct csv_store_handle_common *s_handle,
	struct csv_plugin_static *cps)
{
	if (!s_handle->notify && !cps->notify)
		return NULL;
	/* clean up case of redundant specification. */
	if (s_handle->notify && cps->notify &&
		strcmp(s_handle->notify, cps->notify) == 0) {
		s_handle->notify = NULL;
	}
	uint32_t wto = 6000;
	unsigned mqs = 1000;
	unsigned retry = 10;
	mode_t perm = 0700;
	/* init if unopened */
	if (s_handle->notify && s_handle->onp == NULL) {
		bool fifo = s_handle->notify_isfifo;
		s_handle->onp = ovis_notification_open(s_handle->notify,
				wto, mqs, retry,
				(ovis_notification_log_fn)cps->msglog,
				perm, fifo);
		if (s_handle->onp)
			cps->msglog(LDMSD_LDEBUG,"Created onp %s\n",
				s_handle->notify);
		else
			cps->msglog(LDMSD_LDEBUG,"Create fail for sh.onp %s\n",
				s_handle->notify);
	}
	if (cps->notify && !cps->onp) {
		bool fifo = cps->notify_isfifo;
		cps->onp = ovis_notification_open(cps->notify,
				wto, mqs, retry,
				(ovis_notification_log_fn)cps->msglog,
				perm, fifo);
		if (cps->onp)
			cps->msglog(LDMSD_LDEBUG,"Created onp %s\n",
				cps->notify);
		else
			cps->msglog(LDMSD_LDEBUG,"Create fail for cps.onp %s\n",
				cps->notify);
	}
	if (s_handle->onp)
		return &(s_handle->onp);
	else
		return &(cps->onp);
}

void notify_output(const char *event, const char *name, const char *ftype,
	struct csv_store_handle_common *s_handle, struct csv_plugin_static *cps,
	const char * container, const char *schema) {
	if (!cps)
		return;
	if (s_handle && !s_handle->notify && !cps->notify)
		return;
	if (!event || !name || !ftype || !s_handle ||
		!container || !schema) {
		cps->msglog(LDMSD_LDEBUG,"Invalid argument in notify_output"
				"(%s, %s, %s, %p, %p)\n",
				event ? event : "missing event",
				name ? name : "missing name",
				ftype ? ftype : "missing ftype",
				container ? container : "missing container",
				schema ? schema : "missing schema",
				s_handle, cps);
		return;
	}
	struct ovis_notification **onph = get_onph(s_handle, cps);
	if (! onph || ! *onph) {
		cps->msglog(LDMSD_LDEBUG,"onp not set in handle or cps\n");
		return;
	}

	int *hcp = (NULL != s_handle->onp) ? &(s_handle->hooks_closed) :
						&(cps->hooks_closed);
	char *msg;
	if (*hcp) {
		cps->msglog(LDMSD_LINFO, "Request by storecsv with output closed: %s\n",
			s_handle->notify);
		return;
	}
	dsinit(ds);
	dscat(ds,event);
	dscat(ds," ");
	dscat(ds,cps->pname);
	dscat(ds," ");
	dscat(ds,container);
	dscat(ds," ");
	dscat(ds,schema);
	dscat(ds," ");
	dscat(ds,ftype);
	dscat(ds," ");
	dscat(ds,name);
	msg = dsdone(ds);
	if (!msg) {
		cps->msglog(LDMSD_LERROR,
			"Out of memory in notify_output for %s\n",name);
		return;
	}
	int rc = ovis_notification_add(*onph, msg);
	switch (rc) {
	case 0:
		cps->msglog(LDMSD_LDEBUG,"Notification of %s\n", msg); //
		break;
	case EINVAL:
		cps->msglog(LDMSD_LERROR,"Notification error by %s for %s: %s\n",
			cps->pname, name, msg);
		break;
	case ESHUTDOWN:
		cps->msglog(LDMSD_LERROR,"Disconnected output detected. Closing.\n");
		ovis_notification_close(*onph);
		*hcp = 1;
		*onph = NULL;
		break;
	default:
		cps->msglog(LDMSD_LERROR,"Unexpected error type %d in notify_spool\n",rc);
	}
	free(msg);
}

void rename_output(const char *name,
	const char *ftype, struct csv_store_handle_common *s_handle,
	struct csv_plugin_static *cps) {
#define EBSIZE 512
	char errbuf[EBSIZE];
	if (!cps) {
		return;
	}
	if (!s_handle) {
		cps->msglog(LDMSD_LERROR,"rename_output: NULL store handle received.\n");
		return;
	}
	const char *container = s_handle->container;
	const char *schema = s_handle->schema;
	if (s_handle && !s_handle->rename_template && !cps->rename_template)
		return;
	char *rt = s_handle->rename_template ? s_handle->rename_template :
		cps->rename_template;
	if (!rt || !name || !ftype || !container || !schema) {
		cps->msglog(LDMSD_LDEBUG,"Invalid argument in rename_output"
				"(%s, %s, %s, %s, %s, %p, %p)\n",
				rt ? rt : "missing rename_template ",
				name ? name : "missing name",
				ftype ? ftype : "missing ftype",
				container ? container : "missing container",
				schema ? schema : "missing schema",
				s_handle, cps);
		return;
	}
	mode_t mode = (mode_t) (s_handle->rename_perm ? s_handle->rename_perm : cps->rename_perm);
	if (mode > 0) {
		errno = 0;
		int merr = chmod(name, mode);
		int rc = errno;
		if (merr) {
			strerror_r(rc, errbuf, EBSIZE);
			cps->msglog(LDMSD_LERROR,"rename_output: unable to chmod(%s,%o): %s.\n",
				name, s_handle->rename_perm, errbuf);
		}
	}
	
	gid_t newgid = (gid_t) (s_handle->rename_gid ? s_handle->rename_gid : cps->rename_gid);
	gid_t newuid = (uid_t) (s_handle->rename_uid ? s_handle->rename_uid : cps->rename_uid);
	if (newuid > 0)
	{
		if (newgid == 0)
			newgid = -1;
		errno = 0;
		int merr = chown(name, newuid, newgid);
		int rc = errno;
		if (merr) {
			strerror_r(rc, errbuf, EBSIZE);
			cps->msglog(LDMSD_LERROR,"rename_output: unable to chown(%s,%u, %u): %s.\n",
				name, newuid, newgid, errbuf);
		}
	}

	dsinit(ds);
	char *head = rt;
	char *end = strchr(head,'%');
	char *namedup = NULL;
	while (end != NULL) {
		dstrcat(&ds, head, (end - head));
		switch (end[1]) {
		case 'P':
			head = end + 2;
			dscat(ds, cps->pname);
			break;
		case 'S':
			head = end + 2;
			dscat(ds, s_handle->schema);
			break;
		case 'C':
			head = end + 2;
			dscat(ds, s_handle->container);
			break;
		case 'T':
			head = end + 2;
			dscat(ds, ftype);
			break;
		case 'B':
			head = end + 2;
			namedup = strdup(name);
			if (namedup) {
				char *bname = basename(namedup);
				dscat(ds, bname);
				free(namedup);
			} else {
				cps->msglog(LDMSD_LERROR,"rename_output: ENOMEM\n");
				dstr_free(&ds);
				return;
			}
			break;
		case 'D':
			head = end + 2;
			namedup = strdup(name);
			if (namedup) {
				char *dname = dirname(namedup);
				dscat(ds, dname);
				free(namedup);
			} else {
				cps->msglog(LDMSD_LERROR,"rename_output: ENOMEM\n");
				dstr_free(&ds);
				return;
			}
			break;
		case 's':
			head = end + 2;
			char *dot = strrchr(name,'.');
			if (!dot) {
				cps->msglog(LDMSD_LERROR,"rename_output: no timestamp\n");
				dstr_free(&ds);
				return;
			}
			dot = dot + 1;
			char *num = dot;
			while (isdigit(*num)) {
				num++;
			}	
			if (*num != '\0') {
				cps->msglog(LDMSD_LERROR,"rename_output: no timestamp at end\n");
				dstr_free(&ds);
				return;
			}
			dscat(ds,dot);
			break;
		default:
			/* unknown subst */
			dstrcat(&ds, "%", 1);
			head = end + 1;
		}
		end = strchr(head, '%');
	}
	dscat(ds, head);
	char *newname = dsdone(ds);
	dstr_free(&ds);
	if (!newname) {
		cps->msglog(LDMSD_LERROR,"rename_output: failed to create new filename for %s\n",
			name);
		return;
	}
	
	namedup = strdup(newname);
	char *ndname = dirname(namedup);
	int err = 0;
	if (mode) {
		/* derive directory mode from perm */
		mode |= S_IWUSR;
		if (mode & S_IROTH)
			mode |= S_IXOTH;
		if (mode & S_IRGRP)
			mode |= S_IXGRP;
		if (mode & S_IRUSR)
			mode |= S_IXUSR;
	} else {
		/* default 750 */
		mode = S_IXGRP | S_IXUSR | S_IRGRP | S_IRUSR |S_IWUSR;
	}
	cps->msglog(LDMSD_LDEBUG,"f_mkdir_p %o %s\n", (int)mode, ndname);
	err = f_mkdir_p(ndname, mode);
	free(namedup);
	if (err) {
		err = errno;
		switch (err) {
		case EEXIST:
			break;
		default:
			strerror_r(err, errbuf, EBSIZE);
			cps->msglog(LDMSD_LERROR,"rename_output: failed to create directory for %s: %s\n",
				newname, errbuf);
			return;
		}
	
	}

	cps->msglog(LDMSD_LDEBUG,"rename_output: rename(%s, %s)\n", name, newname);
	err = rename(name, newname);
	if (err) {
		int ec = errno;
		strerror_r(ec, errbuf, EBSIZE);
		cps->msglog(LDMSD_LERROR,"rename_output: failed rename(%s, %s): %s\n",
			name, newname, errbuf);
	}
	free(newname);
}

struct swap_data {
	size_t nstorekeys;
	size_t usedkeys;
	time_t appx;
	struct old_file *old;
	struct csv_plugin_static *cps;
};


/**
 * configurations for a container+schema that can override the vals in config_init
 * Locking and cfgstate are caller's job.
 */
int config_custom_common(struct attr_value_list *kwl, struct attr_value_list *avl, struct storek_common *sk, struct csv_plugin_static *cps)
{
	//defaults to init
	sk->notify_isfifo = cps->notify_isfifo;
	sk->rename_uid = cps->rename_uid;
	sk->rename_gid = cps->rename_gid;
	sk->rename_perm = cps->rename_perm;

	int rc = 0;
	char *notify =  av_value(avl, "notify");
	if (notify && strlen(notify) >= 2 ) {
		char *tmp1 = strdup(notify);
		if (!tmp1) {
			rc = ENOMEM;
		} else {
			sk->notify = tmp1;
			rc = parse_bool(cps, avl, "notify_isfifo",
				&(sk->notify_isfifo));
		}
	} else {
		if (notify) {
			cps->msglog(LDMSD_LERROR, "%s: notify "
				"must be specificed correctly. "
				"got instead %s\n", cps->pname,
				notify ) ;
			rc = EINVAL;
		} else {
			if (cps->notify) {
				sk->notify = strdup(cps->notify);
				if (!sk->notify) {
					rc = errno;
					cps->msglog(LDMSD_LERROR,
						"%s: config_custom_common out of memory\n",
						cps->pname);
				}
			}
		}
	}
	char *rename_template =  av_value(avl, "rename_template");
	if (rename_template && strlen(rename_template) >= 2 ) {
		char *tmp1 = strdup(rename_template);
		if (!tmp1) {
			rc = ENOMEM;
		} else {
			sk->rename_template = tmp1;
		}
	} else {
		if (rename_template) {
			cps->msglog(LDMSD_LERROR, "%s: rename_template "
				"must be specificed correctly. "
				"got instead %s\n", cps->pname,
				rename_template ) ;
			rc = EINVAL;
		} else {
			if (cps->rename_template) {
				sk->rename_template = strdup(cps->rename_template);
				if (!sk->rename_template) {
					rc = errno;
					cps->msglog(LDMSD_LERROR,
						"%s: config_custom_common out of memory\n",
						cps->pname);
				}
			}
		}
	}

	char * rename_uval = av_value(avl, "rename_uid");
	char * rename_gval = av_value(avl, "rename_gid");
	char * rename_pval = av_value(avl, "rename_perm");
	if (rename_uval) {
		int uid = atoi(rename_uval);
		if (uid < 1 || uid > USHRT_MAX) {
			rc = EINVAL;
			sk->rename_uid = 0;
			cps->msglog(LDMSD_LERROR,
				"%s: config_custom_common ignoring bad rename_uid=%s\n",
				cps->pname, rename_uval);
		} else {
			sk->rename_uid = uid;
		}
	}

	if (rename_gval) {
		int gid = atoi(rename_gval);
		if (gid < 1 || gid > USHRT_MAX) {
			rc = EINVAL;
			sk->rename_gid = 0;
			cps->msglog(LDMSD_LERROR,
				"%s: config_custom_common ignoring bad rename_gid=%s\n",
				cps->pname, rename_gval);
		} else {
			sk->rename_gid = gid;
		}
	}

	if (rename_pval) {
		int perm = strtol(rename_pval, NULL, 8);
		if (perm < 1 || perm > 4777) {
			rc = EINVAL;
			sk->rename_perm = 0;
			cps->msglog(LDMSD_LERROR,
				"%s: config_custom_common ignoring bad rename_perm=%s\n",
				cps->pname, rename_pval);
		} else {
			sk->rename_perm = perm;
		}
	}

	return rc;
}


void clear_storek_common(struct storek_common *s)
{
	if (!s)
		return;
	free(s->notify);
	s->notify = NULL;
	free(s->rename_template);
	s->rename_template = NULL;
}

/**
 * configurations for the whole store. these will be defaults if not overridden.
 * some implementation details are for backwards compatibility
 */
int config_init_common(struct attr_value_list *kwl, struct attr_value_list *avl, void *arg, struct csv_plugin_static *cps)
{
	if (!cps || !avl)
		return EINVAL;

	int rc = 0;
	char *notify =  av_value(avl, "notify");
	if (notify && strlen(notify) >= 2 ) {
		char *tmp1 = strdup(notify);
		if (!tmp1) {
			rc = ENOMEM;
		} else {
			cps->notify = tmp1;
		}
	} else {
		if (notify) {
			cps->msglog(LDMSD_LERROR, "%s: notify "
				"must be specificed correctly. "
				"got instead %s\n", cps->pname,
				notify ) ;
			rc = EINVAL;
		}
	}
	if (!rc)
		rc = parse_bool(cps, avl, "notify_isfifo",
			&(cps->notify_isfifo));

	char *rename_template =  av_value(avl, "rename_template");
	if (rename_template && strlen(rename_template) >= 2 ) {
		char *tmp1 = strdup(rename_template);
		if (!tmp1) {
			rc = ENOMEM;
		} else {
			cps->rename_template = tmp1;
		}
	} else {
		if (rename_template) {
			cps->msglog(LDMSD_LERROR, "%s: rename_template "
				"must be specificed correctly. "
				"got instead %s\n", cps->pname,
				rename_template ) ;
			rc = EINVAL;
		}
	}

	char * rename_uval = av_value(avl, "rename_uid");
	char * rename_gval = av_value(avl, "rename_gid");
	char * rename_pval = av_value(avl, "rename_perm");
	if (!rename_uval) {
		cps->rename_uid = 0;
	} else {
		int uid = atoi(rename_uval);
		if (uid < 1 || uid > USHRT_MAX) {
			rc = EINVAL;
			cps->rename_uid = 0;
			cps->msglog(LDMSD_LERROR,
				"%s: config_init_common ignoring bad rename_uid=%s\n",
				cps->pname, rename_uval);
		} else {
			cps->rename_uid = uid;
		}
	}

	if (!rename_gval) {
		cps->rename_gid = 0;
	} else {
		int gid = atoi(rename_gval);
		if (gid < 1 || gid > USHRT_MAX) {
			rc = EINVAL;
			cps->rename_gid = 0;
			cps->msglog(LDMSD_LERROR,
				"%s: config_init_common ignoring bad rename_gid=%s\n",
				cps->pname, rename_gval);
		} else {
			cps->rename_gid = gid;
		}
	}

	if (!rename_pval) {
		cps->rename_perm = 0;
	} else {
		int perm = strtol(rename_pval, NULL, 8);
		if (perm < 1 || perm > 4777) {
			rc = EINVAL;
			cps->rename_perm = 0;
			cps->msglog(LDMSD_LERROR,
				"%s: config_init_common ignoring bad rename_perm=%s\n",
				cps->pname, rename_pval);
		} else {
			cps->rename_perm = perm;
		}
	}
	return rc;
}

void close_store_common(struct csv_store_handle_common *s_handle, struct csv_plugin_static *cps) {
	if (!s_handle || !cps) {
		cps->msglog(LDMSD_LERROR,
			"%s: close_store_common with null argument\n",
			cps->pname);
		return;
	}

	notify_output(NOTE_CLOSE, s_handle->filename, NOTE_DAT, s_handle, cps,
		s_handle->container, s_handle->schema);
	notify_output(NOTE_CLOSE, s_handle->headerfilename, NOTE_HDR, s_handle,
		cps, s_handle->container, s_handle->schema);
	rename_output(s_handle->filename, NOTE_DAT, s_handle, cps);
	rename_output(s_handle->headerfilename, NOTE_HDR, s_handle, cps);
	replace_string(&(s_handle->filename), NULL);
	replace_string(&(s_handle->headerfilename),  NULL);
	/* free(s_handle->notify); skip. handle notify is always copy of pg or sk notify or null */
	s_handle->notify = NULL;
	s_handle->rename_template = NULL;
}

void print_csv_plugin_common(struct csv_plugin_static *cps)
{
	cps->msglog(LDMSD_LALL, "notify: %s\n", cps->notify);
	cps->msglog(LDMSD_LALL, "notify is fifo: %s\n", cps->notify_isfifo ?
		"true" : "false");
	cps->msglog(LDMSD_LALL, "rename_template: %s\n", cps->rename_template);
	cps->msglog(LDMSD_LALL, "rename_uid: %d\n", cps->rename_uid);
	cps->msglog(LDMSD_LALL, "rename_gid: %d\n", cps->rename_gid);
	cps->msglog(LDMSD_LALL, "rename_perm: %d\n", cps->rename_perm);
	cps->msglog(LDMSD_LALL, "onp: %p\n", cps->onp);
}

void print_csv_store_handle_common(struct csv_store_handle_common *h, struct csv_plugin_static *p)
{
	if (!p)
		return;
	if (!h) {
		p->msglog(LDMSD_LALL, "csv store handle dump: NULL handle.\n");
		return;
	}
	p->msglog(LDMSD_LALL, "csv store handle dump:\n");
	p->msglog(LDMSD_LALL, "filename: %s\n", h->filename);
	p->msglog(LDMSD_LALL, "headerfilename: %s\n", h->headerfilename);
	p->msglog(LDMSD_LALL, "notify:%s\n", h->notify);
	p->msglog(LDMSD_LALL, "notify_isfifo:%s\n", h->notify_isfifo ?
			                "true" : "false");
	p->msglog(LDMSD_LALL, "rename_template:%s\n", h->rename_template);
	p->msglog(LDMSD_LALL, "rename_uid: %d\n", h->rename_uid);
	p->msglog(LDMSD_LALL, "rename_gid: %d\n", h->rename_gid);
	p->msglog(LDMSD_LALL, "rename_perm: %d\n", h->rename_perm);
	p->msglog(LDMSD_LALL, "onp: %p\n", h->onp);
}
