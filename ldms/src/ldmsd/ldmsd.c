/* -*- c-basic-offset: 8 -*-
 * Copyright (c) 2010-2018 National Technology & Engineering Solutions
 * of Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
 * NTESS, the U.S. Government retains certain rights in this software.
 * Copyright (c) 2010-2018 Open Grid Computing, Inc. All rights reserved.
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
#include <unistd.h>
#include <inttypes.h>
#include <stdarg.h>
#include <getopt.h>
#include <stdlib.h>
#include <sys/errno.h>
#include <stdio.h>
#include <syslog.h>
#include <string.h>
#include <sys/queue.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/fcntl.h>
#include <sys/mman.h>
#include <pthread.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/un.h>
#include <ctype.h>
#include <netdb.h>
#include <dlfcn.h>
#include <assert.h>
#include <libgen.h>
#include <time.h>
#include <coll/rbt.h>
#include <coll/str_map.h>
#include <ev/ev.h>
#include "ldms.h"
#include "ldmsd.h"
#include "ldms_xprt.h"
#include "ldmsd_request.h"
#include "config.h"
#include "kldms_req.h"

#include "ovis_event/ovis_event.h"

#ifdef DEBUG
#include <mcheck.h>
#endif /* DEBUG */

#define LDMSD_AUTH_ENV "LDMS_AUTH_FILE"

#define LDMSD_SETFILE "/proc/sys/kldms/set_list"
#define LDMSD_LOGFILE "/var/log/ldmsd.log"
#define LDMSD_PIDFILE_FMT "/var/run/%s.pid"

#define FMT "B:H:i:l:S:s:x:I:T:M:t:P:m:FkN:r:R:p:v:Vz:Z:q:c:u:a:A:n:"

#define LDMSD_MEM_SIZE_ENV "LDMSD_MEM_SZ"
#define LDMSD_MEM_SIZE_STR "512kB"
#define LDMSD_MEM_SIZE_DEFAULT 512L * 1024L

char myname[512]; /* name to identify ldmsd */
		  /* NOTE: fqdn limit: 255 characters */
		  /* DEFAULT: myhostname:port */
char myhostname[80];
char ldmstype[20];
int foreground;
pthread_t event_thread = (pthread_t)-1;
char *test_set_name;
int test_set_count=1;
int notify=0;
char *logfile;
char *pidfile;
char *bannerfile;
int banner = 1;
pthread_mutex_t log_lock = PTHREAD_MUTEX_INITIALIZER;
size_t max_mem_size;
char *max_mem_sz_str;

mode_t inband_cfg_mask = LDMSD_PERM_FAILOVER_ALLOWED;
	/* LDMSD_PERM_FAILOVER_INTERNAL will be added in `failover_start`
	 * command.
	 *
	 * If failover is not in use, 0777 will later be added after
	 * process_config_file.
	 */
int ldmsd_use_failover = 0;

ldms_t ldms;
FILE *log_fp;

int do_kernel = 0;
char *setfile = NULL;

static int set_cmp(void *a, const void *b)
{
	return strcmp(a, b);
}

static struct rbt set_tree = {
		.root = 0,
		.comparator = set_cmp,
};
static pthread_mutex_t set_tree_lock = PTHREAD_MUTEX_INITIALIZER;

int find_least_busy_thread();

int passive = 0;
int log_level_thr = LDMSD_LERROR;  /* log level threshold */
int quiet = 0; /* Is verbosity quiet? 0 for no and 1 for yes */

const char *auth_name = "none";
struct attr_value_list *auth_opt = NULL;
const int AUTH_OPT_MAX = 128;

const char* ldmsd_loglevel_names[] = {
	LOGLEVELS(LDMSD_STR_WRAP)
	NULL
};

void ldmsd_sec_ctxt_get(ldmsd_sec_ctxt_t sctxt)
{
	if (!ldms)
		return;
	ldms_local_cred_get(ldms, &sctxt->crd);
}

void ldmsd_version_get(struct ldmsd_version *v)
{
	v->major = LDMSD_VERSION_MAJOR;
	v->minor = LDMSD_VERSION_MINOR;
	v->patch = LDMSD_VERSION_PATCH;
	v->flags = LDMSD_VERSION_FLAGS;
}

int ldmsd_loglevel_set(char *verbose_level)
{
	int level = -1;
	if (0 == strcmp(verbose_level, "QUIET")) {
		quiet = 1;
		level = LDMSD_LLASTLEVEL;
	} else {
		level = ldmsd_str_to_loglevel(verbose_level);
		quiet = 0;
	}
	if (level < 0)
		return level;
	log_level_thr = level;
	return 0;
}

enum ldmsd_loglevel ldmsd_loglevel_get()
{
	return log_level_thr;
}

int ldmsd_loglevel_to_syslog(enum ldmsd_loglevel level)
{
	switch (level) {
#define MAPLOG(X,Y) case LDMSD_L##X: return LOG_##Y
	MAPLOG(DEBUG,DEBUG);
	MAPLOG(INFO,INFO);
	MAPLOG(WARNING,WARNING);
	MAPLOG(ERROR,ERR);
	MAPLOG(CRITICAL,CRIT);
	MAPLOG(ALL,ALERT);
	default:
		return LOG_ERR;
	}
#undef MAPLOG
}

/* Impossible file pointer as syslog-use sentinel */
#define LDMSD_LOG_SYSLOG ((FILE*)0x7)

void __ldmsd_log(enum ldmsd_loglevel level, const char *fmt, va_list ap)
{
	if ((level != LDMSD_LALL) &&
			(quiet || ((0 <= level) && (level < log_level_thr))))
		return;
	if (log_fp == LDMSD_LOG_SYSLOG) {
		vsyslog(ldmsd_loglevel_to_syslog(level),fmt,ap);
		return;
	}
	time_t t;
	struct tm *tm;
	char dtsz[200];

	pthread_mutex_lock(&log_lock);
	if (!log_fp) {
		pthread_mutex_unlock(&log_lock);
		return;
	}
	t = time(NULL);
	tm = localtime(&t);
	if (strftime(dtsz, sizeof(dtsz), "%a %b %d %H:%M:%S %Y", tm))
		fprintf(log_fp, "%s: ", dtsz);

	if (level < LDMSD_LALL) {
		fprintf(log_fp, "%-10s: ", ldmsd_loglevel_names[level]);
	}

	vfprintf(log_fp, fmt, ap);
	fflush(log_fp);
	pthread_mutex_unlock(&log_lock);
}

void ldmsd_log(enum ldmsd_loglevel level, const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	__ldmsd_log(level, fmt, ap);
	va_end(ap);
}

/* All messages from the ldms library are of e level.*/
#define LDMSD_LOG_AT(e,fsuf) \
void ldmsd_l##fsuf(const char *fmt, ...) \
{ \
	va_list ap; \
	va_start(ap, fmt); \
	__ldmsd_log(e, fmt, ap); \
	va_end(ap); \
}

LDMSD_LOG_AT(LDMSD_LDEBUG,debug);
LDMSD_LOG_AT(LDMSD_LINFO,info);
LDMSD_LOG_AT(LDMSD_LWARNING,warning);
LDMSD_LOG_AT(LDMSD_LERROR,error);
LDMSD_LOG_AT(LDMSD_LCRITICAL,critical);
LDMSD_LOG_AT(LDMSD_LALL,all);

static char msg_buf[4096];
void ldmsd_msg_logger(enum ldmsd_loglevel level, const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(msg_buf, sizeof(msg_buf), fmt, ap);
	ldmsd_log(level, "%s", msg_buf);
}

enum ldmsd_loglevel ldmsd_str_to_loglevel(const char *level_s)
{
	int i;
	for (i = 0; i < LDMSD_LLASTLEVEL; i++)
		if (0 == strcasecmp(level_s, ldmsd_loglevel_names[i]))
			return i;
	if (strcasecmp(level_s,"QUIET") == 0) {
		return LDMSD_LALL;
				}
	if (strcasecmp(level_s,"ALWAYS") == 0) {
		return LDMSD_LALL;
	}
	if (strcasecmp(level_s,"CRIT") == 0) {
		return LDMSD_LCRITICAL;
	}
	return LDMSD_LNONE;
}

const char *ldmsd_loglevel_to_str(enum ldmsd_loglevel level)
{
	if ((level >= LDMSD_LDEBUG) && (level < LDMSD_LLASTLEVEL))
		return ldmsd_loglevel_names[level];
	return "LDMSD_LNONE";
}

const char *ldmsd_myhostname_get()
{
	return myhostname;
}

const char *ldmsd_myname_get()
{
	return myname;
}

mode_t ldmsd_inband_cfg_mask_get()
{
	return inband_cfg_mask;
}

void ldmsd_inband_cfg_mask_set(mode_t mask)
{
	inband_cfg_mask = mask;
}

void ldmsd_inband_cfg_mask_add(mode_t mask)
{
	inband_cfg_mask |= mask;
}

void ldmsd_inband_cfg_mask_rm(mode_t mask)
{
	inband_cfg_mask &= ~mask;
}

#ifdef LDMSD_UPDATE_TIME
double ldmsd_timeval_diff(struct timeval *start, struct timeval *end)
{
	return (end->tv_sec-start->tv_sec)*1000000.0 + (end->tv_usec-start->tv_usec);
}
#endif /* LDMSD_UPDATE_TIME */

extern void ldmsd_strgp_close();

void cleanup(int x, const char *reason)
{
	int llevel = LDMSD_LINFO;
	if (x)
		llevel = LDMSD_LCRITICAL;
	ldmsd_mm_status(LDMSD_LDEBUG,"mmap use at exit");
	ldmsd_strgp_close();
	ldmsd_log(llevel, "LDMSD_ LDMS Daemon exiting...status %d, %s\n", x,
		       (reason && x) ? reason : "");
	if (ldms) {
		/* No need to close the xprt. It has never been connected. */
		ldms_xprt_put(ldms);
		ldms = NULL;
	}

	if (!foreground && pidfile) {
		unlink(pidfile);
		free(pidfile);
		pidfile = NULL;
		if (bannerfile) {
			if ( banner < 2) {
				unlink(bannerfile);
			}
			free(bannerfile);
			bannerfile = NULL;
		}
	}
	if (pidfile) {
		free(pidfile);
		pidfile = NULL;
	}
	ldmsd_log(llevel, "LDMSD_ cleanup end.\n");
	if (logfile) {
		free(logfile);
		logfile = NULL;
	}

	av_free(auth_opt);
	exit(x);
}

/** return a file pointer or a special syslog pointer */
FILE *ldmsd_open_log(const char *progname)
{
	FILE *f;
	if (strcasecmp(logfile,"syslog")==0) {
		ldmsd_log(LDMSD_LDEBUG, "Switching to syslog.\n");
		f = LDMSD_LOG_SYSLOG;
		openlog(progname, LOG_NDELAY|LOG_PID, LOG_DAEMON);
		return f;
	}

	f = fopen_perm(logfile, "a", LDMSD_DEFAULT_FILE_PERM);
	if (!f) {
		ldmsd_log(LDMSD_LERROR, "Could not open the log file named '%s'\n",
							logfile);
		cleanup(9, "log open failed");
	} else {
		int fd = fileno(f);
		if (dup2(fd, 1) < 0) {
			ldmsd_log(LDMSD_LERROR, "Cannot redirect log to %s\n",
							logfile);
			cleanup(10, "error redirecting stdout");
		}
		if (dup2(fd, 2) < 0) {
			ldmsd_log(LDMSD_LERROR, "Cannot redirect log to %s\n",
							logfile);
			cleanup(11, "error redirecting stderr");
		}
		stdout = f;
		stderr = f;
	}
	return f;
}

int ldmsd_logrotate() {
	int rc;
	if (!logfile) {
		ldmsd_log(LDMSD_LERROR, "Received a logrotate command but "
			"the log messages are printed to the standard out.\n");
		return EINVAL;
	}
	if (log_fp == LDMSD_LOG_SYSLOG) {
		/* nothing to do */
		return 0;
	}
	struct timeval tv;
	char ofile_name[PATH_MAX];
	gettimeofday(&tv, NULL);
	sprintf(ofile_name, "%s-%ld", logfile, tv.tv_sec);

	pthread_mutex_lock(&log_lock);
	if (!log_fp) {
		pthread_mutex_unlock(&log_lock);
		return EINVAL;
	}
	fflush(log_fp);
	fclose(log_fp);
	rename(logfile, ofile_name);
	log_fp = fopen_perm(logfile, "a", LDMSD_DEFAULT_FILE_PERM);
	if (!log_fp) {
		printf("%-10s: Failed to rotate the log file. Cannot open a new "
			"log file\n", "ERROR");
		fflush(stdout);
		rc = errno;
		goto err;
	}
	int fd = fileno(log_fp);
	if (dup2(fd, 1) < 0) {
		rc = errno;
		goto err;
	}
	if (dup2(fd, 2) < 0) {
		rc = errno;
		goto err;
	}
	stdout = stderr = log_fp;
	pthread_mutex_unlock(&log_lock);
	return 0;
err:
	pthread_mutex_unlock(&log_lock);
	return rc;
}

void cleanup_sa(int signal, siginfo_t *info, void *arg)
{
	ldmsd_log(LDMSD_LINFO, "signo : %d\n", info->si_signo);
	ldmsd_log(LDMSD_LINFO, "si_pid: %d\n", info->si_pid);
	cleanup(0, "signal to exit caught");
}


void usage_hint(char *argv[],char *hint)
{
	printf("%s: [%s]\n", argv[0], FMT);
	printf("  General Options\n");
	printf("    -F	     Foreground mode, don't daemonize the program [false].\n");
	printf("    -B mode  Daemon mode banner file with pidfile [1].\n"
	       "   		modes:0-no banner file, 1-banner auto-deleted, 2-banner left.\n");
	printf("    -u	name List named plugin if available, and where possible\n");
	printf("       	its usage, then exit. Name all, sampler, and store limit output.\n");
	printf("    -u	name List named plugin if available, and where possible their usage, then exit.\n");
	printf("    -m memory size Maximum size of pre-allocated memory for metric sets.\n"
	       "		   The given size must be less than 1 petabytes.\n"
	       "		   The default value is %s\n"
	       "		   For example, 20M or 20mb are 20 megabytes.\n"
	       "		   - The environment variable %s could be set instead of\n"
	       "		   giving the -m option. If both are given, the -m option\n"
	       "		   takes precedence over the environment variable.\n",
	       LDMSD_MEM_SIZE_STR, LDMSD_MEM_SIZE_ENV);
	printf("    -n NAME        The name of the daemon. By default, it is \"IHOSTNAME:PORT\".");
	printf("    -r pid_file    The path to the pid file for daemon mode.\n"
	       "		   [" LDMSD_PIDFILE_FMT "]\n",basename(argv[0]));
	printf("  Log Verbosity Options\n");
	printf("    -l log_file    The path to the log file for status messages.\n"
	       "		   [" LDMSD_LOGFILE "]\n");
	printf("    -v level       The available verbosity levels, in order of decreasing verbosity,\n"
	       "		   are DEBUG, INFO, ERROR, CRITICAL and QUIET.\n"
	       "		   The default level is ERROR.\n");
	printf("  Communication Options\n");
	printf("    -x xprt:port   Specifies the transport type to listen on. May be specified\n"
	       "		   more than once for multiple transports. The transport string\n"
	       "		   is one of 'rdma', 'sock' or 'ugni'. A transport specific port number\n"
	       "		   is optionally specified following a ':', e.g. rdma:50000.\n");
	printf("    -a AUTH        Transport authentication plugin (default: 'none')\n");
	printf("    -A KEY=VALUE   Authentication plugin options (repeatable)\n");
	printf("  Kernel Metric Options\n");
	printf("    -k	     Publish kernel metrics.\n");
	printf("    -s setfile     Text file containing kernel metric sets to publish.\n"
	       "		   [" LDMSD_SETFILE "]\n");
	printf("  Thread Options\n");
	printf("    -P thr_count   Count of event threads to start.\n");
	printf("    -f count       The number of flush threads.\n");
	printf("    -D num	 The dirty threshold.\n");
	printf("  Test Options\n");
	printf("    -H host_name   The host/producer name for metric sets.\n");
	printf("    -i	     Test metric set sample interval.\n");
	printf("    -t count       Create set_count instances of set_name.\n");
	printf("    -T set_name    Test set prefix.\n");
	printf("    -N	     Notify registered monitors of the test metric sets\n");
	printf("  Configuration Options\n");
	printf("    -c path	The path to configuration file (optional, default: <none>).\n");
	printf("    -V	     Print LDMS version and exit\n.");
	printf("   Deprecated Options\n");
	printf("    -S     	   DEPRECATED.\n");
	printf("    -p     	   DEPRECATED.\n");
	if (hint) {
		printf("\nHINT: %s\n",hint);
	}
	cleanup(1, "usage provided");
}

void usage(char *argv[]) {
	usage_hint(argv,NULL);
}

#define EVTH_MAX 1024
int ev_thread_count = 1;
ovis_scheduler_t *ovis_scheduler;
pthread_t *ev_thread;		/* sampler threads */
int *ev_count;			/* number of hosts/samplers assigned to each thread */

int find_least_busy_thread()
{
	int i;
	int idx = 0;
	int count = ev_count[0];
	for (i = 1; i < ev_thread_count; i++) {
		if (ev_count[i] < count) {
			idx = i;
			count = ev_count[i];
		}
	}
	return idx;
}

ovis_scheduler_t get_ovis_scheduler(int idx)
{
	__sync_add_and_fetch(&ev_count[idx], 1);
	return ovis_scheduler[idx];
}

void release_ovis_scheduler(int idx)
{
	__sync_sub_and_fetch(&ev_count[idx], 1);
}

pthread_t get_thread(int idx)
{
	return ev_thread[idx];
}

void kpublish(int map_fd, int set_no, int set_size, char *set_name)
{
	ldms_set_t map_set;
	int rc, id = set_no << 13;
	void *meta_addr, *data_addr;
	struct ldms_set_hdr *sh;

	ldmsd_linfo("Mapping set %d:%d:%s\n", set_no, set_size, set_name);
	meta_addr = mmap((void *)0, set_size,
			 PROT_READ | PROT_WRITE, MAP_SHARED,
			 map_fd, id);
	if (meta_addr == MAP_FAILED) {
		ldmsd_lerror("Error %d mapping %d bytes for kernel "
			     "metric set\n", errno, set_size);
		return;
	}
	data_addr = (struct ldms_data_hdr *)((unsigned char*)meta_addr + sh->meta_sz);
	rc = ldms_mmap_set(meta_addr, data_addr, &map_set);
	if (rc) {
		munmap(meta_addr, set_size);
		ldmsd_lerror("Error %d mmapping the set '%s'\n", rc, set_name);
		return;
	}
	sh = meta_addr;
	sprintf(sh->producer_name, "%s", myhostname);
}

pthread_t k_thread;
void *k_proc(void *arg)
{
	int rc, map_fd;
	int i, j;
	int set_no;
	int set_size;
	char set_name[128];
	FILE *fp;
	union kldms_req k_req;

	fp = fopen(setfile, "r");
	if (!fp) {
		ldmsd_lerror("The specified kernel metric set file '%s' "
			     "could not be opened.\n", setfile);
		cleanup(1, "Could not open kldms set file");
	}

	map_fd = open("/dev/kldms0", O_RDWR);
	if (map_fd < 0) {
		ldmsd_lerror("Error %d opening the KLDMS device file "
			     "'/dev/kldms0'\n", map_fd);
		cleanup(1, "Could not open the kernel device /dev/kldms0");
	}

	while (3 == fscanf(fp, "%d %d %128s", &set_no, &set_size, set_name)) {
		kpublish(map_fd, set_no, set_size, set_name);
	}

	/* Read from map_fd and process events as they are delivered by the kernel */
	while (0 < (rc = read(map_fd, &k_req, sizeof(k_req)))) {
		switch (k_req.hdr.req_id) {
		case KLDMS_REQ_HELLO:
			ldmsd_ldebug("KLDMS_REQ_HELLO: %s\n", k_req.hello.msg);
			break;
		case KLDMS_REQ_PUBLISH_SET:
			ldmsd_ldebug("KLDMS_REQ_PUBLISH_SET: set_id %d data_len %zu\n",
				     k_req.publish.set_id, k_req.publish.data_len);
			kpublish(map_fd, k_req.publish.set_id, k_req.publish.data_len, "");
			break;
		case KLDMS_REQ_UNPUBLISH_SET:
			ldmsd_ldebug("KLDMS_REQ_UNPUBLISH_SET: set_id %d data_len %zu\n",
				     k_req.unpublish.set_id, k_req.publish.data_len);
			break;
		case KLDMS_REQ_UPDATE_SET:
			ldmsd_ldebug("KLDMS_REQ_UPDATE_SET: set_id %d\n",
				     k_req.update.set_id);
			break;
		default:
			ldmsd_lerror("Unrecognized kernel request %d\n",
				     k_req.hdr.req_id);
			break;
		}
	}
}
/*
 * This function opens the device file specified by 'devname' and
 * mmaps the metric set 'set_no'.
 */
int publish_kernel(const char *setfile)
{
	int res = pthread_create(&k_thread, NULL, k_proc, (void *)setfile);
	if (!res)
		pthread_setname_np(k_thread, "ldmsd:kernel");
	return 0;
}

void ldmsd_set_tree_lock()
{
	pthread_mutex_lock(&set_tree_lock);
}

void ldmsd_set_tree_unlock()
{
	pthread_mutex_unlock(&set_tree_lock);
}

/* Caller must hold the set tree lock. */
ldmsd_plugin_set_list_t ldmsd_plugin_set_list_first()
{
	struct rbn *rbn;
	ldmsd_plugin_set_list_t list;

	rbn = rbt_min(&set_tree);
	if (!rbn)
		return NULL;
	return container_of(rbn, struct ldmsd_plugin_set_list, rbn);
}

ldmsd_plugin_set_list_t ldmsd_plugin_set_list_next(ldmsd_plugin_set_list_t list)
{
	struct rbn *rbn;
	rbn = rbn_succ(&list->rbn);
	if (!rbn)
		return NULL;
	return container_of(rbn, struct ldmsd_plugin_set_list, rbn);
}

ldmsd_plugin_set_list_t ldmsd_plugin_set_list_find(const char *plugin_name)
{
	struct rbn *rbn;
	rbn = rbt_find(&set_tree, plugin_name);
	if (!rbn) {
		return NULL;
	}
	return container_of(rbn, struct ldmsd_plugin_set_list, rbn);
}

/* Caller must hold the set_tree lock */
ldmsd_plugin_set_t ldmsd_plugin_set_first(const char *plugin_name)
{
	struct rbn *rbn;
	ldmsd_plugin_set_t set;
	ldmsd_plugin_set_list_t list;
	rbn = rbt_find(&set_tree, plugin_name);
	if (!rbn)
		return NULL;
	list = container_of(rbn, struct ldmsd_plugin_set_list, rbn);
	return LIST_FIRST(&list->list);
}

/* Caller must hold the set_tree lock */
ldmsd_plugin_set_t ldmsd_plugin_set_next(ldmsd_plugin_set_t set)
{
	return LIST_NEXT(set, entry);
}

int ldmsd_set_register(ldms_set_t set, const char *plugin_name)
{
	if (!set || ! plugin_name)
		return EINVAL;
	struct rbn *rbn;
	ldmsd_plugin_set_t s;
	ldmsd_plugin_set_list_t list;
	struct ldmsd_plugin_cfg *pi;
	int rc;

	s = malloc(sizeof(*s));
	if (!s)
		return ENOMEM;
	s->plugin_name = strdup(plugin_name);
	if (!s->plugin_name) {
		rc = ENOMEM;
		goto free_set;
	}
	s->inst_name = strdup(ldms_set_instance_name_get(set));
	if (!s->inst_name) {
		rc = ENOMEM;
		goto free_plugin;
	}
	s->set = ldms_set_by_name(ldms_set_instance_name_get(set));
	if (!s->set) {
		rc = ENOMEM;
		goto free_inst_name;
	}
	ldmsd_set_tree_lock();
	rbn = rbt_find(&set_tree, s->plugin_name);
	if (!rbn) {
		list = malloc(sizeof(*list));
		if (!list) {
			ldmsd_set_tree_unlock();
			ldms_set_put(s->set);
			rc = ENOMEM;
			goto free_inst_name;
		}
		rbn_init(&list->rbn, s->plugin_name);
		LIST_INIT(&list->list);
		rbt_ins(&set_tree, &list->rbn);
	} else {
		list = container_of(rbn, struct ldmsd_plugin_set_list, rbn);
	}
	LIST_INSERT_HEAD(&list->list, s, entry);
	ldmsd_set_tree_unlock();

	return 0;
free_inst_name:
	free(s->inst_name);
free_plugin:
	free(s->plugin_name);
free_set:
	free(s);
	return rc;
}

void ldmsd_set_deregister(const char *inst_name, const char *plugin_name)
{
	ldmsd_plugin_set_t set;
	ldmsd_plugin_set_list_t list;
	struct rbn *rbn;
	ldmsd_set_tree_lock();
	rbn = rbt_find(&set_tree, plugin_name);
	if (!rbn)
		goto out;
	list = container_of(rbn, struct ldmsd_plugin_set_list, rbn);
	LIST_FOREACH(set, &list->list, entry) {
		if (0 == strcmp(set->inst_name, inst_name)) {
			LIST_REMOVE(set, entry);
			free(set->inst_name);
			free(set->plugin_name);
			ldms_set_put(set->set);
			free(set);
		}
	}
	if (LIST_EMPTY(&list->list)) {
		rbt_del(&set_tree, &list->rbn);
		free(list);
	}
out:
	ldmsd_set_tree_unlock();
}

static void resched_task(ldmsd_task_t task)
{
	struct timeval new_tv;
	long adj_interval, epoch_us;

	if (task->flags & LDMSD_TASK_F_IMMEDIATE) {
		adj_interval = random() % 1000000;
		task->flags &= ~LDMSD_TASK_F_IMMEDIATE;
	} else if (task->flags & LDMSD_TASK_F_SYNCHRONOUS) {
		gettimeofday(&new_tv, NULL);
		/* The task is already counted when the task is started */
		epoch_us = (1000000 * (long)new_tv.tv_sec) + (long)new_tv.tv_usec;
		adj_interval = task->sched_us -
			(epoch_us % task->sched_us) + task->offset_us;
		if (adj_interval <= 0)
			adj_interval += task->sched_us; /* Guaranteed to be positive */
	} else {
		adj_interval = task->sched_us;
	}
	task->oev.param.timeout.tv_sec = adj_interval / 1000000;
	task->oev.param.timeout.tv_usec = adj_interval % 1000000;
}

static int start_task(ldmsd_task_t task)
{
	int rc = ovis_scheduler_event_add(task->os, &task->oev);
	if (!rc) {
		errno = rc;
		return LDMSD_TASK_STATE_STARTED;
	}
	return LDMSD_TASK_STATE_STOPPED;
}

static void task_cb_fn(ovis_event_t ev)
{
	ldmsd_task_t task = ev->param.ctxt;
	enum ldmsd_task_state next_state;

	pthread_mutex_lock(&task->lock);
	if (task->os) {
		ovis_scheduler_event_del(task->os, ev);
		resched_task(task);
		next_state = start_task(task);
	}
	task->state = LDMSD_TASK_STATE_RUNNING;
	pthread_mutex_unlock(&task->lock);

	task->fn(task, task->fn_arg);

	pthread_mutex_lock(&task->lock);
	if (task->flags & LDMSD_TASK_F_STOP) {
		task->flags &= ~LDMSD_TASK_F_STOP;
		if (task->state != LDMSD_TASK_STATE_STOPPED)
			task->state = LDMSD_TASK_STATE_STOPPED;
	} else
		task->state = next_state;
 out:
	if (task->state == LDMSD_TASK_STATE_STOPPED) {
		if (task->os)
			ovis_scheduler_event_del(task->os, &task->oev);
		task->os = NULL;
		release_ovis_scheduler(task->thread_id);
		pthread_cond_signal(&task->join_cv);
	}
	pthread_mutex_unlock(&task->lock);
}

void ldmsd_task_init(ldmsd_task_t task)
{
	memset(task, 0, sizeof *task);
	task->state = LDMSD_TASK_STATE_STOPPED;
	pthread_mutex_init(&task->lock, NULL);
	pthread_cond_init(&task->join_cv, NULL);
}

void ldmsd_task_stop(ldmsd_task_t task)
{

	pthread_mutex_lock(&task->lock);
	if (task->state == LDMSD_TASK_STATE_STOPPED)
		goto out;
	if (task->state != LDMSD_TASK_STATE_RUNNING) {
		ovis_scheduler_event_del(task->os, &task->oev);
		task->os = NULL;
		release_ovis_scheduler(task->thread_id);
		task->state = LDMSD_TASK_STATE_STOPPED;
		pthread_cond_signal(&task->join_cv);
	} else {
		task->flags |= LDMSD_TASK_F_STOP;
	}
out:
	pthread_mutex_unlock(&task->lock);
}

int ldmsd_task_resched(ldmsd_task_t task, int flags, long sched_us, long offset_us)
{
	int rc = 0;
	pthread_mutex_lock(&task->lock);
	if ((task->state != LDMSD_TASK_STATE_RUNNING)
			&& (task->state != LDMSD_TASK_STATE_STARTED))
		goto out;
	ovis_scheduler_event_del(task->os, &task->oev);
	task->flags = flags;
	task->sched_us = sched_us;
	task->offset_us = offset_us;
	resched_task(task);
	rc = ovis_scheduler_event_add(task->os, &task->oev);
out:
	pthread_mutex_unlock(&task->lock);
	return rc;
}

int ldmsd_task_start(ldmsd_task_t task,
		     ldmsd_task_fn_t task_fn, void *task_arg,
		     int flags, long sched_us, long offset_us)
{
	int rc = 0;
	pthread_mutex_lock(&task->lock);
	if (task->state != LDMSD_TASK_STATE_STOPPED) {
		rc = EBUSY;
		goto out;
	}
	task->thread_id = find_least_busy_thread();
	task->os = get_ovis_scheduler(task->thread_id);
	task->fn = task_fn;
	task->fn_arg = task_arg;
	task->flags = flags;
	task->sched_us = sched_us;
	task->offset_us = offset_us;
	OVIS_EVENT_INIT(&task->oev);
	task->oev.param.type = OVIS_EVENT_TIMEOUT;
	task->oev.param.cb_fn = task_cb_fn;
	task->oev.param.ctxt = task;
	resched_task(task);
	task->state = start_task(task);
	if (task->state != LDMSD_TASK_STATE_STARTED)
		rc = errno;
 out:
	pthread_mutex_unlock(&task->lock);
	return rc;
}

void ldmsd_task_join(ldmsd_task_t task)
{
	pthread_mutex_lock(&task->lock);
	while (task->state != LDMSD_TASK_STATE_STOPPED)
		pthread_cond_wait(&task->join_cv, &task->lock);
	pthread_mutex_unlock(&task->lock);
}

void *event_proc(void *v)
{
	ovis_scheduler_t os = v;
	ovis_scheduler_loop(os, 0);
	ldmsd_log(LDMSD_LINFO, "Exiting the sampler thread.\n");
	return NULL;
}

void ev_log_cb(int sev, const char *msg)
{
	const char *sev_s[] = {
		"EV_DEBUG",
		"EV_MSG",
		"EV_WARN",
		"EV_ERR"
	};
	ldmsd_log(LDMSD_LERROR, "%s: %s\n", sev_s[sev], msg);
}

char *ldmsd_get_max_mem_sz_str()
{
	return max_mem_sz_str;
}

enum ldms_opttype {
	LO_PATH,
	LO_UINT,
	LO_INT,
	LO_NAME,
};

int check_arg(char *c, char *optarg, enum ldms_opttype t)
{
	if (!optarg)
		return 1;
	switch (t) {
	case LO_PATH:
		av_check_expansion((printf_t)printf, c, optarg);
		if ( optarg[0] == '-'  ) {
			printf("option -%s expected path name, not %s\n",
				c,optarg);
			return 1;
		}
		break;
	case LO_UINT:
		if (av_check_expansion((printf_t)printf, c, optarg))
			return 1;
		if ( optarg[0] == '-' || !isdigit(optarg[0]) ) {
			printf("option -%s expected number, not %s\n",c,optarg);
			return 1;
		}
		break;
	case LO_INT:
		if (av_check_expansion((printf_t)printf, c, optarg))
			return 1;
		if ( optarg[0] == '-' && !isdigit(optarg[1]) ) {
			printf("option -%s expected number, not %s\n",c,optarg);
			return 1;
		}
		break;
	case LO_NAME:
		if (av_check_expansion((printf_t)printf, c, optarg))
			return 1;
		if ( !isalnum(optarg[0]) ) {
			printf("option -%s expected name, not %s\n",c,optarg);
			return 1;
		}
		break;
	}
	return 0;
}

int default_actor(ev_worker_t src, ev_worker_t dst, ev_status_t status, ev_t ev)
{
	ldmsd_log(LDMSD_LINFO, "Unhandled Event: type=%s, id=%d\n",
		  ev_type_name(ev_type(ev)), ev_type_id(ev_type(ev)));
	ldmsd_log(LDMSD_LINFO, "    status  : %s\n", status ? "FLUSH" : "OK" );
	ldmsd_log(LDMSD_LINFO, "    src     : %s\n", ev_worker_name(src));
	ldmsd_log(LDMSD_LINFO, "    dst     : %s\n", ev_worker_name(dst));
	return 0;
}

void ldmsd_ev_init(void)
{
	smplr_sample_type = ev_type_new("smplr:sample", sizeof(struct sample_data));
	prdcr_connect_type = ev_type_new("prdcr:connect", sizeof(struct connect_data));
	prdcr_set_update_type = ev_type_new("prdcr_set:update", sizeof(struct update_data));
	prdcr_set_store_type = ev_type_new("prdcr_set:store", sizeof(struct store_data));
	prdcr_set_state_type = ev_type_new("prdcr_set:state", sizeof(struct state_data));
	updtr_start_type = ev_type_new("updtr:start", sizeof(struct start_data));
	prdcr_start_type = ev_type_new("prdcr:start", sizeof(struct start_data));
	strgp_start_type = ev_type_new("strgp:start", sizeof(struct start_data));
	smplr_start_type = ev_type_new("smplr:start", sizeof(struct start_data));
	updtr_stop_type = ev_type_new("updtr:stop", sizeof(struct stop_data));
	prdcr_stop_type = ev_type_new("prdcr:stop", sizeof(struct stop_data));
	strgp_stop_type = ev_type_new("strgp:stop", sizeof(struct stop_data));
	smplr_stop_type = ev_type_new("smplr:stop", sizeof(struct stop_data));

	producer = ev_worker_new("producer", default_actor);
	updater = ev_worker_new("updater", default_actor);
	sampler = ev_worker_new("sampler", default_actor);
	storage = ev_worker_new("storage", default_actor);

	ev_dispatch(sampler, smplr_sample_type, sample_actor);
	ev_dispatch(updater, prdcr_set_update_type, prdcr_set_update_actor);
	ev_dispatch(updater, prdcr_set_state_type, prdcr_set_state_actor);
	ev_dispatch(updater, prdcr_start_type, prdcr_start_actor);
	ev_dispatch(updater, prdcr_stop_type, prdcr_stop_actor);
	ev_dispatch(producer, prdcr_connect_type, prdcr_connect_actor);
	ev_dispatch(producer, updtr_start_type, updtr_start_actor);
	ev_dispatch(producer, updtr_stop_type, updtr_stop_actor);
}

int main(int argc, char *argv[])
{
#ifdef DEBUG
	mtrace();
#endif /* DEBUG */

	struct ldms_version ldms_version;
	struct ldmsd_version ldmsd_version;
	ldms_version_get(&ldms_version);
	ldmsd_version_get(&ldmsd_version);
	char *sockname = NULL;
	char *lval = NULL;
	char *rval = NULL;
	char *plug_name = NULL;
	const char *port;
	int list_plugins = 0;
	int ret;
	int sample_interval = 2000000;
	int op;
	ldms_set_t test_set;
	log_fp = stdout;
	struct sigaction action;
	sigset_t sigset;
	sigemptyset(&sigset);
	sigaddset(&sigset, SIGUSR1);

	memset(&action, 0, sizeof(action));
	action.sa_sigaction = cleanup_sa;
	action.sa_flags = SA_SIGINFO;
	action.sa_mask = sigset;

	sigaction(SIGHUP, &action, NULL);
	sigaction(SIGINT, &action, NULL);
	sigaction(SIGTERM, &action, NULL);

	sigaddset(&sigset, SIGHUP);
	sigaddset(&sigset, SIGINT);
	sigaddset(&sigset, SIGTERM);
	sigaddset(&sigset, SIGABRT);

	auth_opt = av_new(AUTH_OPT_MAX);
	if (!auth_opt) {
		printf("Not enough memory!!!\n");
		exit(1);
	}

	opterr = 0;

	while ((op = getopt(argc, argv, FMT)) != -1) {
		switch (op) {
		case 'B':
			if (check_arg("B", optarg, LO_UINT))
				return 1;
			banner = atoi(optarg);
			break;
		case 'H':
			if (check_arg("H", optarg, LO_NAME))
				return 1;
			strcpy(myhostname, optarg);
			break;
		case 'i':
			if (check_arg("i", optarg, LO_UINT))
				return 1;
			sample_interval = atoi(optarg);
			break;
		case 'k':
			do_kernel = 1;
			break;
		case 'r':
			if (check_arg("r", optarg, LO_PATH))
				return 1;
			pidfile = strdup(optarg);
			break;
		case 'l':
			if (check_arg("l", optarg, LO_PATH))
				return 1;
			logfile = strdup(optarg);
			break;
		case 's':
			if (check_arg("s", optarg, LO_PATH))
				return 1;
			setfile = strdup(optarg);
			break;
		case 'v':
			if (check_arg("v", optarg, LO_NAME))
				return 1;
			if (0 == strcmp(optarg, "QUIET")) {
				quiet = 1;
				log_level_thr = LDMSD_LLASTLEVEL;
			} else {
				log_level_thr = ldmsd_str_to_loglevel(optarg);
			}
			if (log_level_thr < 0) {
				usage(argv);
				printf("Invalid verbosity levels '%s'. "
					"See -v option.\n", optarg);
			}
			break;
		case 'F':
			foreground = 1;
			break;
		case 'T':
			if (check_arg("T", optarg, LO_NAME))
				return 1;
			test_set_name = strdup(optarg);
			break;
		case 't':
			if (check_arg("t", optarg, LO_UINT))
				return 1;
			test_set_count = atoi(optarg);
			break;
		case 'P':
			if (check_arg("P", optarg, LO_UINT))
				return 1;
			ev_thread_count = atoi(optarg);
			if (ev_thread_count < 1 )
				ev_thread_count = 1;
			if (ev_thread_count > EVTH_MAX)
				ev_thread_count = EVTH_MAX;
			break;
		case 'N':
			notify = 1;
			break;
		case 'm':
			max_mem_sz_str = strdup(optarg);
			break;
		case 'q':
			usage_hint(argv,"-q becomes -v in LDMS v3. Update your scripts.\n"
				"This message will disappear in a future release.");
		case 'z':
			usage_hint(argv,"-z not available in LDMS v3.\n"
				"This message will disappear in a future release.");
			break;
		case 'Z':
			usage_hint(argv,"-Z not needed in LDMS v3. Remove it.\n"
				"This message will disappear in a future release.");
			break;
		case 'V':
			printf("LDMSD Version: %s\n", PACKAGE_VERSION);
			printf("LDMS Protocol Version: %hhu.%hhu.%hhu.%hhu\n",
							ldms_version.major,
							ldms_version.minor,
							ldms_version.patch,
							ldms_version.flags);
			printf("LDMSD Plugin Interface Version: %hhu.%hhu.%hhu.%hhu\n",
							ldmsd_version.major,
							ldmsd_version.minor,
							ldmsd_version.patch,
							ldmsd_version.flags);
			printf("git-SHA: %s\n", OVIS_GIT_LONG);
			exit(0);
			break;
		case 'u':
			if (check_arg("u", optarg, LO_NAME))
				return 1;
			list_plugins = 1;
			plug_name = strdup(optarg);
			break;
		case 'x':
			/* Listening port processing is handled below */
			port = strchr(optarg, ':');
			if (!port) {
				printf("Bad xprt format, expecting XPRT:PORT, "
				       "but got: %s\n", optarg);
				exit(1);
			}
			port++;
			break;
		case 'c':
			/* Handle below */
			break;
		case 'p':
			usage_hint(argv,"-p is deprecated.");
			break;
		case 'a':
			/* auth name */
			auth_name = optarg;
			break;
		case 'A':
			/* (multiple) auth options */
			lval = strtok(optarg, "=");
			if (!lval) {
				printf("ERROR: Expecting -A name=value\n");
				exit(1);
			}
			rval = strtok(NULL, "");
			if (!rval) {
				printf("ERROR: Expecting -A name=value\n");
				exit(1);
			}
			if (auth_opt->count == auth_opt->size) {
				printf("ERROR: Too many auth options\n");
				exit(1);
			}
			auth_opt->list[auth_opt->count].name = lval;
			auth_opt->list[auth_opt->count].value = rval;
			auth_opt->count++;
			break;
		case 'n':
			snprintf(myname, sizeof(myname), "%s", optarg);
			break;
		case '?':
			printf("Error: unknown argument: %c\n", optopt);
		default:
			usage(argv);
		}
	}
	if (list_plugins) {
		if (plug_name) {
			if (strcmp(plug_name,"all") == 0) {
				free(plug_name);
				plug_name = NULL;
			}
		}
		ldmsd_plugins_usage(plug_name);
		if (plug_name)
			free(plug_name);
		av_free(auth_opt);
		exit(0);
	}

	if (logfile)
		log_fp = ldmsd_open_log(argv[0]);

	if (!foreground) {
		if (daemon(1, 1)) {
			perror("ldmsd: ");
			cleanup(8, "daemon failed to start");
		}
	}

	ldmsd_ev_init();

	/* Initialize LDMS */
	umask(0);
	if (!max_mem_sz_str) {
		max_mem_sz_str = getenv(LDMSD_MEM_SIZE_ENV);
		if (!max_mem_sz_str)
			max_mem_sz_str = LDMSD_MEM_SIZE_STR;
	}
	if ((max_mem_size = ovis_get_mem_size(max_mem_sz_str)) == 0) {
		printf("Invalid memory size '%s'. See the -m option.\n",
							max_mem_sz_str);
		usage(argv);
	}
	if (ldms_init(max_mem_size)) {
		ldmsd_log(LDMSD_LCRITICAL, "LDMS could not pre-allocate "
				"the memory of size %s.\n", max_mem_sz_str);
		av_free(auth_opt);
		exit(1);
	}

	if (myhostname[0] == '\0') {
		ret = gethostname(myhostname, sizeof(myhostname));
		if (ret)
			myhostname[0] = '\0';
	}

	if (myname[0] == '\0') {
		snprintf(myname, sizeof(myname), "%s:%s", myhostname, port);
	}

	if (!foreground) {
		/* Create pidfile for daemon that usually goes away on exit. */
		/* user arg, then env, then default to get pidfile name */
		if (!pidfile) {
			char *pidpath = getenv("LDMSD_PIDFILE");
			if (!pidpath) {
				pidfile = malloc(strlen(LDMSD_PIDFILE_FMT)
						+ strlen(basename(argv[0]) + 1));
				if (pidfile)
					sprintf(pidfile, LDMSD_PIDFILE_FMT, basename(argv[0]));
			} else {
				pidfile = strdup(pidpath);
			}
			if (!pidfile) {
				ldmsd_log(LDMSD_LERROR, "Out of memory\n");
				av_free(auth_opt);
				exit(1);
			}
		}
		if( !access( pidfile, F_OK ) ) {
			ldmsd_log(LDMSD_LERROR, "Existing pid file named '%s': %s\n",
				pidfile, "overwritten if writable");
		}
		FILE *pfile = fopen_perm(pidfile,"w", LDMSD_DEFAULT_FILE_PERM);
		if (!pfile) {
			int piderr = errno;
			ldmsd_log(LDMSD_LERROR, "Could not open the pid file named '%s': %s\n",
				pidfile, strerror(piderr));
			free(pidfile);
			pidfile = NULL;
		} else {
			pid_t mypid = getpid();
			fprintf(pfile,"%ld\n",(long)mypid);
			fclose(pfile);
		}
		if (pidfile && banner) {
			char *suffix = ".version";
			bannerfile = malloc(strlen(suffix)+strlen(pidfile)+1);
			if (!bannerfile) {
				ldmsd_log(LDMSD_LCRITICAL, "Memory allocation failure.\n");
				av_free(auth_opt);
				exit(1);
			}
			sprintf(bannerfile, "%s%s", pidfile, suffix);
			if( !access( bannerfile, F_OK ) ) {
				ldmsd_log(LDMSD_LERROR, "Existing banner file named '%s': %s\n",
					bannerfile, "overwritten if writable");
			}
			FILE *bfile = fopen_perm(bannerfile,"w", LDMSD_DEFAULT_FILE_PERM);
			if (!bfile) {
				int banerr = errno;
				ldmsd_log(LDMSD_LERROR, "Could not open the banner file named '%s': %s\n",
					bannerfile, strerror(banerr));
				free(bannerfile);
				bannerfile = NULL;
			} else {

#define BANNER_PART1_A "Started LDMS Daemon with authentication "
#define BANNER_PART1_NOA "Started LDMS Daemon without authentication "
#define BANNER_PART2 "version %s. LDMSD Interface Version " \
	"%hhu.%hhu.%hhu.%hhu. LDMS Protocol Version %hhu.%hhu.%hhu.%hhu. " \
	"git-SHA %s\n", PACKAGE_VERSION, \
	ldmsd_version.major, ldmsd_version.minor, \
	ldmsd_version.patch, ldmsd_version.flags, \
	ldms_version.major, ldms_version.minor, ldms_version.patch, \
	ldms_version.flags, OVIS_GIT_LONG

#if OVIS_LIB_HAVE_AUTH
				fprintf(bfile, BANNER_PART1_A
#else /* OVIS_LIB_HAVE_AUTH */
				fprintf(bfile, BANNER_PART1_NOA
#endif /* OVIS_LIB_HAVE_AUTH */
					BANNER_PART2);
				fclose(bfile);
			}
		}
	}

	ev_count = calloc(ev_thread_count, sizeof(int));
	if (!ev_count) {
		ldmsd_log(LDMSD_LCRITICAL, "Memory allocation failure.\n");
		av_free(auth_opt);
		exit(1);
	}
	ovis_scheduler = calloc(ev_thread_count, sizeof(*ovis_scheduler));
	if (!ovis_scheduler) {
		ldmsd_log(LDMSD_LCRITICAL, "Memory allocation failure.\n");
		av_free(auth_opt);
		exit(1);
	}
	ev_thread = calloc(ev_thread_count, sizeof(pthread_t));
	if (!ev_thread) {
		ldmsd_log(LDMSD_LCRITICAL, "Memory allocation failure.\n");
		av_free(auth_opt);
		exit(1);
	}
	for (op = 0; op < ev_thread_count; op++) {
		ovis_scheduler[op] = ovis_scheduler_new();
		if (!ovis_scheduler[op]) {
			ldmsd_log(LDMSD_LERROR, "Error creating an OVIS scheduler.\n");
			cleanup(6, "OVIS scheduler create failed");
		}
		ret = pthread_create(&ev_thread[op], NULL, event_proc, ovis_scheduler[op]);
		if (ret) {
			ldmsd_log(LDMSD_LERROR, "Error %d creating the event "
					"thread.\n", ret);
			cleanup(7, "event thread create fail");
		}
		pthread_setname_np(ev_thread[op], "ldmsd:scheduler");
	}

	/* Create the test sets */
	ldms_set_t *test_sets = calloc(test_set_count, sizeof(ldms_set_t));
	int job_id, comp_id;
	if (test_set_name) {
		int rc, set_no;
		static char test_set_name_no[1024];
		ldms_schema_t schema = ldms_schema_new("test_set");
		if (!schema)
			cleanup(11, "test schema create failed");
		job_id = ldms_schema_meta_add(schema, "job_id", LDMS_V_U32);
		if (job_id < 0)
			cleanup(12, "test schema meta_add jid failed");
		comp_id = ldms_schema_meta_add(schema, "component_id", LDMS_V_U32);
		if (comp_id < 0)
			cleanup(12, "test schema meta_add cid failed");
		rc = ldms_schema_metric_add(schema, "u8_metric", LDMS_V_U8);
		if (rc < 0)
			cleanup(13, "test schema metric_add u8 failed");
		rc = ldms_schema_metric_add(schema, "u16_metric", LDMS_V_U16);
		if (rc < 0)
			cleanup(13, "test schema metric_add u16 failed");
		rc = ldms_schema_metric_add(schema, "u32_metric", LDMS_V_U32);
		if (rc < 0)
			cleanup(13, "test schema metric_add u32 failed");
		rc = ldms_schema_metric_add(schema, "u64_metric", LDMS_V_U64);
		if (rc < 0)
			cleanup(13, "test schema metric_add u64 failed");
		rc = ldms_schema_metric_add(schema, "float_metric", LDMS_V_F32);
		if (rc < 0)
			cleanup(13, "test schema metric_add float failed");
		rc = ldms_schema_metric_add(schema, "double_metric", LDMS_V_D64);
		if (rc < 0)
			cleanup(13, "test schema metric_add double failed");
		rc = ldms_schema_metric_array_add(schema, "char_array_metric",
						  LDMS_V_CHAR_ARRAY, 16);
		if (rc < 0)
			cleanup(13, "test schema metric_add char array failed");
		rc = ldms_schema_metric_array_add(schema, "u8_array_metric",
						  LDMS_V_U8_ARRAY, 4);
		if (rc < 0)
			cleanup(13, "test schema metric_add u8 array failed");
		rc = ldms_schema_metric_array_add(schema, "u16_array_metric",
						  LDMS_V_U16_ARRAY, 4);
		if (rc < 0)
			cleanup(13, "test schema metric_add u16 array failed");
		rc = ldms_schema_metric_array_add(schema, "u32_array_metric",
						  LDMS_V_U32_ARRAY, 4);
		if (rc < 0)
			cleanup(13, "test schema metric_add u32 array failed");
		rc = ldms_schema_metric_array_add(schema, "u64_array_metric",
						  LDMS_V_U64_ARRAY, 4);
		if (rc < 0)
			cleanup(13, "test schema metric_add u64 array failed");
		rc = ldms_schema_metric_array_add(schema, "f32_array_metric",
						  LDMS_V_F32_ARRAY, 4);
		if (rc < 0)
			cleanup(13, "test schema metric_add f32 array failed");
		rc = ldms_schema_metric_array_add(schema, "d64_array_metric",
						  LDMS_V_D64_ARRAY, 4);
		if (rc < 0)
			cleanup(13, "test schema metric_add d64 array failed");
		for (set_no = 1; set_no <= test_set_count; set_no++) {
			sprintf(test_set_name_no, "%s/%s_%d", myhostname,
				test_set_name, set_no);
			test_set = ldms_set_new(test_set_name_no, schema);
			if (!test_set)
				cleanup(14, "test set new failed");
			union ldms_value v;
			v.v_u64 = set_no;
			ldms_metric_set(test_set, comp_id, &v);
			ldms_metric_set(test_set, job_id, &v);
			ldms_set_producer_name_set(test_set, myhostname);
			test_sets[set_no-1] = test_set;
		}
	} else
		test_set_count = 0;

	if (!setfile)
		setfile = LDMSD_SETFILE;


	if (do_kernel && publish_kernel(setfile))
		cleanup(3, "start kernel sampler failed");

	char *xprt_str, *port_str;
	opterr = 0;
	optind = 0;
	while ((op = getopt(argc, argv, FMT)) != -1) {
		char *dup_arg;
		switch (op) {
		case 'x':
			if (check_arg("x", optarg, LO_NAME))
				return 1;
			dup_arg = strdup(optarg);
			xprt_str = strtok(dup_arg, ":");
			port_str = strtok(NULL, ":");
			ldms = listen_on_ldms_xprt(xprt_str, port_str);
			free(dup_arg);
			if (!ldms) {
				cleanup(ret, "Error setting up ldms transport");
			}
			break;
		}
	}

	opterr = 0;
	optind = 0;
	while ((op = getopt(argc, argv, FMT)) != -1) {
		char *dup_arg;
		int lln = -1;
		switch (op) {
		case 'c':
			dup_arg = strdup(optarg);
			ret = process_config_file(dup_arg, &lln, 1);
			free(dup_arg);
			if (ret) {
				char errstr[128];
				snprintf(errstr, sizeof(errstr),
					 "Error %d processing configuration file '%s'",
					 ret, optarg);
				cleanup(ret, errstr);
			}
			ldmsd_log(LDMSD_LINFO, "Processing the config file '%s' is done.\n", optarg);
			break;
		}
	}
	if (ldmsd_use_failover) {
		/* failover will be the one starting cfgobjs */
		ret = ldmsd_failover_start();
		if (ret) {
			ldmsd_log(LDMSD_LERROR,
				  "failover_start failed, rc: %d\n", ret);
			cleanup(100, "failover start failed");
		}
	} else {
		/* we can start cfgobjs right away */
		ret = ldmsd_ourcfg_start_proc();
		if (ret) {
			ldmsd_log(LDMSD_LERROR,
				  "config start failed, rc: %d\n", ret);
			cleanup(100, "config start failed");
		}
		ldmsd_linfo("Enabling in-band config\n");
		ldmsd_inband_cfg_mask_add(0777);
	}

	uint64_t count = 1;
	int name = 0;
	char *names[] = {
		"this",
		"that",
		"the other",
		"biffle"
	};
	int set_no, i;
	ldms_set_t set;
	do {
		for (set_no = 0; set_no < test_set_count; set_no++) {
			set = test_sets[set_no];
			ldms_transaction_begin(set);

			ldms_metric_set_u64(set, 0, count);
			ldms_metric_set_u64(set, 1, count);
			ldms_metric_set_u8 (set, 2, (uint8_t)count);
			ldms_metric_user_data_set (set, 2, count);
			ldms_metric_set_u16(set, 3, (uint16_t)count);
			ldms_metric_set_u32(set, 4, (uint32_t)count);
			ldms_metric_set_u64(set, 5, count);
			ldms_metric_set_float(set, 6, (float)count * 3.1415);
			ldms_metric_set_double(set, 7, (double)count * 3.1415);

			name = (name + 1) % 4;
			for (i = 0; i < 4; i++) {
				ldms_metric_array_set_str(set, 8, names[name]);
				ldms_metric_array_set_u8 (set, 9, i, (uint8_t)(count + i));
				ldms_metric_array_set_u16(set, 10, i, (uint16_t)(count + i));
				ldms_metric_array_set_u32(set, 11, i, (uint32_t)(count + i));
				ldms_metric_array_set_u64(set, 12, i, i + count);
				ldms_metric_array_set_float(set, 13, i,
							    (float)(count + i) * 3.1415);
				ldms_metric_array_set_double(set, 14, i,
							     (double)(count + i) * 3.1415);
			}
			ldms_transaction_end(set);
			if (notify) {
				struct ldms_notify_event_s event;
				ldms_init_notify_modified(&event);
				ldms_notify(set, &event);
			}
		}
		count++;
		usleep(sample_interval);
	} while (1);

	cleanup(0,NULL);
	return 0;
}
