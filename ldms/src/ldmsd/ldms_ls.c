/* -*- c-basic-offset: 8 -*-
 * Copyright (c) 2010-2016 Open Grid Computing, Inc. All rights reserved.
 * Copyright (c) 2010-2016 Sandia Corporation. All rights reserved.
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
#include <getopt.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <signal.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <inttypes.h>
#include <fcntl.h>
#include <sys/queue.h>
#include <time.h>
#include <ovis_util/util.h>
#include <semaphore.h>
#include "ldms.h"
#include "ldms_xprt.h"
#include <event2/event.h>
#include "config.h"

#define LDMS_LS_MEM_SZ_ENVVAR "LDMS_LS_MEM_SZ"
#define LDMS_LS_MAX_MEM_SIZE 512L * 1024L * 1024L
#define LDMS_LS_MAX_MEM_SZ_STR "512MB"

static size_t max_mem_size;
static char *mem_sz;

static pthread_mutex_t dir_lock;
static pthread_cond_t dir_cv;
static int dir_done;
static int dir_status;

static pthread_mutex_t print_lock;
static pthread_cond_t print_cv;
static int print_done;

static pthread_mutex_t done_lock;
static pthread_cond_t done_cv;
static int done;

static sem_t conn_sem;

struct ls_set {
	char *name;
	LIST_ENTRY(ls_set) entry;
};
LIST_HEAD(set_list, ls_set) set_list;

void null_log(const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	vfprintf(stderr, fmt, ap);
	fflush(stderr);
}

#define FMT "h:p:x:w:m:ESIlvua:V"
void usage(char *argv[])
{
	printf("%s -h <hostname> -x <transport> [ name ... ]\n"
	       "\n    -h <hostname>    The name of the host to query. Default is localhost.\n"
	       "\n    -p <port_num>    The port number. The default is 50000.\n"
	       "\n    -l               Show the values of the metrics in each metric set.\n"
	       "\n    -u               Show the user-defined metric meta data value.\n"
	       "\n    -x <name>        The transport name: sock, rdma, or local. Default is\n"
	       "                     localhost unless -h is specified in which case it is sock.\n"
	       "\n    -w <secs>        The time to wait before giving up on the server.\n"
	       "                     The default is 10 seconds.\n"
	       "\n    -v               Show detail information about the metric set. Specifying\n"
	       "                     this option multiple times increases the verbosity.\n"
	       "\n    -E               The <name> arguments are regular expressions.\n"
	       "\n    -S               The <name>s refers to the schema name.\n"
	       "\n    -I               The <name>s refer to the instance name (default).\n",
		argv[0]);
	printf("\n    -m <memory size> Maximum size of pre-allocated memory for metric sets.\n"
	       "                     The given size must be less than 1 petabytes.\n"
	       "                     The default is %s.\n"
	       "                     For example, 20M or 20mb are 20 megabytes.\n"
	       "                     - The environment variable %s could be set\n"
	       "                     instead of giving the -m option. If both are given,\n"
	       "                     this option takes precedence over the environment variable.\n",
	       LDMS_LS_MAX_MEM_SZ_STR, LDMS_LS_MEM_SZ_ENVVAR);
#if OVIS_LIB_HAVE_AUTH
	printf("\n    -a <path>        The full Path to the file containing the shared secret word.\n"
	       "                     Set the environment variable %s to the full path\n"
	       "                     to avoid giving it at command-line every time.\n",
		LDMS_AUTH_ENV);
	printf("                     Normally, the environment variable\n"
	       "                     must be set to the full path to the file storing\n"
	       "                     the shared secret word, e.g., secretword=<word>\n");
#endif /* OVIS_LIB_HAVE_AUTH */
	printf("\n    -V               Print LDMS version and exit.\n");

	exit(1);
}

void server_timeout(void)
{
	printf("A timeout occurred waiting for a response from the server.\n"
	       "Use the -w option to specify the amount of time to wait "
	       "for the server\n");
	exit(1);
}

void value_printer(ldms_set_t s, int idx)
{
	enum ldms_value_type type;
	ldms_mval_t val;
	int n, i;

	type = ldms_metric_type_get(s, idx);
	n = ldms_metric_array_get_len(s, idx);
	val = ldms_metric_get(s, idx);

	switch (type) {
	case LDMS_V_CHAR_ARRAY:
		printf("\"%s\"", val->a_char);
		break;
	case LDMS_V_CHAR:
		printf("'%c'", val->v_char);
		break;
	case LDMS_V_U8:
		printf("%hhu", val->v_u8);
		break;
	case LDMS_V_U8_ARRAY:
		for (i = 0; i < n; i++) {
			if (i)
				printf(",");
			printf("0x%02hhx", val->a_u8[i]);
		}
		break;
	case LDMS_V_S8:
		printf("%18hhd", val->v_s8);
		break;
	case LDMS_V_S8_ARRAY:
		for (i = 0; i < n; i++) {
			if (i)
				printf(",");
			printf("%hhd", val->a_s8[i]);
		}
		break;
	case LDMS_V_U16:
		printf("%hu", val->v_u16);
		break;
	case LDMS_V_U16_ARRAY:
		for (i = 0; i < n; i++) {
			if (i)
				printf(",");
			printf("%hu", val->a_u16[i]);
		}
		break;
	case LDMS_V_S16:
		printf("%hd", val->v_s16);
		break;
	case LDMS_V_S16_ARRAY:
		for (i = 0; i < n; i++) {
			if (i)
				printf(",");
			printf("%hd", val->a_s16[i]);
		}
		break;
	case LDMS_V_U32:
		printf("%u", val->v_u32);
		break;
	case LDMS_V_U32_ARRAY:
		for (i = 0; i < n; i++) {
			if (i)
				printf(",");
			printf("%u", val->a_u32[i]);
		}
		break;
	case LDMS_V_S32:
		printf("%d", val->v_s32);
		break;
	case LDMS_V_S32_ARRAY:
		for (i = 0; i < n; i++) {
			if (i)
				printf(",");
			printf("%d", val->a_s32[i]);
		}
		break;
	case LDMS_V_U64:
		printf("%"PRIu64, val->v_u64);
		break;
	case LDMS_V_U64_ARRAY:
		for (i = 0; i < n; i++) {
			if (i)
				printf(",");
			printf("%"PRIu64, val->a_u64[i]);
		}
		break;
	case LDMS_V_S64:
		printf("%"PRId64, val->v_s64);
		break;
	case LDMS_V_S64_ARRAY:
		for (i = 0; i < n; i++) {
			if (i)
				printf(",");
			printf("%"PRId64, val->a_s64[i]);
		}
		break;
	case LDMS_V_F32:
		printf("%f", val->v_f);
		break;
	case LDMS_V_F32_ARRAY:
		for (i = 0; i < n; i++) {
			if (i)
				printf(",");
			printf("%f", val->a_f[i]);
		}
		break;
	case LDMS_V_D64:
		printf("%f", val->v_d);
		break;
	case LDMS_V_D64_ARRAY:
		for (i = 0; i < n; i++) {
			if (i)
				printf(",");
			printf("%f", val->a_d[i]);
		}
		break;
	default:
		printf("Unknown metric type\n");
	}
}

static int user_data = 0;
void metric_printer(ldms_set_t s, int i)
{
	enum ldms_value_type type = ldms_metric_type_get(s, i);
	char name_str[256];

	const char *metname;
	if (type != LDMS_V_NONE) {
		metname = ldms_metric_name_get(s, i);
	} else {
		metname = "SET_ERROR";
	}
	if (user_data)
		sprintf(name_str, "%-42s 0x%" PRIx64,
			metname,
			ldms_metric_user_data_get(s, i));
	else
		strcpy(name_str, metname);

	printf("%c %-10s %-42s ",
	       (ldms_metric_flags_get(s, i) & LDMS_MDESC_F_DATA ? 'D' : 'M'),
	       ldms_metric_type_to_str(type), name_str);

	value_printer(s, i);
	printf("\n");
}

void print_detail(ldms_set_t s)
{
	struct ldms_set_desc *sd = s;
	struct ldms_timestamp _ts = ldms_transaction_timestamp_get(s);
	struct ldms_timestamp _dur = ldms_transaction_duration_get(s);
	struct ldms_timestamp const *ts = &_ts;
	struct ldms_timestamp const *dur = &_dur;
	int consistent = ldms_set_is_consistent(s);
	struct tm *tm;
	char dtsz[200];
	time_t t = ts->sec;
	tm = localtime(&t);
	strftime(dtsz, sizeof(dtsz), "%a %b %d %H:%M:%S %Y %z", tm);

	printf("  METADATA --------\n");
	printf("    Producer Name : %s\n", ldms_set_producer_name_get(s));
	printf("    Instance Name : %s\n", ldms_set_instance_name_get(s));
	printf("      Schema Name : %s\n", ldms_set_schema_name_get(s));
	printf("             Size : %" PRIu32 "\n", ldms_set_meta_sz_get(sd));
	printf("     Metric Count : %" PRIu32 "\n", ldms_set_card_get(s));
	printf("               GN : %" PRIu64 "\n", ldms_set_meta_gn_get(s));
	printf("  DATA ------------\n");
	printf("        Timestamp : %s [%dus]\n", dtsz, ts->usec);
	printf("         Duration : [%d.%06ds]\n", dur->sec, dur->usec);
	printf("       Consistent : %s\n", (consistent?"TRUE":"FALSE"));
	printf("             Size : %" PRIu32 "\n", ldms_set_data_sz_get(sd));
	printf("               GN : %" PRIu64 "\n", ldms_set_data_gn_get(s));
	printf("  -----------------\n");
}

static int verbose = 0;
static int long_format = 0;

void print_cb(ldms_t t, ldms_set_t s, int rc, void *arg)
{
	unsigned long last = (unsigned long)arg;
	struct ldms_timestamp _ts = ldms_transaction_timestamp_get(s);
	struct ldms_timestamp const *ts = &_ts;
	int consistent = ldms_set_is_consistent(s);
	struct tm *tm;
	char dtsz[200];
	time_t ti = ts->sec;
	tm = localtime(&ti);
	strftime(dtsz, sizeof(dtsz), "%a %b %d %H:%M:%S %Y %z", tm);

	printf("%s: %s, last update: %s [%dus]\n",
	       ldms_set_instance_name_get(s),
	       (consistent?"consistent":"inconsistent"), dtsz, ts->usec);
	if (rc) {
		printf("    Error %d updating metric set.\n", rc);
		goto out;
	}
	if (verbose)
		print_detail(s);
	if (long_format) {
		int i;
		for (i = 0; i < ldms_set_card_get(s); i++)
			metric_printer(s, i);
	}
	ldms_set_delete(s);
 out:
	printf("\n");
	pthread_mutex_lock(&print_lock);
	print_done = 1;
	pthread_cond_signal(&print_cv);
	pthread_mutex_unlock(&print_lock);
	if (last) {
		done = 1;
		pthread_cond_signal(&done_cv);
	}
}

void lookup_cb(ldms_t t, enum ldms_lookup_status status,
	       int more,
	       ldms_set_t s, void *arg)
{
	unsigned long last = (unsigned long)arg;
	if (status) {
		last = 1;
		pthread_mutex_lock(&print_lock);
		print_done = 1;
		pthread_cond_signal(&print_cv);
		pthread_mutex_unlock(&print_lock);
		goto err;
	}
	ldms_xprt_update(s, print_cb, (void *)(unsigned long)(last && !more));
	return;
 err:
	printf("ldms_ls: Error %d looking up metric set.\n", status);
	if (status == ENOMEM) {
		printf("Changing the LDMS_LS_MEM_SZ environment variable or the "
				"-m option to a bigger value. The current "
				"value is %s\n", mem_sz);
	}
	if (last && !more) {
		pthread_mutex_lock(&done_lock);
		done = 1;
		pthread_cond_signal(&done_cv);
		pthread_mutex_unlock(&done_lock);
	}
}

static void add_set(char *name)
{
	struct ls_set *lss;

	lss = calloc(1, sizeof(struct ls_set));
	if (!lss) {
		dir_status = ENOMEM;
		return;
	}
	lss->name = strdup(name);
	LIST_INSERT_HEAD(&set_list, lss, entry);
}

void add_set_list(ldms_t t, ldms_dir_t _dir)
{
	int i;
	for (i = 0; i < _dir->set_count; i++)
		add_set(_dir->set_names[i]);
}

void dir_cb(ldms_t t, int status, ldms_dir_t _dir, void *cb_arg)
{
	int more;
	if (status) {
		dir_status = status;
		goto wakeup;
	}
	more = _dir->more;
	add_set_list(t, _dir);
	ldms_xprt_dir_free(t, _dir);
	if (more)
		return;

 wakeup:
	if (!verbose && !long_format)
		done = 1;
	dir_done = 1;
	pthread_cond_signal(&dir_cv);
}

void ldms_connect_cb(ldms_t x, ldms_conn_event_t e, void *cb_arg)
{
	if ((e == LDMS_CONN_EVENT_ERROR) || (e == LDMS_CONN_EVENT_REJECTED)) {
		printf("Connection failed/rejected.\n");
		done = 1;
	}

	sem_post(&conn_sem);
}

void link_libevent(const char *v)
{
	if (strcmp(v,"/")==0) {
#ifdef _EVENT2_EVENT_H_
		event_base_gettimeofday_cached(NULL,NULL);
#endif
	}
}

int main(int argc, char *argv[])
{
	struct ldms_version version;
	struct sockaddr_in sin;
	ldms_t ldms;
	int ret;
	struct hostent *h;
	char *hostname = "localhost";
	unsigned short port_no = LDMS_DEFAULT_PORT;
	int op;
	int i;
	char *xprt = "sock";
	int waitsecs = 10;
	int regex = 0;
	int schema = 0;
	struct timespec ts;
#if OVIS_LIB_HAVE_AUTH
	char *auth_path = 0;
	char *secretword = 0;
#endif
	/* If no arguments are given, print usage. */
	if (argc == 1)
		usage(argv);
	int rc = sem_init(&conn_sem, 0, 0);
	if (rc) {
		perror("sem_init");
		_exit(-1);
	}

	link_libevent(argv[0]);

	opterr = 0;
	while ((op = getopt(argc, argv, FMT)) != -1) {
		switch (op) {
		case 'E':
			regex = 1;
			break;
		case 'S':
			schema = 1;
			break;
		case 'I':
			schema = 0;
			break;
		case 'h':
			hostname = strdup(optarg);
			break;
		case 'p':
			port_no = atoi(optarg);
			break;
		case 'l':
			long_format = 1;
			break;
		case 'u':
			user_data = 1;
			break;
		case 'v':
			verbose++;
			break;
		case 'x':
			xprt = strdup(optarg);
			break;
		case 'w':
			waitsecs = atoi(optarg);
			break;
		case 'm':
			mem_sz = strdup(optarg);
			break;
#if OVIS_LIB_HAVE_AUTH
		case 'a':
			auth_path = strdup(optarg);
			break;
#endif /* OVIS_LIB_HAVE_AUTH */
		case 'V':
			ldms_version_get(&version);
			printf("LDMS_LS Version: %s\n", PACKAGE_VERSION);
			printf("LDMS Protocol Version: %hhu.%hhu.%hhu.%hhu\n",
							version.major,
							version.minor,
							version.patch,
							version.flags);
			exit(0);
			break;
		default:
			usage(argv);
		}
	}

	h = gethostbyname(hostname);
	if (!h) {
		herror(argv[0]);
		usage(argv);
	}

	if (h->h_addrtype != AF_INET)
		usage(argv);

	/* Initialize LDMS */
	if (!mem_sz) {
		mem_sz = getenv(LDMS_LS_MEM_SZ_ENVVAR);
		if (!mem_sz)
			mem_sz = LDMS_LS_MAX_MEM_SZ_STR;
	}
	max_mem_size = ovis_get_mem_size(mem_sz);
	if (!max_mem_size) {
		printf("Invalid memory size '%s'. "
			"See the -m option in the ldms_ls help.\n",
							mem_sz);
		usage(argv);
	}
	if (ldms_init(max_mem_size)) {
		printf("LDMS could not pre-allocate the memory of size %s.\n",
		       mem_sz);
		exit(1);
	}

#if OVIS_LIB_HAVE_AUTH
	if (auth_path && strcmp(auth_path,"none")==0) {
		ldms = ldms_xprt_new(xprt, null_log);
	} else {
		secretword = ldms_get_secretword(auth_path, null_log);
		if (secretword)
			ldms = ldms_xprt_with_auth_new(xprt, null_log, secretword);
		else
			exit(EINVAL);
	}
#else /* OVIS_LIB_HAVE_AUTH */
	ldms = ldms_xprt_new(xprt, null_log);
#endif /* OVIS_LIB_HAVE_AUTH */
	if (!ldms) {
		printf("Error creating transport.\n");
		exit(1);
	}

	memset(&sin, 0, sizeof sin);
	sin.sin_addr.s_addr = *(unsigned int *)(h->h_addr_list[0]);
	sin.sin_family = h->h_addrtype;
	sin.sin_port = htons(port_no);
	if (verbose > 1) {
		printf("Hostname    : %s\n", hostname);
		printf("IP Address  : %s\n", inet_ntoa(sin.sin_addr));
		printf("Port        : %hu\n", port_no);
		printf("Transport   : %s\n", xprt);
	}
	ret  = ldms_xprt_connect(ldms, (struct sockaddr *)&sin, sizeof(sin),
				 ldms_connect_cb, NULL);
	if (ret) {
		perror("ldms_xprt_connect");
		exit(2);
	}

	sem_wait(&conn_sem);
	if (done) {
		/* Connection error/rejected */
		exit(1);
	}
	pthread_mutex_init(&dir_lock, 0);
	pthread_cond_init(&dir_cv, NULL);
	pthread_mutex_init(&done_lock, 0);
	pthread_cond_init(&done_cv, NULL);
	pthread_mutex_init(&print_lock, 0);
	pthread_cond_init(&print_cv, NULL);

	if (optind == argc) {
		ret = ldms_xprt_dir(ldms, dir_cb, NULL, 0);
		if (ret) {
			printf("ldms_dir returned synchronous error %d\n",
			      ret);
			exit(1);
		}
	} else {
		if (!verbose && !long_format)
			usage(argv);
		/*
		 * Set list specified on the command line. Dummy up a
		 * directory and call our ldms_dir callback
		 * function
		 */
		struct ldms_dir_s *dir =
			calloc(1, sizeof(*dir) +
			       ((argc - optind) * sizeof (char *)));
		if (!dir) {
			perror("ldms: ");
			exit(2);
		}
		dir->set_count = argc - optind;
		dir->type = LDMS_DIR_LIST;
		for (i = optind; i < argc; i++)
			dir->set_names[i - optind] = strdup(argv[i]);
		add_set_list(ldms, dir);
		dir_done = 1;	/* no need to wait */
		dir_status = 0;
	}

	clock_gettime(CLOCK_REALTIME, &ts);
	ts.tv_sec += waitsecs;
	pthread_mutex_lock(&dir_lock);
	while (!dir_done)
		ret = pthread_cond_timedwait(&dir_cv, &dir_lock, &ts);
	pthread_mutex_unlock(&dir_lock);
	if (ret)
		server_timeout();

	if (dir_status) {
		printf("Error %d looking up the metric set directory.\n",
		       dir_status);
		exit(3);
	}

	struct ls_set *lss;
	if (LIST_EMPTY(&set_list)) {
		done = 1;
		goto done;
	}
	while (!LIST_EMPTY(&set_list)) {
		enum ldms_lookup_flags flags = 0;
		if (regex)
			flags |= LDMS_LOOKUP_RE;
		if (schema)
			flags |= LDMS_LOOKUP_BY_SCHEMA;
		lss = LIST_FIRST(&set_list);
		LIST_REMOVE(lss, entry);

		if (verbose || long_format) {
			pthread_mutex_lock(&print_lock);
			print_done = 0;
			pthread_mutex_unlock(&print_lock);
			ret = ldms_xprt_lookup(ldms, lss->name, flags,
					       lookup_cb,
					       (void *)(unsigned long)
					       LIST_EMPTY(&set_list));
			if (ret) {
				printf("ldms_xprt_lookup returned %d for set '%s'\n",
				       ret, lss->name);
			}
			pthread_mutex_lock(&print_lock);
			while (!print_done)
				pthread_cond_wait(&print_cv, &print_lock);
			pthread_mutex_unlock(&print_lock);
		} else
			printf("%s\n", lss->name);
	}
done:
	pthread_mutex_lock(&done_lock);
	while (!done)
		pthread_cond_wait(&done_cv, &done_lock);
	pthread_mutex_unlock(&done_lock);

	ldms_xprt_close(ldms);
	exit(0);
}
