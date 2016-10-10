/* -*- c-basic-offset: 8 -*-
 * Copyright (c) 2011-2015 Open Grid Computing, Inc. All rights reserved.
 * Copyright (c) 2011-2015 Sandia Corporation. All rights reserved.
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
#include <inttypes.h>
#include <unistd.h>
#include <stdarg.h>
#include <getopt.h>
#include <stdlib.h>
#include <sys/errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/un.h>
#include <libgen.h>
#include <signal.h>
#include <search.h>
#include "config.h"

#ifdef HAVE_LIBREADLINE
#  if defined(HAVE_READLINE_READLINE_H)
#    include <readline/readline.h>
#  elif defined(HAVE_READLINE_H)
#    include <readline.h>
#  else /* !defined(HAVE_READLINE_H) */
extern char *readline ();
#  endif /* !defined(HAVE_READLINE_H) */
#else /* !defined(HAVE_READLINE_READLINE_H) */
  /* no readline */
#endif /* HAVE_LIBREADLINE */

#ifdef HAVE_READLINE_HISTORY
#  if defined(HAVE_READLINE_HISTORY_H)
#    include <readline/history.h>
#  elif defined(HAVE_HISTORY_H)
#    include <history.h>
#  else /* !defined(HAVE_HISTORY_H) */
extern void add_history ();
extern int write_history ();
extern int read_history ();
#  endif /* defined(HAVE_READLINE_HISTORY_H) */
  /* no history */
#endif /* HAVE_READLINE_HISTORY */

#include "ldms.h"
#include "ldmsd.h"
#include <ovis_ctrl/ctrl.h>
#include <ovis_util/util.h>

#define FMT "S:"
#define ARRAY_SIZE(a)  (sizeof(a) / sizeof(a[0]))

struct attr_value_list *av_list, *kw_list;

void usage(char *argv[])
{
	printf("%s: [%s]\n"
               "    -S <socket>     The UNIX socket that the ldms daemon is listening on.\n"
               "                    [" LDMSD_CONTROL_SOCKNAME "].\n",
               argv[0], FMT);
	exit(1);
}

int handle_help(char *kw, char *err_str)
{
	printf("help\n"
	       "   - Print this menu.\n"
	       "\n"
	       "usage\n"
	       "   - Show loaded plugin usage information.\n"
	       "\n"
	       "load name=<name>\n"
	       "   - Loads the specified plugin. The library that implements\n"
	       "     the plugin should be in the directory specified by the\n"
	       "     LDMSD_PLUGIN_LIBPATH environment variable.\n"
	       "     <name>       The plugin name, this is used to locate a loadable\n"
	       "                  library named \"lib<name>.so\"\n"
	       "\n"
	       "term name=<name>\n"
	       "   - Unloads the specified plugin.\n"
	       "     <name>       The plugin name.\n"
	       "\n"
	       "config name=<name> [ <attr>=<value> ... ]\n"
	       "   - Provides a mechanism to specify configuration options\n"
	       "     <name>       The plugin name.\n"
	       "     <attr>       An attribute name.\n"
	       "     <value>      An attribute value.\n"
	       "\n"
               "udata set=<set_name> metric=<metric_name> udata=<user_data>\n"
               "   - Set the user data of the specified metric in the given set\n"
               "     <set_name>      The metric set name\n"
               "     <metric_name>   The metric name\n"
               "     <user_data>     The user data value\n"
               "\n"
               "udata_regex set=<set_name> regex=<regex> base=<base> [incr=<incr>]\n"
               "   - Set the user data of multiple metrics using regular expression.\n"
               "     The user data of the first matched metric is set to the base value.\n"
               "     The base value is incremented by the given 'incr' value and then\n"
               "     sets to the user data of the consecutive matched metric and so on.\n"
               "     <set_name>      The metric set name\n"
               "     <regex>         A regular expression to match metric names to be set\n"
               "     <base>          The base value of user data (uint64).\n"
               "     <incr>          Increment value (int). The default is 0. If incr is 0,\n"
               "                     the user data of all matched metrics are set\n"
               "                     to the base value.\n"
               "\n"
	       "start name=<name> interval=<interval> [ offset=<offset>]\n"
	       "   - Begins calling the sampler's 'sample' method at the\n"
	       "     sample interval.\n"
	       "     <name>       The sampler name.\n"
	       "     <interval>   The sample interval in microseconds.\n"
	       "     <offset>     Optional offset (shift) from the sample mark\n"
	       "                  in microseconds. Offset can be positive or\n"
	       "                  negative with magnitude up to 1/2 the sample interval.\n"
	       "                  If this offset is specified, including 0, \n"
	       "                  collection will be synchronous; if the offset\n"
	       "                  is not specified, collection will be asychronous.\n"
	       "\n"
	       "stop name=<name>\n"
	       "   - Cancels sampling on the specified plugin.\n"
	       "     <name>       The sampler name.\n"
	       "\n"
	       "add host=<host> type=<type> sets=<set names>\n"
	       "                [ interval=<interval> ] [ offset=<offset>]\n"
	       "                [ xprt=<xprt> ] [ port=<port> ]\n"
	       "                [ standby=<agg_no> ]\n"
	       "   - Adds a host to the list of hosts monitored by this ldmsd.\n"
	       "     <host>       The hostname. This can be an IP address or DNS\n"
	       "                  hostname.\n"
	       "     <type>       One of the following host types: \n"
	       "         active   An connection is initiated with the peer and\n"
	       "                  it's metric sets will be periodically queried.\n"
	       "         passive  A connect request is expected from the specified host.\n"
	       "                  After this request is received, the peer's metric sets\n"
	       "                  will be queried periodically.\n"
	       "         bridging A connect request is initiated to the remote peer,\n"
	       "                  but it's metric sets are not queried. This is the active\n"
	       "                  side of the passive host above.\n"
	       "         local    The to-be-added host is the local host. The given\n"
	       "                  set name(s) must be the name(s) of local set(s).\n"
	       "                  This option is used so that ldmsd can store\n"
	       "                  the given local set(s) if it is configured to do so.\n"
               "     <set names>  The list of metric set names to be queried.\n"
               "		  The list is comma separated.\n"
	       "     <interval>   An optional sampling interval in microseconds,\n"
	       "                  defaults to 1000000.\n"
	       "     <offset>     An optional offset (shift) from the sample mark\n"
	       "                  in microseconds. If this offset is specified,\n "
	       "                  including 0, the collection will be synchronous;\n"
	       "                  if the offset is not specified, the collection\n"
	       "                  will be asychronous\n"
	       "     <xprt>       The transport type, defaults to 'sock'\n"
	       "         sock     The sockets transport.\n"
	       "         rdma     The OFA Verbs Transport for Infiniband or iWARP.\n"
	       "         ugni     The Cray Gemini transport.\n"
	       "     <port>       The port number to connect on, defaults to 50000.\n"
	       "     <agg_no>     The number of the aggregator that this is standby for.\n"
	       "                  Defaults to 0 which means this is an active aggregator.\n"
	       "\n"
	       "store name=<plugin> policy=<policy> container=<container> schema=<schema>\n"
	       "      [hosts=<hosts>] [metric=<metric>,<metric>,...]\n"
	       "   - Saves a metrics from one or more hosts to persistent storage.\n"
	       "      <policy>      The storage policy name. This must be unique.\n"
               "      <container>   The container name used by the plugin to name data.\n"
	       "      <schema>      A name used to name the set of metrics stored together.\n"
	       "      <metrics>     A comma separated list of metric names. If not specified,\n"
	       "                    all metrics in the metric set will be saved.\n"
	       "      <hosts>       The set of hosts whose data will be stored. If hosts is not\n"
	       "                    specified, the metric set will be saved for all hosts. If\n"
	       "                    specified, the value should be a comma separated list of\n"
	       "                    host names.\n"
	       "\n"
	       "standby agg_no=<agg_no> state=<0/1>\n"
	       "   - ldmsd will update the standby state (standby/active) of\n"
	       "     the given aggregator number.\n"
	       "    <agg_no>    Unique integer id for an aggregator from 1 to 64\n"
	       "    <state>     0/1 - standby/active\n"
	       "\n"
	       "oneshot name=<name> time=<time>\n"
	       "   - Schedule a one-shot sample event\n"
	       "     <name>       The sampler name.\n"
	       "     <time>       A Unix timestamp or a special keyword 'now+<second>'\n"
	       "                  The sample will occur at the specified timestamp or at\n"
	       "                  the <second> from now.\n"
	       "\n"
	       "info\n"
	       "   - Causes the ldmsd to dump out information about plugins,\n"
	       "     work queue utilization, hosts and object stores.\n"
	       "\n"
	       "quit\n"
	       "   - Exit.\n");
	return 0;
}

char *err_str;
char *linebuf;
char *sockname = LDMSD_CONTROL_SOCKNAME;
struct ctrlsock *ctrl_sock;

void cleanup()
{
	if (ctrl_sock)
		ctrl_close(ctrl_sock);
}

int handle_usage(char *kw, char *err_str)
{
	return ctrl_request(ctrl_sock, LDMSCTL_LIST_PLUGINS, av_list, err_str);
}

int handle_loglevel(char *kw, char *err_str)
{
	return ctrl_request(ctrl_sock, LDMSCTL_VERBOSE, av_list, err_str);
}

int handle_version(char *kw, char *err_str)
{
	return ctrl_request(ctrl_sock, LDMSCTL_VERSION, av_list, err_str);
}

int handle_plugin_load(char *kw, char *err_str)
{
	return ctrl_request(ctrl_sock, LDMSCTL_LOAD_PLUGIN, av_list, err_str);
}

int handle_oneshot_sample(char *kw, char *err_str)
{
	return ctrl_request(ctrl_sock, LDMSCTL_ONESHOT_SAMPLE, av_list, err_str);
}

int handle_plugin_term(char *kw, char *err_str)
{
	return ctrl_request(ctrl_sock, LDMSCTL_TERM_PLUGIN, av_list, err_str);
}

int handle_plugin_config(char *kw, char *err_str)
{
	return ctrl_request(ctrl_sock, LDMSCTL_CFG_PLUGIN, av_list, err_str);
}

int handle_set_udata(char *kw, char *err_str)
{
	return ctrl_request(ctrl_sock, LDMSCTL_SET_UDATA, av_list, err_str);
}

int handle_set_udata_regex(char *kw, char *err_str)
{
	return ctrl_request(ctrl_sock, LDMSCTL_SET_UDATA_REGEX, av_list, err_str);
}

int handle_sampler_start(char *kw, char *err_str)
{
	return ctrl_request(ctrl_sock, LDMSCTL_START_SAMPLER, av_list, err_str);
}

int handle_sampler_stop(char *kw, char *err_str)
{
	return ctrl_request(ctrl_sock, LDMSCTL_STOP_SAMPLER, av_list, err_str);
}

int handle_host_add(char *kw, char *err_str)
{
	return ctrl_request(ctrl_sock, LDMSCTL_ADD_HOST, av_list, err_str);
}

int handle_update_standby(char *kw, char *err_str)
{
	return ctrl_request(ctrl_sock, LDMSCTL_UPDATE_STANDBY, av_list, err_str);
}

int handle_store(char *kw, char *err_str)
{
	return ctrl_request(ctrl_sock, LDMSCTL_STORE, av_list, err_str);
}

int handle_info(char *kw, char *err_str)
{
	return ctrl_request(ctrl_sock, LDMSCTL_INFO_DAEMON, av_list, err_str);
}

int handle_quit(char *kw, char *err_str)
{
	exit(0);
	return 0;
}

struct kw {
	char *token;
	int (*action)(char *kw, char *err_str);
};

int handle_nxt_token(char *kw, char *err_str);
struct kw keyword_tbl[] = {
	{ "?", handle_help },
	{ "add", handle_host_add },
	{ "config", handle_plugin_config },
	{ "help", handle_help },
	{ "info", handle_info },
	{ "load", handle_plugin_load },
	{ "loglevel", handle_loglevel },
	{ "oneshot", handle_oneshot_sample },
	{ "quit", handle_quit },
	{ "standby", handle_update_standby },
	{ "start", handle_sampler_start },
	{ "stop", handle_sampler_stop },
	{ "store", handle_store },
	{ "term", handle_plugin_term },
	{ "udata", handle_set_udata },
	{ "udata_regex", handle_set_udata_regex },
	{ "usage", handle_usage },
	{ "version", handle_version }
};

static int kw_comparator(const void *a, const void *b)
{
	struct kw *_a = (struct kw *)a;
	struct kw *_b = (struct kw *)b;
	return strcmp(_a->token, _b->token);
}

int nxt_kw;
int handle_nxt_token(char *word, char *err_str)
{
	struct kw key;
	struct kw *kw;

	key.token = av_name(kw_list, nxt_kw);
	kw = bsearch(&key, keyword_tbl, ARRAY_SIZE(keyword_tbl),
		     sizeof(*kw), kw_comparator);
	if (kw) {
		nxt_kw++;
		return kw->action(key.token, err_str);
	}
	printf("Unrecognized keyword '%s'.", key.token);
	return EINVAL;
}

int main(int argc, char *argv[])
{
	int op;
	char *s = NULL;
	int rc;
	size_t cfg_buf_len = LDMSD_MAX_CONFIG_STR_LEN;
	char *env;

	env = getenv("LDMSD_MAX_CONFIG_STR_LEN");
	if (env)
		cfg_buf_len = strtol(env, NULL, 0);
	linebuf = malloc(cfg_buf_len);
	err_str = malloc(cfg_buf_len);
	if (!linebuf | !err_str) {
		printf("Error allocating %zu bytes for configuration buffers.\n",
		       cfg_buf_len);
		exit(1);
	}

	opterr = 0;
	while ((op = getopt(argc, argv, FMT)) != -1) {
		switch (op) {
		case 'S':
			sockname = strdup(optarg);
			break;
		default:
			usage(argv);
		}
	}
	av_list = av_new(128);
	kw_list = av_new(128);

	ctrl_sock = ctrl_connect(basename(argv[0]), sockname,
						"LDMSD_SOCKPATH");
	if (!ctrl_sock) {
		printf("Error setting up connection with ldmsd.\n");
		exit(1);
	}
	atexit(cleanup);
	do {

#ifdef HAVE_LIBREADLINE
#ifndef HAVE_READLINE_HISTORY
		if (s != NULL)
			free(s); /* previous readline output must be freed if not in history */
#endif /* HAVE_READLINE_HISTORY */
		if (isatty(0) ) {
			s = readline("ldmsctl> ");
		} else {
			s = fgets(linebuf, cfg_buf_len, stdin);
		}
#else /* HAVE_LIBREADLINE */
		if (isatty(0) ) {
			fputs("ldmsctl> ", stdout);
		}
		s = fgets(linebuf, cfg_buf_len, stdin);
#endif /* HAVE_LIBREADLINE */
		if (!s)
			break;
#ifdef HAVE_READLINE_HISTORY
		add_history(s);
#endif /* HAVE_READLINE_HISTORY */

		err_str[0] = '\0';
		rc = tokenize(s, kw_list, av_list);
		if (rc) {
			sprintf(err_str, "Memory allocation failure.");
			continue;
		}

		if (!kw_list->count)
			continue;

		struct kw key;
		struct kw *kw;

		nxt_kw = 0;
		key.token = av_name(kw_list, nxt_kw);
		kw = bsearch(&key, keyword_tbl, ARRAY_SIZE(keyword_tbl),
			     sizeof(*kw), kw_comparator);
		if (kw)
			(void)kw->action(key.token, err_str);
		else
			printf("Unrecognized keyword '%s'.\n", key.token);
		if (err_str[0] != '\0')
			printf("%s\n", err_str);
	} while (s);
 	return 0;
}
