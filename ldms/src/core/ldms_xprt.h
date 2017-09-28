/* -*- c-basic-offset: 8 -*-
 * Copyright (c) 2013-2015 Open Grid Computing, Inc. All rights reserved.
 * Copyright (c) 2013-2015 Sandia Corporation. All rights reserved.
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

#ifndef __LDMS_XPRT_H__
#define __LDMS_XPRT_H__

#include <semaphore.h>
#include <sys/queue.h>

#include <zap/zap.h>

#include "ldms.h"

#include "config.h"

#pragma pack(4)
struct ldms_rbuf_desc {
	struct ldms_xprt *xprt;
	struct ldms_set *set;
	uint64_t remote_set_id;	    /* Remote set id returned by lookup */
	uint64_t local_notify_xid;  /* Value sent in reg_notify */
	uint64_t remote_notify_xid; /* Value received in req_notify */
	uint32_t notify_flags;	    /* What events are notified  */
	struct zap_map *rmap;	/* remote map */
	struct zap_map *lmap;	/* local map */

	enum ldms_rbd_type {
		LDMS_RBD_LOCAL = 1,
		LDMS_RBD_REMOTE,
	} type;

	LIST_ENTRY(ldms_rbuf_desc) set_link; /* list of RBD for a set */
	LIST_ENTRY(ldms_rbuf_desc) xprt_link; /* list of RBD for a transport */
};

enum ldms_request_cmd {
	LDMS_CMD_DIR = 0,
	LDMS_CMD_DIR_CANCEL,
	LDMS_CMD_LOOKUP,
	LDMS_CMD_UPDATE,
	LDMS_CMD_REQ_NOTIFY,
	LDMS_CMD_CANCEL_NOTIFY,
	LDMS_CMD_SEND_MSG,
	LDMS_CMD_REPLY = 0x100,
	LDMS_CMD_DIR_REPLY,
	LDMS_CMD_DIR_CANCEL_REPLY,
	LDMS_CMD_DIR_UPDATE_REPLY,
	LDMS_CMD_LOOKUP_REPLY,
	LDMS_CMD_REQ_NOTIFY_REPLY,
	LDMS_CMD_AUTH_CHALLENGE_REPLY,
	LDMS_CMD_AUTH_APPROVAL_REPLY,
	/* Transport private requests set bit 32 */
	LDMS_CMD_XPRT_PRIVATE = 0x80000000,
};

struct ldms_send_cmd_param {
	uint32_t msg_len;
	char msg[OVIS_FLEX];
};

struct ldms_lookup_cmd_param {
	uint32_t flags;
	uint32_t path_len;
	char path[LDMS_LOOKUP_PATH_MAX+1];
};

struct ldms_dir_cmd_param {
	uint32_t flags;		/*! Directory update flags */
};

struct ldms_req_notify_cmd_param {
	uint64_t set_id;	/*! The set we want notifications for  */
	uint32_t flags;		/*! Events we want  */
};

struct ldms_cancel_notify_cmd_param {
	uint64_t set_id;	/*! The set we want to cancel notifications for  */
};

struct ldms_request_hdr {
	uint64_t xid;		/*! Transaction id returned in reply */
	uint32_t cmd;		/*! The operation being requested  */
	uint32_t len;		/*! The length of the request  */
};

struct ldms_request {
	struct ldms_request_hdr hdr;
	union {
		struct ldms_send_cmd_param send;
		struct ldms_dir_cmd_param dir;
		struct ldms_lookup_cmd_param lookup;
		struct ldms_req_notify_cmd_param req_notify;
		struct ldms_cancel_notify_cmd_param cancel_notify;
	};
};

struct ldms_lookup_msg {
	uint64_t xid;
	uint64_t set_id;
	uint32_t more;
	uint32_t meta_len;
	uint32_t data_len;
	uint32_t card;
	uint32_t schema_len;
	uint32_t inst_name_len;
	char schema_inst_name[OVIS_FLEX]; /* schema name and then instance name */
};

struct ldms_dir_reply {
	uint32_t type;
	uint32_t more;
	uint32_t set_count;
	uint32_t set_list_len;
	char set_list[OVIS_FLEX];
};

struct ldms_req_notify_reply {
	struct ldms_notify_event_s event;
};

#define LDMS_PASSWORD_MAX 128

struct ldms_auth_challenge_reply {
	char s[LDMS_PASSWORD_MAX];
};

struct ldms_reply_hdr {
	uint64_t xid;
	uint32_t cmd;
	uint32_t len;
	uint32_t rc;
};
struct ldms_reply {
	struct ldms_reply_hdr hdr;
	union {
		struct ldms_dir_reply dir;
		struct ldms_req_notify_reply req_notify;
		struct ldms_auth_challenge_reply auth_challenge;
	};
};
#pragma pack()

typedef enum ldms_context_type {
	LDMS_CONTEXT_DIR,
	LDMS_CONTEXT_DIR_CANCEL,
	LDMS_CONTEXT_LOOKUP,
	LDMS_CONTEXT_UPDATE,
	LDMS_CONTEXT_REQ_NOTIFY,
	LDMS_CONTEXT_SEND
} ldms_context_type_t;

struct ldms_context {
	sem_t sem;
	sem_t *sem_p;
	int rc;
	ldms_context_type_t type;
	union {
		struct {
			ldms_dir_cb_t cb;
			void *cb_arg;
		} dir;
		struct {
			char *path;
			ldms_lookup_cb_t cb;
			void *cb_arg;
			int more;
			enum ldms_lookup_flags flags;
			ldms_set_t s;
			zap_map_t remote_map;
		} lookup;
		struct {
			ldms_set_t s;
			ldms_update_cb_t cb;
			void *arg;
		} update;
		struct {
			ldms_set_t s;
			ldms_notify_cb_t cb;
			void *arg;
		} req_notify;
	};
	TAILQ_ENTRY(ldms_context) link;
};

#define LDMS_MAX_TRANSPORT_NAME_LEN 16

struct ldms_xprt {
	char name[LDMS_MAX_TRANSPORT_NAME_LEN];
	uint32_t ref_count;
	pthread_mutex_t lock;

	/* Semaphore and return code for synchronous xprt calls */
	sem_t sem;
	int sem_rc;

	/* Maximum size of a send/recv message */
	int max_msg;
	/* Points to local ctxt expected when dir updates returned to this endpoint */
	uint64_t local_dir_xid;
	/* This is the peers local_dir_xid that we provide when providing dir updates */
	uint64_t remote_dir_xid;

#ifdef DEBUG
	int active_dir; /* Number of outstanding dir requests */
	int active_dir_cancel; /* Number of outstanding dir cancel requests */
	int active_lookup; /* Number of outstanding lookup requests */
#endif /* DEBUG */
	TAILQ_HEAD(, ldms_context) ctxt_list;

	/* Callback that implements xprt state machine on the active side */
	ldms_connect_cb_t connect_cb;
	void *connect_cb_arg;

	ldms_recv_cb_t recv_cb;
	void *recv_cb_arg;

	zap_t zap;
	zap_ep_t zap_ep;

	/* Authentication */
	const char *password;
	enum {
		LDMS_XPRT_AUTH_FAILED = -1, /* authentication failed */
		LDMS_XPRT_AUTH_DISABLE = 0, /* authentication not is used */
		LDMS_XPRT_AUTH_INIT = 1, /* Use authentication */
		LDMS_XPRT_AUTH_PASSWORD = 2, /* Sent password to server */
		LDMS_XPRT_AUTH_APPROVED = 3 /* authentication approved */
	} auth_flag;

	/** Transport message logging callback */
	ldms_log_fn_t log;

	LIST_HEAD(xprt_rbd_list, ldms_rbuf_desc) rbd_list;
	LIST_ENTRY(ldms_xprt) xprt_link;
};

#endif
