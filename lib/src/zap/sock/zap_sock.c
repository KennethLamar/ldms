/* -*- c-basic-offset: 8 -*-
 * Copyright (c) 2014-2016 Open Grid Computing, Inc. All rights reserved.
 * Copyright (c) 2014-2016 Sandia Corporation. All rights reserved.
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

/*
 * Author: Narate Taerat <narate@ogc.us>
 */
#include <sys/errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <assert.h>
#include <endian.h>
#include <signal.h>
#include "coll/rbt.h"
#include "ovis_util/os_util.h"

#include "zap_sock.h"

#define LOG__(ep, ...) do { \
	if (ep && ep->z && ep->z->log_fn) \
		ep->z->log_fn(__VA_ARGS__); \
} while (0);

#define LOG_(sep, ...) do { \
	if (sep && sep->ep.z && sep->ep.z->log_fn) \
		sep->ep.z->log_fn(__VA_ARGS__); \
} while(0);

int init_complete = 0;

static struct event_base *io_event_loop;
static pthread_t io_thread;

static void *io_thread_proc(void *arg);

static void sock_event(struct bufferevent *buf_event, short ev, void *arg);
static void sock_read(struct bufferevent *buf_event, void *arg);
static void sock_write(struct bufferevent *buf_event, void *arg);

static void timeout_cb(int fd , short events, void *arg);
static zap_err_t __setup_connection(struct z_sock_ep *sep);

uint32_t z_last_key;
struct rbt z_key_tree;
pthread_mutex_t z_key_tree_mutex;

LIST_HEAD(, z_sock_ep) z_sock_list = LIST_HEAD_INITIALIZER(0);
pthread_mutex_t z_sock_list_mutex = PTHREAD_MUTEX_INITIALIZER;

static int z_rbn_cmp(void *a, const void *b)
{
	uint32_t x = (uint32_t)(uint64_t)a;
	uint32_t y = (uint32_t)(uint64_t)b;
	return x - y;
}

/**
 * Allocate key for a \c map.
 *
 * \param map The map.
 *
 * \returns NULL on error.
 * \returns Allocated key structure for \c map.
 */
static struct z_sock_key *z_key_alloc(struct zap_sock_map *map)
{
	struct z_sock_key *key = calloc(1, sizeof(*key));
	if (!key)
		return NULL;
	key->map = map;
	pthread_mutex_lock(&z_key_tree_mutex);
	key->rb_node.key = (void*)(uint64_t)(++z_last_key);
	rbt_ins(&z_key_tree, &key->rb_node);
	pthread_mutex_unlock(&z_key_tree_mutex);
	return key;
}

/* Caller must hold the z_key_tree_mutex lock. */
static struct z_sock_key *z_sock_key_find(uint32_t key)
{
	struct rbn *krbn = rbt_find(&z_key_tree, (void*)(uint64_t)key);
	if (!krbn)
		return NULL;
	return container_of(krbn, struct z_sock_key, rb_node);
}

static void z_key_delete(uint32_t key)
{
	struct z_sock_key *k;
	pthread_mutex_lock(&z_key_tree_mutex);
	k = z_sock_key_find(key);
	if (!k)
		goto out;
	rbt_del(&z_key_tree, &k->rb_node);
	free(k);
out:
	pthread_mutex_unlock(&z_key_tree_mutex);
}

/**
 * Validate access by map key.
 *
 * \param key The map key.
 * \param p The start of the accessing memory.
 * \param sz The size of the accessing memory.
 * \param acc Access flags.
 *
 * The Caller must hold the z_key_tree_mutex lock.
 */
static int z_sock_map_key_access_validate(uint32_t key, char *p, size_t sz,
				zap_access_t acc)
{
	struct z_sock_key *k = z_sock_key_find(key);
	if (!k)
		return ENOENT;
	return z_map_access_validate((zap_map_t)k->map, p, sz, acc);
}

static void z_sock_cleanup(void)
{
	void *dontcare;

	if (io_event_loop)
		event_base_loopbreak(io_event_loop);
	if (io_thread) {
		pthread_cancel(io_thread);
		pthread_join(io_thread, &dontcare);
	}
	if (io_event_loop)
		event_base_free(io_event_loop);
}

static zap_err_t z_sock_close(zap_ep_t ep)
{
	struct z_sock_ep *sep = (struct z_sock_ep *)ep;

	pthread_mutex_lock(&sep->ep.lock);
	switch (sep->ep.state) {
	case ZAP_EP_PEER_CLOSE:
	case ZAP_EP_CONNECTED:
	case ZAP_EP_LISTENING:
		sep->ep.state = ZAP_EP_CLOSE;
		shutdown(sep->sock, SHUT_RDWR);
		break;
	case ZAP_EP_ERROR:
	case ZAP_EP_ACCEPTING:
	case ZAP_EP_CONNECTING:
		shutdown(sep->sock, SHUT_RDWR);
		break;
	case ZAP_EP_CLOSE:
		break;
	default:
		ZAP_ASSERT(0, ep, "%s: Unexpected state '%s'\n",
				__func__, zap_ep_state_str(ep->state));
		break;
	}
	pthread_mutex_unlock(&sep->ep.lock);
	return ZAP_ERR_OK;
}

static zap_err_t z_get_name(zap_ep_t ep, struct sockaddr *local_sa,
			    struct sockaddr *remote_sa, socklen_t *sa_len)
{
	struct z_sock_ep *sep = (struct z_sock_ep *)ep;
	int rc;
	*sa_len = sizeof(struct sockaddr_in);
	rc = getsockname(sep->sock, local_sa, sa_len);
	if (rc)
		goto err;
	rc = getpeername(sep->sock, remote_sa, sa_len);
	if (rc)
		goto err;
	return ZAP_ERR_OK;
 err:
	return zap_errno2zerr(errno);
}

static int __set_keep_alive(struct z_sock_ep *sep)
{
	int rc;
	int optval = 0;
	rc = setsockopt(sep->sock, SOL_SOCKET, SO_KEEPALIVE, &optval,
			sizeof(int));
	if (rc) {
		LOG_(sep, "zap_sock: WARNING: set SO_KEEPALIVE error: %d\n", errno);
		return errno;
	}
	optval = ZAP_SOCK_KEEPCNT;
	rc = setsockopt(sep->sock, SOL_TCP, TCP_KEEPCNT, &optval, sizeof(int));
	if (rc) {
		LOG_(sep, "zap_sock: WARNING: set TCP_KEEPCNT error: %d\n", errno);
		return errno;
	}
	optval = ZAP_SOCK_KEEPIDLE;
	rc = setsockopt(sep->sock, SOL_TCP, TCP_KEEPIDLE, &optval, sizeof(int));
	if (rc) {
		LOG_(sep, "zap_sock: WARNING: set TCP_KEEPIDLE error: %d\n", errno);
		return errno;
	}
	optval = ZAP_SOCK_KEEPINTVL;
	rc = setsockopt(sep->sock, SOL_TCP, TCP_KEEPINTVL, &optval, sizeof(int));
	if (rc) {
		LOG_(sep, "zap_sock: WARNING: set TCP_KEEPINTVL error: %d\n", errno);
		return errno;
	}
	return 0;
}

static zap_err_t z_sock_connect(zap_ep_t ep,
				struct sockaddr *sa, socklen_t sa_len,
				char *data, size_t data_len)
{
	int rc;
	zap_err_t zerr;
	struct z_sock_ep *sep = (struct z_sock_ep *)ep;
	zerr = zap_ep_change_state(&sep->ep, ZAP_EP_INIT, ZAP_EP_CONNECTING);
	if (zerr)
		goto out;

	sep->sock = socket(sa->sa_family, SOCK_STREAM, 0);
	if (sep->sock == -1) {
		zerr = ZAP_ERR_RESOURCE;
		goto out;
	}
	rc = evutil_make_socket_nonblocking(sep->sock);
	if (rc) {
		zerr = ZAP_ERR_RESOURCE;
		goto out;
	}
	rc = __set_keep_alive(sep);
	if (rc) {
		LOG_(sep, "zap_sock: WARNING: __set_keep_alive() rc: %d\n", rc);
	}
	zerr = __setup_connection(sep);
	if (zerr)
		goto out;

	if (data_len) {
		sep->conn_data = malloc(data_len);
		if (sep->conn_data)
			memcpy(sep->conn_data, data, data_len);
		else {
			zerr = ZAP_ERR_RESOURCE;
			goto out;
		}
		sep->conn_data_len = data_len;
	}
	zap_get_ep(&sep->ep); /* Release when disconnect */
	/*
	 * Ignore the error code returned from libevent because
	 * it also delivers the error on the event_cb.
	 */
	(void)bufferevent_socket_connect(sep->buf_event, sa, sa_len);
 out:
	return zerr;
}

static void sock_write(struct bufferevent *buf_event, void *arg)
{
	struct z_sock_ep *sep = (struct z_sock_ep *)arg;
	struct evbuffer *evb;
	size_t buflen;
	evb = bufferevent_get_output(buf_event);
	buflen = evbuffer_get_length(evb);
	if (buflen > 0)
		return;
	pthread_mutex_lock(&sep->ep.lock);
	if (sep->rejecting) {
		/*
		 * This is for the server side after it sent
		 * the reject message. Calling shutdown()
		 * here to make sure that the reject msg has
		 * been flushed already.
		 *
		 * The write low-watermark is set to 0.
		 * This guarantees that it will reach this point
		 * because the reject message is the last message
		 * sent to the peer. Eventually the output buffer
		 * length will reach 0.
		 */
		shutdown(sep->sock, SHUT_RDWR);
		sep->ep.state = ZAP_EP_ERROR;
		sep->rejecting = 0;
	}
	pthread_mutex_unlock(&sep->ep.lock);
	return;
}

/**
 * Process an unknown message in the end point.
 */
static void process_sep_msg_unknown(struct z_sock_ep *sep, size_t reqlen)
{
	LOG_(sep, "zap_sock: Unknown zap message.\n");
	struct evbuffer *evb = bufferevent_get_input(sep->buf_event);
	evbuffer_drain(evb, reqlen);
	pthread_mutex_lock(&sep->ep.lock);
	if (sep->ep.state == ZAP_EP_CONNECTED)
		sep->ep.state = ZAP_EP_CLOSE;
	shutdown(sep->sock, SHUT_RDWR);
	pthread_mutex_unlock(&sep->ep.lock);
}

static zap_err_t __sock_recv(struct z_sock_ep *sep, size_t reqlen,
			     struct sock_msg_sendrecv **pmsg)
{
	int rc;
	zap_err_t zerr;
	struct sock_msg_sendrecv *msg;

	msg = malloc(reqlen);
	if (!msg)
		return ZAP_ERR_RESOURCE;

	rc = bufferevent_read(sep->buf_event, msg, reqlen);
	if (rc < reqlen) {
		LOG_(sep, "Expected %d bytes but read %d bytes.\n", reqlen, rc);
		free(msg);
		return ZAP_ERR_TRANSPORT;
	}
	*pmsg = msg;
	return ZAP_ERR_OK;
}

/**
 * Process connect message.
 */
static void process_sep_msg_connect(struct z_sock_ep *sep, size_t reqlen)
{
	struct sock_msg_connect *msg;
	zap_err_t zerr;

	msg = malloc(reqlen);
	if (!msg) {
		LOG_(sep, "Not enough memory in %s\n", __func__);
		shutdown(sep->sock, SHUT_RDWR);
		return;
	}

	bufferevent_read(sep->buf_event, msg, reqlen);

	if (!zap_version_check(&msg->ver)) {
		LOG_(sep, "Connection request from an unsupported Zap version "
				"%hhu.%hhu.%hhu.%hhu\n",
				msg->ver.major, msg->ver.minor,
				msg->ver.patch, msg->ver.flags);
		free(msg);
		shutdown(sep->sock, SHUT_RDWR);
		return;
	}

	if (memcmp(msg->sig, ZAP_SOCK_SIG, sizeof(msg->sig))) {
		LOG_(sep, "Expecting sig '%s', but got '%.*s'.\n",
				ZAP_SOCK_SIG, sizeof(msg->sig), msg->sig);
		free(msg);
		shutdown(sep->sock, SHUT_RDWR);
		return;
	}

	struct zap_event ev = {
		.type = ZAP_EVENT_CONNECT_REQUEST,
		.data = msg->data,
		.data_len = ntohl(msg->data_len),
	};

	sep->ep.cb(&sep->ep, &ev);
	free(msg);

	return;
}

/**
 * Process accept msg
 */
static void process_sep_msg_accepted(struct z_sock_ep *sep, size_t reqlen)
{
	struct sock_msg_sendrecv *msg;
	struct zap_event ev;
	zap_err_t zerr = __sock_recv(sep, reqlen, &msg);
	if (zerr) {
		msg = NULL;
		ev.type = ZAP_EVENT_CONNECT_ERROR;
		ev.status = zerr;
		ev.data = NULL;
		ev.data_len = 0;
		goto out;
	} else {
		ev.type = ZAP_EVENT_CONNECTED;
		ev.status = ZAP_ERR_OK;
		ev.data = msg->data;
		ev.data_len = ntohl(msg->data_len);
	}

	zerr = zap_ep_change_state(&sep->ep, ZAP_EP_CONNECTING, ZAP_EP_CONNECTED);
	if (zerr != ZAP_ERR_OK) {
		LOG_(sep, "'Accept' message received in unexpected state %d.\n",
				sep->ep.state);
		free(msg);
		return;
	}
out:
	sep->ep.cb((void*)sep, &ev);
	if (msg)
		free(msg);
}

/**
 * Process reject msg
 */
static void process_sep_msg_rejected(struct z_sock_ep *sep, size_t reqlen)
{
	zap_err_t zerr;
	struct sock_msg_sendrecv *msg;
	struct zap_event ev;
	zerr = __sock_recv(sep, reqlen, &msg);
	if (zerr) {
		msg = NULL;
		ev.type = ZAP_EVENT_CONNECT_ERROR;
		ev.status = zerr;
		ev.data = NULL;
		ev.data_len = 0;
		goto out;
	} else {
		ev.type = ZAP_EVENT_REJECTED;
		ev.status = ZAP_ERR_OK;
		ev.data = msg->data;
		ev.data_len = ntohl(msg->data_len);
	}

	zerr = zap_ep_change_state(&sep->ep, ZAP_EP_CONNECTING, ZAP_EP_ERROR);
	if (zerr != ZAP_ERR_OK) {
		LOG_(sep, "'reject' message received in unexpected state %d.\n",
				sep->ep.state);
		free(msg);
		return;
	}
out:
	sep->ep.cb((void*)sep, &ev);
	if (msg)
		free(msg);
	shutdown(sep->sock, SHUT_RDWR);
	return;
}

/**
 * Process send/recv message.
 */
static void process_sep_msg_sendrecv(struct z_sock_ep *sep, size_t reqlen)
{
	struct sock_msg_sendrecv *msg;
	struct zap_event ev = {
			.type = ZAP_EVENT_RECV_COMPLETE,
	};
	zap_err_t zerr = __sock_recv(sep, reqlen, &msg);
	if (zerr) {
		ev.status = zerr;
		ev.data = NULL;
		ev.data_len = 0;
		msg = NULL;
	} else {
		ev.status = ZAP_ERR_OK;
		ev.data = msg->data;
		ev.data_len = ntohl(msg->data_len);
	}

	sep->ep.cb(&sep->ep, &ev);
	if (msg)
		free(msg);
}

/**
 * Receiving a read request message.
 */
static void process_sep_msg_read_req(struct z_sock_ep *sep, size_t reqlen)
{
	/* unpack received message */
	struct sock_msg_read_req msg;
	uint32_t data_len;
	char *src;

	bufferevent_read(sep->buf_event, &msg, sizeof(msg));

	/* Need to swap locally interpreted values */
	data_len = ntohl(msg.data_len);
	src = (char *)be64toh(msg.src_ptr);

	/* Prepare response message */
	struct sock_msg_read_resp rmsg;
	rmsg.hdr.msg_type = htons(SOCK_MSG_READ_RESP);
	rmsg.hdr.xid = msg.hdr.xid;
	rmsg.hdr.ctxt = msg.hdr.ctxt;

	int rc = 0;
	pthread_mutex_lock(&z_key_tree_mutex);
	rc = z_sock_map_key_access_validate(msg.src_map_key, src, data_len,
				       ZAP_ACCESS_READ);
	pthread_mutex_unlock(&z_key_tree_mutex);
	/*
	 * The data the other side receives could be garbage
	 * if the map is deleted after this point.
	 */
	rmsg.data_len = 0;
	switch (rc) {
	case 0:	/* OK */

		rmsg.status = 0;
		rmsg.data_len = msg.data_len; /* Still in BE */
		break;
	case EACCES:
		rmsg.status = htons(ZAP_ERR_REMOTE_PERMISSION);
		break;
	case ERANGE:
		rmsg.status = htons(ZAP_ERR_REMOTE_LEN);
		break;
	case ENOENT:
		rmsg.status = htons(ZAP_ERR_REMOTE_MAP);
		break;
	default:
		rmsg.status = htons(ZAP_ERR_PARAMETER);
		break;
	}
	struct evbuffer *ebuf = evbuffer_new();
	if (!ebuf)
		goto res_err;

	rmsg.hdr.msg_len = htonl(sizeof(rmsg) + data_len);
	if (evbuffer_add(ebuf, &rmsg, sizeof(rmsg)) != 0)
		goto res_err;
	if (rmsg.data_len) {
		if (evbuffer_add(ebuf, src, data_len))
			goto res_err;
	}
	if (bufferevent_write_buffer(sep->buf_event, ebuf) != 0)
		LOG_(sep, "bufferevent_write_buffer error in %s at %s:%d\n",
						__func__, __FILE__, __LINE__);
	evbuffer_free(ebuf);
	return;
res_err:
	if (ebuf)
		evbuffer_free(ebuf);
	rmsg.status = htons(ZAP_ERR_RESOURCE);
	rmsg.data_len = 0;
	rmsg.hdr.msg_len = htonl(sizeof(rmsg));
	if (bufferevent_write(sep->buf_event, &rmsg, sizeof(rmsg)) != 0)
		LOG_(sep, "bufferevent_write error in %s at %s:%d\n",
						__func__, __FILE__, __LINE__);
}

struct z_sock_io *__sock_io_alloc(struct z_sock_ep *sep)
{
	struct z_sock_io *io;
	pthread_mutex_lock(&sep->ep.lock);
	if (!TAILQ_EMPTY(&sep->free_q)) {
		io = TAILQ_FIRST(&sep->free_q);
		TAILQ_REMOVE(&sep->free_q, io, q_link);
	} else
		io = calloc(1, sizeof(*io));
	pthread_mutex_unlock(&sep->ep.lock);
	return io;
}

void __sock_io_free(struct z_sock_ep *sep, struct z_sock_io *io)
{
	pthread_mutex_lock(&sep->ep.lock);
	TAILQ_INSERT_TAIL(&sep->free_q, io, q_link);
	pthread_mutex_unlock(&sep->ep.lock);
}

/**
 * Receiving a read response message.
 */
static void process_sep_msg_read_resp(struct z_sock_ep *sep, size_t reqlen)
{
	struct z_sock_io *io;
	struct sock_msg_read_resp msg;
	uint32_t data_len;
	char *dst;
	int rc;

	bufferevent_read(sep->buf_event, &msg, sizeof(msg));

	/* Get the matching request from the io_q */
	pthread_mutex_lock(&sep->ep.lock);
	io = TAILQ_FIRST(&sep->io_q);
	ZAP_ASSERT(io, (&sep->ep), "%s: The io_q is empty.\n", __func__);
	ZAP_ASSERT(msg.hdr.xid == io->hdr.xid, (&sep->ep),
			"%s: The transaction IDs mismatched between the "
			"IO entry %d and message %d.\n", __func__,
			io->hdr.xid, msg.hdr.xid);
	TAILQ_REMOVE(&sep->io_q, io, q_link);
	pthread_mutex_unlock(&sep->ep.lock);

	data_len = ntohl(msg.data_len);

	if (msg.status == 0) {
		/* Read the data into the local memory after
		 * validating the map. We only need validate base and
		 * bounds because this is local access which is always
		 * allowed. */
		rc = z_map_access_validate(io->dst_map, io->dst_ptr,
					   data_len, 0);
		switch (rc) {
		case 0:
			bufferevent_read(sep->buf_event, io->dst_ptr, data_len);
			break;
		case EACCES:
			rc = ZAP_ERR_LOCAL_PERMISSION;
			break;
		case ERANGE:
			rc = ZAP_ERR_LOCAL_LEN;
			break;
		}
		/* If there's an error, we still need to consume the
		 * data, or the record boundary will be broken */
		if (rc) {
			struct evbuffer *evb = bufferevent_get_input(sep->buf_event);
			evbuffer_drain(evb, data_len);
		}
	}
	__sock_io_free(sep, io);

	struct zap_event ev = {
		.type = ZAP_EVENT_READ_COMPLETE,
		.status = rc,
		.context = (void*) msg.hdr.ctxt
	};
	sep->ep.cb((void*)sep, &ev);
}

static uint32_t g_xid = 0;
static void
z_sock_hdr_init(struct sock_msg_hdr *hdr, uint32_t xid,
	   uint16_t type, uint32_t len, uint64_t ctxt)
{
	if (!xid)
		hdr->xid = __sync_add_and_fetch(&g_xid, 1);
	else
		hdr->xid = xid;
	hdr->msg_type = htons(type);
	hdr->msg_len = htonl(len);
	hdr->ctxt = ctxt;
}

/**
 * Receiving a write request message.
 */
static void process_sep_msg_write_req(struct z_sock_ep *sep, size_t reqlen)
{
	char *dst;
	uint32_t data_len;
	struct sock_msg_write_req msg;
	bufferevent_read(sep->buf_event, &msg, sizeof(msg));

	dst = (void *)(unsigned long)be64toh(msg.dst_ptr);
	data_len = ntohl(msg.data_len);

	/* Prepare the response message */
	struct sock_msg_write_resp rmsg;
	z_sock_hdr_init(&rmsg.hdr, msg.hdr.xid, SOCK_MSG_WRITE_RESP, sizeof(rmsg),
		   msg.hdr.ctxt);

	/* Validate */
	pthread_mutex_lock(&z_key_tree_mutex);
	int rc = z_sock_map_key_access_validate(msg.dst_map_key, dst, data_len,
					     ZAP_ACCESS_WRITE);
	pthread_mutex_unlock(&z_key_tree_mutex);

	size_t lsz = data_len;
	size_t sz;
	switch (rc) {
	case 0:
		/* Write into the destination address */
		while (lsz) {
			sz = bufferevent_read(sep->buf_event, dst, lsz);
			dst += sz;
			lsz -= sz;
		}
		rmsg.status = htons(ZAP_ERR_OK);
		break;
	case EACCES:
		rmsg.status = htons(ZAP_ERR_REMOTE_PERMISSION);
		break;
	case ERANGE:
		rmsg.status = htons(ZAP_ERR_REMOTE_LEN);
		break;
	case ENOENT:
		rmsg.status = htons(ZAP_ERR_REMOTE_MAP);
		break;
	}
	if (rc) {
		/* In the case of write request failure, we still
		 * have to drain the data out. */
		struct evbuffer *evb = bufferevent_get_input(sep->buf_event);
		evbuffer_drain(evb, data_len);
	}
	bufferevent_write(sep->buf_event, &rmsg, sizeof(rmsg));
}

/**
 * Receiving a write response message.
 */
static void process_sep_msg_write_resp(struct z_sock_ep *sep, size_t reqlen)
{
	struct z_sock_io *io;
	struct sock_msg_write_resp msg;
	bufferevent_read(sep->buf_event, &msg, sizeof(msg));

	/* Our request should be on the head of the ep->io_q queue. */
	pthread_mutex_lock(&sep->ep.lock);
	/* Take it off the I/O q */
	io = TAILQ_FIRST(&sep->io_q);
	ZAP_ASSERT(io, &sep->ep, "%s: The io_q is empty\n", __func__);
	TAILQ_REMOVE(&sep->io_q, io, q_link);
	ZAP_ASSERT(io, &sep->ep, "%s: The transaction IDs mismatched "
			"between the IO entry %d and message %d.\n",
			__func__, io->hdr.xid, msg.hdr.xid);
	/* Put it back on the free_q */
	pthread_mutex_unlock(&sep->ep.lock);
	__sock_io_free(sep, io);

	struct zap_event ev = {
		.type = ZAP_EVENT_WRITE_COMPLETE,
		.status = ntohs(msg.status),
		.context = (void*) msg.hdr.ctxt
	};
	sep->ep.cb(&sep->ep, &ev);
}

/**
 * Receiving a rendezvous (share) message.
 */
static void process_sep_msg_rendezvous(struct z_sock_ep *sep, size_t reqlen)
{
	struct sock_msg_rendezvous msg;
	bufferevent_read(sep->buf_event, &msg, sizeof(msg));
	struct zap_sock_map *map = calloc(1, sizeof(*map));
	if (!map) {
		LOG_(sep, "ENOMEM in %s at %s:%d\n",
				__func__, __FILE__, __LINE__);
		goto err0;
	}

	char *amsg = NULL;
	size_t amsg_len = ntohl(msg.hdr.msg_len) - sizeof(msg);
	if (amsg_len) {
		amsg = malloc(amsg_len);
		if (!amsg) {
			LOG_(sep, "ENOMEM in %s at %s:%d\n",
					__func__, __FILE__, __LINE__);
			goto err1;
		}
		size_t rb = bufferevent_read(sep->buf_event, amsg, amsg_len);
		if (rb != amsg_len) {
			/* read error */
			goto err2;
		}
	}

	map->map.ep = &sep->ep;
	map->key = msg.rmap_key;
	map->map.acc = ntohl(msg.acc);
	map->map.type = ZAP_MAP_REMOTE;
	map->map.addr = (void *)(uint64_t)be64toh((uint64_t)msg.addr);
	map->map.len = ntohl(msg.data_len);

	zap_get_ep(&sep->ep); /* Release when app calls zap_unmap(). */
	pthread_mutex_lock(&sep->ep.lock);
	LIST_INSERT_HEAD(&sep->ep.map_list, &map->map, link);
	pthread_mutex_unlock(&sep->ep.lock);

	struct zap_event ev = {
		.type = ZAP_EVENT_RENDEZVOUS,
		.map = (void*)map,
		.data_len = amsg_len,
		.data = amsg
	};

	sep->ep.cb((void*)sep, &ev);

	free(amsg); /* map is owned by cb() function, but amsg is not. */
	return;
err2:
	free(amsg);
err1:
	free(map);
err0:
	return;
}

typedef void(*process_sep_msg_fn_t)(struct z_sock_ep*, size_t reqlen);
static process_sep_msg_fn_t process_sep_msg_fns[] = {
	[SOCK_MSG_SENDRECV] = process_sep_msg_sendrecv,
	[SOCK_MSG_READ_REQ] = process_sep_msg_read_req,
	[SOCK_MSG_READ_RESP] = process_sep_msg_read_resp,
	[SOCK_MSG_WRITE_REQ] = process_sep_msg_write_req,
	[SOCK_MSG_WRITE_RESP] = process_sep_msg_write_resp,
	[SOCK_MSG_RENDEZVOUS] = process_sep_msg_rendezvous,
	[SOCK_MSG_CONNECT] = process_sep_msg_connect,
	[SOCK_MSG_ACCEPTED] = process_sep_msg_accepted,
	[SOCK_MSG_REJECTED] = process_sep_msg_rejected,
};

#define min_t(t, x, y) (t)((t)x < (t)y?(t)x:(t)y)
static void sock_read(struct bufferevent *buf_event, void *arg)
{
	struct z_sock_ep *sep = (struct z_sock_ep *)arg;
	struct evbuffer *evb;
	struct sock_msg_hdr hdr;
	size_t len;
	size_t reqlen;
	size_t buflen;
	enum sock_msg_type msg_type;
	do {
		evb = bufferevent_get_input(buf_event);
		buflen = evbuffer_get_length(evb);
		if (buflen < sizeof(hdr))
			break;
		evbuffer_copyout(evb, &hdr, sizeof(hdr));
		reqlen = ntohl(hdr.msg_len);
		if (buflen < reqlen)
			break;
		msg_type = ntohs(hdr.msg_type);
		if (msg_type < SOCK_MSG_TYPE_LAST)
			process_sep_msg_fns[msg_type](sep, reqlen);
		else /* unknown type */
			process_sep_msg_unknown(sep, reqlen);
	} while (1);
}

static void *io_thread_proc(void *arg)
{
	/* Zap thread will not handle any signal */
	int rc;
	sigset_t sigset;
	sigfillset(&sigset);
	rc = pthread_sigmask(SIG_BLOCK, &sigset, NULL);
	assert(rc == 0 && "pthread_sigmask error");
	event_base_dispatch(io_event_loop);
	return NULL;
}

static void release_buf_event(struct z_sock_ep *sep)
{
	if (sep->listen_ev) {
		evconnlistener_free(sep->listen_ev);
		sep->listen_ev = NULL;
	}
	if (sep->buf_event) {
		bufferevent_free(sep->buf_event);
		sep->buf_event = NULL;
	}

	if (sep->sock > -1) {
		close(sep->sock);
		sep->sock = -1;
	}
}

zap_event_type_t ev_type_cvt[] = {
	[SOCK_MSG_SENDRECV] = ZAP_EVENT_RECV_COMPLETE,
	[SOCK_MSG_RENDEZVOUS] = -1,
	[SOCK_MSG_READ_REQ] = ZAP_EVENT_READ_COMPLETE,
	[SOCK_MSG_READ_RESP] = -1,
	[SOCK_MSG_WRITE_REQ] = ZAP_EVENT_WRITE_COMPLETE,
	[SOCK_MSG_WRITE_RESP] = -1,
	[SOCK_MSG_ACCEPTED] = -1
};

static zap_err_t __sock_send_connect(struct z_sock_ep *sep, char *buf, size_t len)
{
	struct sock_msg_connect msg;
	struct evbuffer *ebuf = evbuffer_new();
	if (!ebuf)
		return ZAP_ERR_RESOURCE;
	z_sock_hdr_init(&msg.hdr, 0, SOCK_MSG_CONNECT, (uint32_t)(sizeof(msg) + len), 0);
	msg.data_len = htonl(len);
	ZAP_VERSION_SET(msg.ver);
	memcpy(&msg.sig, ZAP_SOCK_SIG, sizeof(msg.sig));

	if (evbuffer_add(ebuf, &msg, sizeof(msg)) != 0)
		goto err;
	if (evbuffer_add(ebuf, buf, len) != 0)
		goto err;

	/* This write will drain ebuf, appending data to sep->buf_event
	 * without unnecessary memory copying. */
	if (bufferevent_write_buffer(sep->buf_event, ebuf) != 0)
		goto err;

	/* we don't need ebuf anymore */
	evbuffer_free(ebuf);
	return ZAP_ERR_OK;
err:
	evbuffer_free(ebuf);
	return ZAP_ERR_RESOURCE;
}

static zap_err_t __sock_send(struct z_sock_ep *sep, uint16_t msg_type,
		char *buf, size_t len)
{
	struct sock_msg_sendrecv msg;
	struct evbuffer *ebuf = evbuffer_new();
	if (!ebuf)
		return ZAP_ERR_RESOURCE;

	z_sock_hdr_init(&msg.hdr, 0, msg_type, (uint32_t)(sizeof(msg) + len), 0);
	msg.data_len = htonl(len);

	if (evbuffer_add(ebuf, &msg, sizeof(msg)) != 0)
		goto err;
	if (evbuffer_add(ebuf, buf, len) != 0)
		goto err;

	/* This write will drain ebuf, appending data to sep->buf_event
	 * without unnecessary memory copying. */
	if (bufferevent_write_buffer(sep->buf_event, ebuf) != 0)
		goto err;

	/* we don't need ebuf anymore */
	evbuffer_free(ebuf);
	return ZAP_ERR_OK;
err:
	evbuffer_free(ebuf);
	return ZAP_ERR_RESOURCE;
}

static void sock_event(struct bufferevent *buf_event, short bev, void *arg)
{
	zap_err_t zerr;
	struct z_sock_ep *sep = arg;
	struct zap_event ev = { 0 };
	static const short bev_mask = BEV_EVENT_EOF | BEV_EVENT_ERROR |
				     BEV_EVENT_TIMEOUT;

	if (bev & BEV_EVENT_CONNECTED) {
		/*
		 * This is BEV_EVENT_CONNECTED on initiator side. Send
		 * connect data
		 */
		if (bufferevent_enable(sep->buf_event, EV_READ | EV_WRITE)) {
			LOG_(sep, "Error enabling buffered I/O event for fd %d.\n",
					sep->sock);
		}
		zerr = __sock_send_connect(sep, sep->conn_data, sep->conn_data_len);
		if (sep->conn_data)
			free(sep->conn_data);
		sep->conn_data = NULL;
		sep->conn_data_len = 0;
		if (zerr) {
			zap_ep_change_state(&sep->ep, ZAP_EP_CONNECTING,
					ZAP_EP_ERROR);
			ev.type = ZAP_EVENT_CONNECT_ERROR;
			ev.status = zerr;
			sep->ep.cb(&(sep->ep), &ev);
			shutdown(sep->sock, SHUT_RDWR);
		}
		return;
	}

	/* Reaching here means bev is one of the EOF, ERROR or TIMEOUT */
	int do_cb;
	pthread_mutex_lock(&sep->ep.lock);
	bufferevent_setcb(sep->buf_event, NULL, NULL, NULL, NULL);
	switch (sep->ep.state) {
	case ZAP_EP_ACCEPTING:
		sep->ep.state = ZAP_EP_ERROR;
		do_cb = 0;
		break;
	case ZAP_EP_CONNECTING:
		ev.type = ZAP_EVENT_CONNECT_ERROR;
		sep->ep.state = ZAP_EP_ERROR;
		shutdown(sep->sock, SHUT_RDWR);
		do_cb = 1;
		break;
	case ZAP_EP_CONNECTED:	/* Peer closed. */
		sep->ep.state = ZAP_EP_PEER_CLOSE;
		shutdown(sep->sock, SHUT_RDWR); /* disallow further i/o from our side */
	case ZAP_EP_CLOSE:	/* App called close. */
		ev.type = ZAP_EVENT_DISCONNECTED;
		do_cb = 1;
		break;
	case ZAP_EP_ERROR:
		do_cb = 0;
		break;
	default:
		LOG_(sep, "Unexpected state for EOF %d.\n",
		     sep->ep.state);
		sep->ep.state = ZAP_EP_ERROR;
		do_cb = 0;
		break;
	}

	/* Complete all outstanding I/O with ZEP_ERR_FLUSH */
	while (!TAILQ_EMPTY(&sep->io_q)) {
		zap_event_type_t ev_type;
		sock_msg_type_t msg_type;
		struct z_sock_io *io = TAILQ_FIRST(&sep->io_q);
		TAILQ_REMOVE(&sep->io_q, io, q_link);

		msg_type = ntohs(io->hdr.msg_type);
		ev_type = ev_type_cvt[msg_type];

		/* Call the completion routine */
		struct zap_event ev = {
			.type = ev_type,
			.status = ZAP_ERR_FLUSH,
			.context = (void *)io->hdr.ctxt
		};
		free(io);	/* Don't put back on free_q, we're closing */
		sep->ep.cb(&sep->ep, &ev);
	}
	pthread_mutex_unlock(&sep->ep.lock);
	if (do_cb)
		sep->ep.cb((void*)sep, &ev);

	zap_put_ep(&sep->ep); /* Taken when connect or accept */
	return;
}

static zap_err_t
__setup_connection(struct z_sock_ep *sep)
{
#ifdef DEBUG
	sep->ep.z->log_fn("SOCK: setting up endpoint %p\n", &sep->ep);
#endif
	/* Initialize send and recv I/O events */
	sep->buf_event = bufferevent_socket_new(io_event_loop, sep->sock,
						BEV_OPT_THREADSAFE|
						BEV_OPT_DEFER_CALLBACKS|
						BEV_OPT_UNLOCK_CALLBACKS);
	if(!sep->buf_event) {
		LOG_(sep, "Error initializing buffered I/O event for "
		     "fd %d.\n", sep->sock);
		return ZAP_ERR_RESOURCE;
	}

	bufferevent_setcb(sep->buf_event, sock_read, NULL, sock_event, sep);

	return ZAP_ERR_OK;
}

/**
 * This is a callback function for evconnlistener_new_bind (in z_sock_listen).
 */
static void __z_sock_conn_request(struct evconnlistener *listener,
			 evutil_socket_t sockfd,
			 struct sockaddr *address, int socklen, void *arg)
{
	struct z_sock_ep *sep = arg;
	zap_ep_t new_ep;
	struct z_sock_ep *new_sep;
	zap_err_t zerr;

	new_ep = zap_new(sep->ep.z, sep->ep.app_cb);
	if (!new_ep) {
		zerr = errno;
		LOG_(sep, "Zap Error %d (%s): in %s at %s:%d\n",
				zerr, zap_err_str(zerr) , __func__, __FILE__,
				__LINE__);
		return;
	}
	void *uctxt = zap_get_ucontext(&sep->ep);
	zap_set_ucontext(new_ep, uctxt);
	new_sep = (void*) new_ep;
	new_sep->sock = sockfd;
	new_sep->ep.state = ZAP_EP_ACCEPTING;

	zerr = __setup_connection(new_sep);
	if (zerr || bufferevent_enable(new_sep->buf_event, EV_READ | EV_WRITE)) {
		LOG_(sep, "Error setting up new endpoint on fd %d.\n", new_sep->sock);
		new_sep->ep.state = ZAP_EP_ERROR;
		shutdown(sep->sock, SHUT_RDWR);
	} else {
		/*
		 * Turn on the write callback, in case that the application
		 * rejects the request.
		 */
		bufferevent_setcb(new_sep->buf_event, sock_read,
				sock_write, sock_event, new_sep);
	}
}

static void __listener_err_cb(struct evconnlistener *listen_ev, void *args)
{
#ifdef DEBUG
	struct z_sock_ep *sep = (struct z_sock_ep *)args;
	sep->ep.z->log_fn("SOCK: libevent error '%s'\n", strerror(errno));
#endif
}

static zap_err_t z_sock_listen(zap_ep_t ep, struct sockaddr *sa,
				socklen_t sa_len)
{
	struct z_sock_ep *sep = (struct z_sock_ep *)ep;
	zap_err_t zerr;

	zerr = zap_ep_change_state(&sep->ep, ZAP_EP_INIT, ZAP_EP_LISTENING);
	if (zerr)
		goto err_0;

	zerr = ZAP_ERR_RESOURCE;
	sep->listen_ev = evconnlistener_new_bind(io_event_loop,
					       __z_sock_conn_request, sep,
					       LEV_OPT_THREADSAFE |
					       LEV_OPT_REUSEABLE, 1024, sa,
					       sa_len);
	if (!sep->listen_ev)
		goto err_0;

	evconnlistener_set_error_cb(sep->listen_ev, __listener_err_cb);

	sep->sock = evconnlistener_get_fd(sep->listen_ev);
	return ZAP_ERR_OK;

 err_0:
	return zerr;
}

static zap_err_t z_sock_send(zap_ep_t ep, char *buf, size_t len)
{
	struct z_sock_ep *sep = (struct z_sock_ep *)ep;
	zap_err_t zerr = ZAP_ERR_OK;
	pthread_mutex_lock(&sep->ep.lock);
	if (ep->state != ZAP_EP_CONNECTED) {
		zerr = ZAP_ERR_NOT_CONNECTED;
		goto out;
	}

	zerr = __sock_send(sep, SOCK_MSG_SENDRECV, buf, len);
out:
	pthread_mutex_unlock(&sep->ep.lock);
	return zerr;
}

static struct timeval to;
static struct event *keepalive;
static void timeout_cb(int s, short events, void *arg)
{
	to.tv_sec = 10;
	to.tv_usec = 0;
	evtimer_add(keepalive, &to);
}

static int init_once()
{
	int rc = ENOMEM;

	evthread_use_pthreads();
	io_event_loop = event_base_new();
	if (!io_event_loop)
		return errno;

	keepalive = evtimer_new(io_event_loop, timeout_cb, NULL);
	if (!keepalive)
		goto err_1;

	to.tv_sec = 1;
	to.tv_usec = 0;
	evtimer_add(keepalive, &to);

	rc = pthread_create(&io_thread, NULL, io_thread_proc, 0);
	if (rc)
		goto err_1;

	init_complete = 1;

	z_key_tree.root = NULL;
	z_key_tree.comparator = z_rbn_cmp;
	pthread_mutex_init(&z_key_tree_mutex, NULL);
	return 0;

 err_1:
	event_base_free(io_event_loop);
	return rc;
}

static zap_ep_t z_sock_new(zap_t z, zap_cb_fn_t cb)
{
	struct z_sock_ep *sep = calloc(1, sizeof(*sep));
	if (!sep) {
		errno = ZAP_ERR_RESOURCE;
		return NULL;
	}
	TAILQ_INIT(&sep->free_q);
	TAILQ_INIT(&sep->io_q);
	sep->sock = -1;
	pthread_mutex_lock(&z_sock_list_mutex);
	LIST_INSERT_HEAD(&z_sock_list, sep, link);
	pthread_mutex_unlock(&z_sock_list_mutex);

	return (zap_ep_t)sep;
}

static void z_sock_destroy(zap_ep_t ep)
{
	struct z_sock_io *io;
	struct z_sock_ep *sep = (struct z_sock_ep *)ep;

#ifdef DEBUG
	ep->z->log_fn("SOCK: destroying endpoint %p\n", ep);
#endif
	if (sep->conn_data)
		free(sep->conn_data);
	release_buf_event(sep);
	/* all pending I/O should have been flushed */
	ZAP_ASSERT(TAILQ_EMPTY(&sep->io_q), ep, "%s: The io_q is not empty "
			"when the reference count reaches 0.\n", __func__);
	while (!TAILQ_EMPTY(&sep->free_q)) {
		io = TAILQ_FIRST(&sep->free_q);
		TAILQ_REMOVE(&sep->free_q, io, q_link);
		free(io);
	}
	pthread_mutex_lock(&z_sock_list_mutex);
	LIST_REMOVE(sep, link);
	pthread_mutex_unlock(&z_sock_list_mutex);
	free(ep);
}

zap_err_t z_sock_accept(zap_ep_t ep, zap_cb_fn_t cb, char *data, size_t data_len)
{
	/* ep is the newly created ep from __z_sock_conn_request */
	struct z_sock_ep *sep = (struct z_sock_ep *)ep;
	zap_err_t zerr;

	pthread_mutex_lock(&sep->ep.lock);
	/* Disable the write callback. We use it only for the rejecting case */
	bufferevent_setcb(sep->buf_event, sock_read, NULL, sock_event, sep);

	if (sep->ep.state != ZAP_EP_ACCEPTING) {
		zerr = ZAP_ERR_ENDPOINT;
		goto err_0;
	}

	/* Replace the callback with the one provided by the caller */
	sep->ep.cb = cb;

	zerr = __sock_send(sep, SOCK_MSG_ACCEPTED, data, data_len);
	if (zerr)
		goto err_1;
	sep->ep.state = ZAP_EP_CONNECTED;
	pthread_mutex_unlock(&sep->ep.lock);

	struct zap_event ev = {
		.type = ZAP_EVENT_CONNECTED,
		.status = ZAP_ERR_OK,
	};
	zap_get_ep(&sep->ep); /* Release when receive disconnect/error event. */
	sep->ep.cb(&sep->ep, &ev);
	return ZAP_ERR_OK;

err_1:
	sep->ep.state = ZAP_EP_ERROR;
	shutdown(sep->sock, SHUT_RDWR);
err_0:
	pthread_mutex_unlock(&sep->ep.lock);
	return zerr;
}

static zap_err_t z_sock_reject(zap_ep_t ep, char *data, size_t data_len)
{
	struct z_sock_ep *sep = (struct z_sock_ep *)ep;
	zap_err_t zerr;

	pthread_mutex_lock(&sep->ep.lock);
	zerr = __sock_send(sep, SOCK_MSG_REJECTED, data, data_len);
	if (zerr)
		goto err;
	pthread_mutex_unlock(&sep->ep.lock);
	return ZAP_ERR_OK;
err:
	sep->ep.state = ZAP_EP_ERROR;
	shutdown(sep->sock, SHUT_RDWR);
	pthread_mutex_unlock(&sep->ep.lock);
	return zerr;
}

static zap_err_t
z_sock_map(zap_ep_t ep, zap_map_t *pm, void *buf, size_t len, zap_access_t acc)
{
	struct zap_sock_map *map = calloc(1, sizeof(*map));
	zap_err_t zerr = ZAP_ERR_OK;
	if (!map) {
		zerr = ZAP_ERR_RESOURCE;
		goto err0;
	}
	/* Just point *pm to map and do nothing. zap_map in zap.c will fill
	 * in map->map (base) details */
	struct z_sock_key *k = z_key_alloc(map);
	if (!k) {
		zerr = ZAP_ERR_RESOURCE;
		goto err1;
	}
	map->key = (uint32_t)(uint64_t)k->rb_node.key;
	*pm = (void*)map;
	goto out;
err1:
	free(map);
err0:
out:
	return zerr;
}

static zap_err_t z_sock_unmap(zap_ep_t ep, zap_map_t map)
{
	/* Just free the map */
	struct zap_sock_map *m = (void*) map;
	if (map->type == ZAP_MAP_LOCAL) {
		z_key_delete(m->key);
	}
	free(m);
	return ZAP_ERR_OK;
}

static zap_err_t z_sock_share(zap_ep_t ep, zap_map_t map,
				const char *msg, size_t msg_len)
{

	/* validate */
	if (ep->state != ZAP_EP_CONNECTED)
		return ZAP_ERR_NOT_CONNECTED;

	if (map->type != ZAP_MAP_LOCAL)
		return ZAP_ERR_INVALID_MAP_TYPE;

	/* prepare message */
	struct zap_sock_map *smap = (void*)map;
	size_t sz = sizeof(struct sock_msg_rendezvous) + msg_len;
	struct sock_msg_rendezvous *msgr = malloc(sz);
	if (!msgr)
		return ZAP_ERR_RESOURCE;

	msgr->hdr.msg_type = htons(SOCK_MSG_RENDEZVOUS);
	msgr->hdr.msg_len = htonl(sz);
	msgr->rmap_key = smap->key;
	msgr->acc = htonl(map->acc);
	msgr->addr = htobe64((uint64_t)map->addr);
	msgr->data_len = htonl(map->len);
	if (msg_len)
		memcpy(msgr->msg, msg, msg_len);

	zap_err_t rc = ZAP_ERR_OK;

	/* write message */
	struct z_sock_ep *sep = (void*) ep;
	if (bufferevent_write(sep->buf_event, msgr, sz) != 0)
		rc = ZAP_ERR_RESOURCE;

	free(msgr);
	return rc;
}

static zap_err_t z_sock_read(zap_ep_t ep, zap_map_t src_map, char *src,
			     zap_map_t dst_map, char *dst, size_t sz,
			     void *context)
{
	struct z_sock_ep *sep = (struct z_sock_ep *)ep;
	struct z_sock_io *io = __sock_io_alloc(sep);
	zap_err_t zerr = ZAP_ERR_OK;

	if (!io)
		return ZAP_ERR_RESOURCE;

	/* validate */
	if (z_map_access_validate(src_map, src, sz, ZAP_ACCESS_READ) != 0) {
		zerr = ZAP_ERR_REMOTE_PERMISSION;
		goto err;
	}

	if (z_map_access_validate(dst_map, dst, sz, ZAP_ACCESS_NONE) != 0) {
		zerr = ZAP_ERR_LOCAL_LEN;
		goto err;
	}

	/* prepare message */
	z_sock_hdr_init(&io->read.hdr, 0, SOCK_MSG_READ_REQ,
		   sizeof(io->read), (uint64_t)context);
	struct zap_sock_map *src_smap = (void*) src_map;
	io->read.src_map_key = src_smap->key;
	io->read.src_ptr = htobe64((uint64_t) src);
	io->read.data_len = htonl((uint32_t)sz);
	io->dst_map = dst_map;
	io->dst_ptr = dst;

	pthread_mutex_lock(&sep->ep.lock);
	if (sep->ep.state != ZAP_EP_CONNECTED) {
		zerr = ZAP_ERR_NOT_CONNECTED;
		pthread_mutex_unlock(&sep->ep.lock);
		goto err;
	}

	/* write message */
	zerr = ZAP_ERR_RESOURCE;
	if (bufferevent_write(sep->buf_event, &io->read, sizeof(io->read)) != 0)
		goto err1;

	TAILQ_INSERT_TAIL(&sep->io_q, io, q_link);
	pthread_mutex_unlock(&sep->ep.lock);
	zerr = ZAP_ERR_OK;
	return zerr;
err1:
	pthread_mutex_unlock(&sep->ep.lock);
err:
	__sock_io_free(sep, io);
	return zerr;
}

static zap_err_t z_sock_write(zap_ep_t ep, zap_map_t src_map, char *src,
			      zap_map_t dst_map, char *dst, size_t sz,
			      void *context)
{
	struct z_sock_ep *sep = (struct z_sock_ep *)ep;
	struct z_sock_io *io = __sock_io_alloc(sep);
	zap_err_t zerr;

	if (!io)
		return ZAP_ERR_RESOURCE;

	/* validate */
	if (z_map_access_validate(src_map, src, sz, ZAP_ACCESS_NONE) != 0) {
		zerr = ZAP_ERR_LOCAL_LEN;
		goto err0;
	}

	if (z_map_access_validate(dst_map, dst, sz, ZAP_ACCESS_WRITE) != 0) {
		zerr = ZAP_ERR_REMOTE_PERMISSION;
		goto err0;
	}

	/* prepare message */
	zerr = ZAP_ERR_RESOURCE;
	struct evbuffer *ebuf = evbuffer_new();
	if (!ebuf)
		goto err0;
	z_sock_hdr_init(&io->write.hdr, 0, SOCK_MSG_WRITE_REQ,
		   sizeof(io->write) + sz, (uint64_t)context);
	struct zap_sock_map *sdst_map = (void*)dst_map;
	io->write.dst_map_key = sdst_map->key;
	io->write.dst_ptr = htobe64((uint64_t) dst);
	io->write.data_len = htonl((uint32_t) sz);

	if (evbuffer_add(ebuf, &io->write, sizeof(io->write)) != 0)
		goto err1;
	if (evbuffer_add(ebuf, src, sz) != 0)
		goto err1;

	pthread_mutex_lock(&sep->ep.lock);
	if (sep->ep.state != ZAP_EP_CONNECTED) {
		zerr = ZAP_ERR_NOT_CONNECTED;
		goto err2;
	}

	/* write message */
	if (bufferevent_write_buffer(sep->buf_event, ebuf) != 0) {
		zerr = ZAP_ERR_TRANSPORT;
		goto err2;
	}

	TAILQ_INSERT_TAIL(&sep->io_q, io, q_link);
	pthread_mutex_unlock(&sep->ep.lock);
	evbuffer_free(ebuf);
	return ZAP_ERR_OK;

err2:
	pthread_mutex_unlock(&sep->ep.lock);
err1:
	evbuffer_free(ebuf);
err0:
	__sock_io_free(sep, io);
	return zerr;
}

zap_err_t zap_transport_get(zap_t *pz, zap_log_fn_t log_fn,
			    zap_mem_info_fn_t mem_info_fn)
{
	zap_t z;

	if (!init_complete && init_once())
		goto err;

	z = calloc(1, sizeof (*z));
	if (!z)
		goto err;

	/* max_msg is unused (since RDMA) ... */
	z->max_msg = (1024 * 1024) - sizeof(struct sock_msg_hdr);
	z->new = z_sock_new;
	z->destroy = z_sock_destroy;
	z->connect = z_sock_connect;
	z->accept = z_sock_accept;
	z->reject = z_sock_reject;
	z->listen = z_sock_listen;
	z->close = z_sock_close;
	z->send = z_sock_send;
	z->read = z_sock_read;
	z->write = z_sock_write;
	z->map = z_sock_map;
	z->unmap = z_sock_unmap;
	z->share = z_sock_share;
	z->get_name = z_get_name;

	/* is it needed? */
	z->mem_info_fn = mem_info_fn;

	*pz = z;
	return ZAP_ERR_OK;

 err:
	return ZAP_ERR_RESOURCE;
}
