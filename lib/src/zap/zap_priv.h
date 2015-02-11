/* -*- c-basic-offset: 8 -*-
 * Copyright (c) 2013 Open Grid Computing, Inc. All rights reserved.
 * Copyright (c) 2013 Sandia Corporation. All rights reserved.
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

#ifndef __ZAP_PRIV_H__
#define __ZAP_PRIV_H__
#include <inttypes.h>
#include <sys/queue.h>

#include "config.h"

typedef enum zap_ep_state {
	ZAP_EP_INIT = 0,
	ZAP_EP_LISTENING,
	ZAP_EP_CONNECTING,
	ZAP_EP_CONNECTED,
	ZAP_EP_PEER_CLOSE,
	ZAP_EP_CLOSE,
	ZAP_EP_ERROR
} zap_ep_state_t;

struct zap_ep {
	zap_t z;
	int ref_count;
	pthread_mutex_t lock;
	zap_ep_state_t state;
	void *ucontext;

	LIST_HEAD(zap_map_list, zap_map) map_list;

	/** Event callback routine. */
	zap_cb_fn_t cb;
};

struct zap {
	char name[ZAP_MAX_TRANSPORT_NAME_LEN];
	int max_msg;		/* max send message size */

	LIST_ENTRY(zap) zap_link;

	/** Create a new endpoint */
	zap_err_t (*new)(zap_t z, zap_ep_t *pep, zap_cb_fn_t cb);

	/** Destroy an endpoint. */
	void (*destroy)(zap_ep_t ep);

	/** Request a connection with a server */
	zap_err_t (*connect)(zap_ep_t ep, struct sockaddr *sa, socklen_t sa_len);

	/** Listen for incoming connection requests */
	zap_err_t (*listen)(zap_ep_t ep, struct sockaddr *sa, socklen_t sa_len);

	/** Accept a connection request */
	zap_err_t (*accept)(zap_ep_t ep, zap_cb_fn_t cb);

	/** Reject a connection request */
	zap_err_t (*reject)(zap_ep_t ep);

	/** Close the connection */
	zap_err_t (*close)(zap_ep_t ep);

	/** Send a message */
	zap_err_t (*send)(zap_ep_t ep, void *buf, size_t sz);

	/** RDMA write data to a remote buffer */
	zap_err_t (*write)(zap_ep_t ep,
			   zap_map_t src_map, void *src,
			   zap_map_t dst_map, void *dst, size_t sz,
			   void *context);

	/**  RDMA read data from a remote buffer */
	zap_err_t (*read)(zap_ep_t ep,
			  zap_map_t src_map, void *src,
			  zap_map_t dst_map, void *dst, size_t sz,
			  void *context);

	/** Allocate a remote buffer */
	zap_err_t (*map)(zap_ep_t ep, zap_map_t *pm, void *addr, size_t len,
			 zap_access_t acc);

	/** Free a remote buffer */
	zap_err_t (*unmap)(zap_ep_t ep, zap_map_t map);

	/** Share a mapping with a remote peer */
	zap_err_t (*share)(zap_ep_t ep, zap_map_t m, uint64_t ctxt);


	/** Get the local and remote sockaddr for the endpoint */
	zap_err_t (*get_name)(zap_ep_t ep, struct sockaddr *local_sa,
			      struct sockaddr *remote_sa, socklen_t *sa_len);

	/** Transport message logging callback */
	zap_log_fn_t log_fn;

	/** Memory information callback */
	zap_mem_info_fn_t mem_info_fn;

	/** Pointer to the transport's private data */
	void *private;
};

static inline zap_err_t
zap_ep_change_state(struct zap_ep *ep,
		    zap_ep_state_t from_state,
		    zap_ep_state_t to_state)
{
	zap_err_t err = ZAP_ERR_OK;
	pthread_mutex_lock(&ep->lock);
	if (ep->state != from_state){
		err = ZAP_ERR_BUSY;
		goto out;
	}
	ep->state = to_state;
 out:
	pthread_mutex_unlock(&ep->lock);
	return err;
}

struct zap_map {
	LIST_ENTRY(zap_map) link; /*! List of maps for an endpoint. */
	zap_map_type_t type;	  /*! Is this a local or remote map  */
	zap_ep_t ep;		  /*! The endpoint */
	zap_access_t acc;	  /*! Access rights */
	void *addr;		  /*! Address of buffer. */
	size_t len;		  /*! Length of the buffer */
};

typedef zap_err_t (*zap_get_fn_t)(zap_t *pz, zap_log_fn_t log_fn,
				  zap_mem_info_fn_t map_info_fn);

#endif
