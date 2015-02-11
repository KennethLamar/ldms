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
#include <sys/errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <sys/queue.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <pthread.h>
#include <dlfcn.h>
#include <assert.h>
#include <time.h>
#include <limits.h>
#include <fcntl.h>
#include "zap.h"
#include "zap_priv.h"

#ifdef DEBUG
#define DLOG(ep, fmt, ...) do { \
	if (ep && ep->z && ep->z->log_fn) \
		ep->z->log_fn(fmt, ##__VA_ARGS__); \
} while(0)
#else
#define DLOG(ep, fmt, ...)
#endif

static void default_log(const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	vfprintf(stdout, fmt, ap);
	fflush(stdout);
}

#if 0
#define TF() default_log("%s:%d\n", __FUNCTION__, __LINE__)
#else
#define TF()
#endif

LIST_HEAD(zap_list, zap) zap_list;

pthread_mutex_t zap_list_lock;

#define ZAP_LIBPATH_DEFAULT "/usr/local/lib"
#define _SO_EXT ".so"
static char _libdir[PATH_MAX];

void *zap_get_ucontext(zap_ep_t ep)
{
	return ep->ucontext;
}

void zap_set_ucontext(zap_ep_t ep, void *context)
{
	ep->ucontext = context;
}

zap_err_t zap_get(const char *name, zap_t *pz, zap_log_fn_t log_fn,
		  zap_mem_info_fn_t mem_info_fn)
{
	zap_err_t ret = 0;
	char *libdir;
	zap_t z = NULL;
	char *errstr;
	int len;

	if (!log_fn)
		log_fn = default_log;

	libdir = getenv("ZAP_LIBPATH");
	if (!libdir || libdir[0] == '\0')
		strcpy(_libdir, ZAP_LIBPATH_DEFAULT);
	else
		strcpy(_libdir, libdir);

	/* Add a trailing / if one is not present in the path */
	len = strlen(_libdir);
	if (_libdir[len-1] != '/')
		strcat(_libdir, "/");

	strcat(_libdir, "libzap_");
	strcat(_libdir, name);
	strcat(_libdir, _SO_EXT);
	void *d = dlopen(_libdir, RTLD_NOW);
	if (!d) {
		/* The library doesn't exist */
		log_fn("dlopen: %s\n", dlerror());
		ret = ZAP_ERR_TRANSPORT;
		goto err;
	}
	dlerror();
	zap_get_fn_t get = dlsym(d, "zap_transport_get");
	errstr = dlerror();
	if (errstr || !get) {
		log_fn("dlsym: %s\n", errstr);
		/* The library exists but doesn't export the correct
		 * symbol and is therefore likely the wrong library type */
		ret = ZAP_ERR_TRANSPORT;
		goto err;
	}
	ret = get(pz, log_fn, mem_info_fn);
	if (ret)
		goto err;
	z = *pz;
	strcpy(z->name, name);
	z->log_fn = log_fn;
	z->mem_info_fn = mem_info_fn;

	pthread_mutex_lock(&zap_list_lock);
	LIST_INSERT_HEAD(&zap_list, z, zap_link);
	pthread_mutex_unlock(&zap_list_lock);

	return ZAP_ERR_OK;
 err:
	if (z)
		free(z);
	return ret;
}

size_t zap_max_msg(zap_t z)
{
	return z->max_msg;
}

zap_err_t zap_new(zap_t z, zap_ep_t *pep, zap_cb_fn_t cb)
{
	zap_err_t zerr = z->new(z, pep, cb);
	if (!zerr) {
		(*pep)->z = z;
		(*pep)->cb = cb;
		(*pep)->ref_count = 1;
		(*pep)->state = ZAP_EP_INIT;
		pthread_mutex_init(&(*pep)->lock, NULL);
	}
	return zerr;
}

zap_err_t zap_accept(zap_ep_t ep, zap_cb_fn_t cb)
{
	return ep->z->accept(ep, cb);
}

zap_err_t zap_connect(zap_ep_t ep, struct sockaddr *sa, socklen_t sa_len)
{
	return ep->z->connect(ep, sa, sa_len);
}

zap_err_t zap_listen(zap_ep_t ep, struct sockaddr *sa, socklen_t sa_len)
{
	return ep->z->listen(ep, sa, sa_len);
}

zap_err_t zap_close(zap_ep_t ep)
{
	return ep->z->close(ep);
}

zap_err_t zap_send(zap_ep_t ep, void *buf, size_t sz)
{
	return ep->z->send(ep, buf, sz);
}

zap_err_t zap_write(zap_ep_t ep,
		    zap_map_t src_map, void *src,
		    zap_map_t dst_map, void *dst,
		    size_t sz,
		    void *context)
{
	return ep->z->write(ep, src_map, src, dst_map, dst, sz, context);
}

zap_err_t zap_get_name(zap_ep_t ep, struct sockaddr *local_sa,
		       struct sockaddr *remote_sa, socklen_t *sa_len)
{
	return ep->z->get_name(ep, local_sa, remote_sa, sa_len);
}

void zap_get_ep(zap_ep_t ep)
{
	pthread_mutex_lock(&ep->lock);
	ep->ref_count++;
	pthread_mutex_unlock(&ep->lock);
}

zap_err_t zap_free(zap_ep_t ep)
{
	DLOG(ep, "zap_free: freeing 0x%016x\n", ep);
	/* Unmap the zap_map first */
	zap_map_t map;
	pthread_mutex_lock(&ep->lock);
	while ((map = LIST_FIRST(&ep->map_list))) {
		LIST_REMOVE(map, link);
		ep->z->unmap(ep, map);
	}
	pthread_mutex_unlock(&ep->lock);

	/* Then, destroy the endpoint */
	ep->z->destroy(ep);
}

void zap_put_ep(zap_ep_t ep)
{
	int destroy = 0;

	pthread_mutex_lock(&ep->lock);
	assert(ep->ref_count);
	ep->ref_count--;
	if (!ep->ref_count)
		destroy = 1;
	pthread_mutex_unlock(&ep->lock);

	if (destroy)
		zap_free(ep);
}

zap_err_t zap_read(zap_ep_t ep,
		   zap_map_t src_map, void *src,
		   zap_map_t dst_map, void *dst,
		   size_t sz,
		   void *context)
{
	if (dst_map->type != ZAP_MAP_LOCAL)
		return ZAP_ERR_INVALID_MAP_TYPE;
	if (src_map->type != ZAP_MAP_REMOTE)
		return ZAP_ERR_INVALID_MAP_TYPE;

	return ep->z->read(ep, src_map, src, dst_map, dst, sz, context);
}


size_t zap_map_len(zap_map_t map)
{
	return map->len;
}

void* zap_map_addr(zap_map_t map)
{
	return map->addr;
}

zap_err_t zap_map(zap_ep_t ep, zap_map_t *pm,
		  void *addr, size_t len, zap_access_t acc)
{
	zap_map_t map;
	zap_err_t err = ep->z->map(ep, pm, addr, len, acc);
	if (err)
		goto out;

	map = *pm;
	map->type = ZAP_MAP_LOCAL;
	map->ep = ep;
	map->addr = addr;
	map->len = len;
	map->acc = acc;
	pthread_mutex_lock(&ep->lock);
	LIST_INSERT_HEAD(&ep->map_list, map, link);
	pthread_mutex_unlock(&ep->lock);
 out:
	return err;
}

enum zap_err_e errno2zaperr(int e)
{
	switch (e) {
	case ENOTCONN:
		return ZAP_ERR_NOT_CONNECTED;
	case ENOMEM:
	case ENOBUFS:
		return ZAP_ERR_RESOURCE;
	case ECONNREFUSED:
		return ZAP_ERR_CONNECT;
	case EISCONN:
		return ZAP_ERR_BUSY;
	case EFAULT:
	case EINVAL:
		return ZAP_ERR_PARAMETER;
	default:
		return ZAP_ERR_ENDPOINT;
	}
}

zap_err_t zap_unmap(zap_ep_t ep, zap_map_t map)
{
	pthread_mutex_lock(&ep->lock);
	LIST_REMOVE(map, link);
	pthread_mutex_unlock(&ep->lock);

	return ep->z->unmap(ep, map);
}

zap_err_t zap_share(zap_ep_t ep, zap_map_t map, uint64_t ctxt)
{
	return ep->z->share(ep, map, ctxt);
}

zap_err_t zap_reject(zap_ep_t ep)
{
	return ep->z->reject(ep);
}

void __attribute__ ((constructor)) cs_init(void)
{
	pthread_mutex_init(&zap_list_lock, 0);
}

void __attribute__ ((destructor)) cs_term(void)
{
}
