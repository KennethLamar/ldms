/* -*- c-basic-offset: 8 -*-
 * Copyright (c) 2016 Open Grid Computing, Inc. All rights reserved.
 * Copyright (c) 2016 Sandia Corporation. All rights reserved.
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
#ifndef __OVIS_EVENT_PRIV_H
#define __OVIS_EVENT_PRIV_H
#include "ovis-lib-config.h"
#include "ovis_event.h"
#include "../coll/heap.h"
#include <pthread.h>
#include <stddef.h>

struct ovis_event {
	uint32_t flags;
	uint32_t epoll_events;
	int fd;
	ovis_event_cb cb;
	void *ctxt;
	struct timeval tv;
	struct timeval timer;
	int idx;
};

#define MAX_OVIS_EVENTS 128

struct ovis_event_heap {
	uint32_t alloc_len;
	uint32_t heap_len;
	struct ovis_event *ev[OVIS_FLEX];
};

struct ovis_event_manager {
	int evcount;
	int refcount;
	int efd; /* epoll fd */
	int pfd[2]; /* pipe for event notification */
	struct ovis_event ovis_ev;
	struct epoll_event ev[MAX_OVIS_EVENTS];
	pthread_mutex_t mutex;
	struct ovis_event_heap *heap;
	enum {
		OVIS_EVENT_MANAGER_INIT,
		OVIS_EVENT_MANAGER_RUNNING,
		OVIS_EVENT_MANAGER_WAITING,
		OVIS_EVENT_MANAGER_TERM,
	} state;
};

#endif
