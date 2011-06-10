/*  =====================================================================
    VTX - 0MQ virtual transport interface

    ---------------------------------------------------------------------
    Copyright (c) 1991-2011 iMatix Corporation <www.imatix.com>
    Copyright other contributors as noted in the AUTHORS file.

    This file is part of VTX, the 0MQ virtual transport interface:
    http://vtx.zeromq.org.

    This is free software; you can redistribute it and/or modify it under
    the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation; either version 3 of the License, or (at
    your option) any later version.

    This software is distributed in the hope that it will be useful, but
    WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
    Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public
    License along with this program. If not, see
    <http://www.gnu.org/licenses/>.
    =====================================================================
*/

#ifndef __VTX_INCLUDED__
#define __VTX_INCLUDED__

#include "czmq.h"

//  Types of routing per driver socket
#define VTX_ROUTING_NONE     0      //  No output routing allowed
#define VTX_ROUTING_REPLY    1      //  Reply to specific link
#define VTX_ROUTING_ROTATE   2      //  Rotate to links in turn
#define VTX_ROUTING_CCEACH   3      //  Carbon-copy to each link

//  Types of flow control per driver socket
#define VTX_FLOW_ASYNC       0      //  Async message tranefers
#define VTX_FLOW_SYNREQ      1      //  Synchronous requests
#define VTX_FLOW_SYNREP      2      //  Synchronous replies

#ifdef __cplusplus
extern "C" {
#endif

//  Opaque class structure
typedef struct _vtx_t vtx_t;

//  Application program interface (API)
vtx_t *
    vtx_new (zctx_t *ctx);
void
    vtx_destroy (vtx_t **self_p);
void *
    vtx_socket (vtx_t *self, int type);
int
    vtx_close (vtx_t *self, void *socket);
int
    vtx_bind (vtx_t *self, void *socket, char *endpoint);
int
    vtx_connect (vtx_t *self, void *socket, char *endpoint);
int
    vtx_register (vtx_t *self, char *protocol, zthread_attached_fn *driver_fn);

#ifdef __cplusplus
}
#endif

#endif
