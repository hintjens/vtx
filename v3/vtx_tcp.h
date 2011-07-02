/*  =====================================================================
    VTX - 0MQ virtual transport interface - ZMTP / TCP driver

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

#ifndef __VTX_TCP_INCLUDED__
#define __VTX_TCP_INCLUDED__

#include "vtx.h"

//  Configurable defaults
//  Scheme we use for this protocol driver
#define VTX_TCP_SCHEME         "tcp"
//  Listen backlog
#define VTX_TCP_BACKLOG         100     //  Waiting connections
//  Input buffer size
#define VTX_TCP_BUFSIZE         1024
//  Time between connection retries
#define VTX_TCP_RECONNECT_IVL   1000    //  Msecs
#define VTX_TCP_RECONNECT_MAX   1000    //  Msecs, limit
//  Codec buffer sizes
#define VTX_TCP_INBUF_MAX       1024    //  Messages
#define VTX_TCP_OUTBUF_MAX      1024    //  Messages

#ifdef __cplusplus
extern "C" {
#endif

int vtx_tcp_load (vtx_t *vtx);

#ifdef __cplusplus
}
#endif

#endif
