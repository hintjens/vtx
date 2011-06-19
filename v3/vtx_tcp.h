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

//  Scheme we use for this protocol driver
#define VTX_TCP_SCHEME         "tcp"

//  Maximum size of a message we'll send over TCP
#define VTX_TCP_MSGMAX          512

//  Configurable defaults
//  Time we allow a peering to be silent before we kill it
#define VTX_TCP_TIMEOUT         10000   // msecs
//  Time between OHAI retries
#define VTX_TCP_OHAI_IVL        1000    // msecs
//  Time between NOM request retry attempts
#define VTX_TCP_RESEND_IVL      500     // msecs

//  ID and version number for our TCP protocol
#define VTX_TCP_VERSION         0x01

//  ZDTP wire-level protocol commands
#define VTX_TCP_ROTFL           0x00
#define VTX_TCP_OHAI            0x01
#define VTX_TCP_OHAI_OK         0x02
#define VTX_TCP_HUGZ            0x03
#define VTX_TCP_HUGZ_OK         0x04
#define VTX_TCP_NOM             0x05
#define VTX_TCP_CMDLIMIT        0x06

//  Size of VTX_TCP header in bytes
#define VTX_TCP_HEADER          2

#ifdef __cplusplus
extern "C" {
#endif

int vtx_tcp_load (vtx_t *vtx);

#ifdef __cplusplus
}
#endif

#endif
