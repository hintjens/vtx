/*  =====================================================================
    VTX - 0MQ virtual transport interface - UDP driver

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

#ifndef __VTX_UDP_INCLUDED__
#define __VTX_UDP_INCLUDED__

#include "vtx.h"

//  Scheme we use for this protocol driver
#define VTX_UDP_SCHEME         "udp"

//  Maximum size of a message we'll send over UDP
#define VTX_UDP_MSGMAX          512

//  Configurable defaults
//  Time we allow a peering to be silent before we kill it
#define VTX_UDP_TIMEOUT         10000   // msecs
//  Time between OHAI retries
#define VTX_UDP_OHAI_IVL        1000    // msecs
//  Time between NOM request retry attempts
#define VTX_UDP_RESEND_IVL      500     // msecs

//  ID and version number for our UDP protocol
#define VTX_UDP_VERSION         0x01

//  ZDTP wire-level protocol commands
#define VTX_UDP_ROTFL           0x00
#define VTX_UDP_OHAI            0x01
#define VTX_UDP_OHAI_OK         0x02
#define VTX_UDP_HUGZ            0x03
#define VTX_UDP_HUGZ_OK         0x04
#define VTX_UDP_NOM             0x05
#define VTX_UDP_CMDLIMIT        0x06

//  Size of VTX_UDP header in bytes
#define VTX_UDP_HEADER          2

#ifdef __cplusplus
extern "C" {
#endif

int vtx_udp_load (vtx_t *vtx);

#ifdef __cplusplus
}
#endif

#endif
