/*  =====================================================================
    zvudp - 0MQ virtual UDP transport driver

    ---------------------------------------------------------------------
    Copyright (c) 1991-2011 iMatix Corporation <www.imatix.com>
    Copyright other contributors as noted in the AUTHORS file.

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

#ifndef __ZVUDP_INCLUDED__
#define __ZVUDP_INCLUDED__

#include "czmq.h"

//  Maximum size of a message we'll send over UDP
#define ZVUDP_MSGMAX    512

#ifdef __cplusplus
extern "C" {
#endif

//  Opaque class structure
typedef struct _zvudp_t zvudp_t;

zvudp_t *
    zvudp_new (void);
void
    zvudp_destroy (zvudp_t **self_p);
void
    zvudp_bind (zvudp_t *self, char *interface, int port);
void
    zvudp_connect (zvudp_t *self, char *address, int port);
void *
    zvudp_socket (zvudp_t *self);

#ifdef __cplusplus
}
#endif

#endif
