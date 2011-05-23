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

#include "vtx.h"


//  ---------------------------------------------------------------------
//  Structure of our class
//  Not threadsafe, do not access from multiple threads

struct _vtx_t {
    zctx_t *ctx;        //  Our czmq context
    zhash_t *sockets;   //  0MQ sockets
    zhash_t *drivers;   //  Registered drivers
};

//  This structure instantiates a single driver
typedef struct {
    char *protocol;
    void *pipe;
} driver_t;

//  Destroy driver object, when driver is removed from vtx->drivers
static void
s_driver_destroy (void *argument)
{
    driver_t *driver = (driver_t *) argument;
    free (driver->protocol);
    free (driver);
}


//  ---------------------------------------------------------------------
//  Constructor

vtx_t *
vtx_new (zctx_t *ctx)
{
    vtx_t
        *self;

    self = (vtx_t *) zmalloc (sizeof (vtx_t));
    self->ctx = ctx;
    self->sockets = zhash_new ();
    self->drivers = zhash_new ();
    return self;
}


//  ---------------------------------------------------------------------
//  Destructor

void
vtx_destroy (vtx_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        vtx_t *self = *self_p;
        zhash_destroy (&self->sockets);
        zhash_destroy (&self->drivers);
        free (self);
        *self_p = NULL;
    }
}


//  ---------------------------------------------------------------------
//  Register a transport driver
//  Creates a driver thread and registers it in the driver hash table

int
vtx_register (vtx_t *self, char *protocol, zthread_attached_fn *driver_fn)
{
    assert (self);
    assert (protocol);
    assert (driver_fn);

    //  Driver protocol cannot already exist
    int rc = 0;
    driver_t *driver = (driver_t *) zhash_lookup (self->drivers, protocol);
    if (driver == NULL) {
        driver = (driver_t *) zmalloc (sizeof (driver_t));
        driver->protocol = strdup (protocol);
        driver->pipe = zthread_fork (self->ctx, driver_fn, NULL);
        zhash_insert (self->drivers, protocol, driver);
        zhash_freefn (self->drivers, protocol, s_driver_destroy);
    }
    else {
        rc = -1;
        errno = ENOTUNIQ;
    }
    return rc;
}


//  ---------------------------------------------------------------------
//  Create a new socket

void *
vtx_socket (vtx_t *self, int type)
{
    //  Create frontend socket for caller and bind it
    assert (self);
    void *frontend = zsocket_new (self->ctx, ZMQ_PAIR);
    assert (frontend);
    zsocket_bind (frontend, "inproc://vtx-%p", frontend);
    return frontend;
}


//  ---------------------------------------------------------------------
//  Close a socket

int
vtx_close (vtx_t *self, void *socket)
{
    // TODO:
    return 0;
}


//  Do a bind/connect call to a driver
static int
s_driver_call (vtx_t *self, void *socket, char *endpoint, char *command)
{
    assert (self);
    assert (socket);
    assert (endpoint);

    int rc = 0;
    char *protocol = strdup (endpoint);
    char *address = strstr (protocol, "://");
    if (address) {
        //  Split protocol from address
        *address = 0;
        address += 3;

        driver_t *driver = (driver_t *) zhash_lookup (self->drivers, protocol);
        if (driver) {
            zstr_sendf (driver->pipe, "%s:vtx-%p:%s", command, socket, address);
            char *reply = zstr_recv (driver->pipe);
            rc = atoi (reply);
            free (reply);
        }
        else {
            errno = ENOPROTOOPT;
            rc = -1;
        }
    }
    else {
        errno = EINVAL;
        rc = -1;
    }
    free (protocol);
    return rc;
}


//  ---------------------------------------------------------------------
//  Bind socket; lookup driver and send it BIND request, wait for
//  reply and provide to caller. Endpoint syntax is protocol://address.

int
vtx_bind (vtx_t *self, void *socket, char *endpoint)
{
    return s_driver_call (self, socket, endpoint, "BIND");
}


//  ---------------------------------------------------------------------
//  Connect socket; lookup driver and send it CONNECT request, wait for
//  reply and provide to caller. Endpoint syntax is protocol://address.

int
vtx_connect (vtx_t *self, void *socket, char *endpoint)
{
    return s_driver_call (self, socket, endpoint, "CONNECT");
}
