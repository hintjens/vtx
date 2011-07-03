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
    zctx_t *ctx;            //  Our CZMQ context
    zhash_t *drivers;       //  Registered drivers
    zhash_t *sockets;       //  All active sockets
};

//  This structure instantiates a single VTX driver
typedef struct {
    char *protocol;         //  Registered protocol name
    void *commands;         //  Command pipe to driver
} vtx_driver_t;

//  This structure instantiates a single VTX socket
typedef struct {
    void *socket;           //  0MQ socket object
    int type;               //  Desired socket type
    vtx_driver_t *driver;   //  VTX driver, if known
    char *address;          //  Bind/connect address
} vtx_socket_t;

//  Driver & socket manipulation
static vtx_driver_t *
    s_driver_new (vtx_t *vtx, char *protocol,
                  zthread_attached_fn *driver_fn, Bool verbose);
static void
    s_driver_destroy (void *argument);
static vtx_socket_t *
    s_socket_new (vtx_t *vtx, void *socket, int type, char *socket_key);
static void
    s_socket_destroy (void *argument);
static char *
    s_socket_key (void *self);


//  ---------------------------------------------------------------------
//  Constructor

vtx_t *
vtx_new (zctx_t *ctx)
{
    vtx_t
        *self;

    self = (vtx_t *) zmalloc (sizeof (vtx_t));
    self->ctx = ctx;
    self->drivers = zhash_new ();
    self->sockets = zhash_new ();
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
        zhash_destroy (&self->drivers);
        zhash_destroy (&self->sockets);
        free (self);
        *self_p = NULL;
    }
}


//  ---------------------------------------------------------------------
//  Register a transport driver
//  Creates a driver thread and registers it in the driver hash table

int
vtx_register (vtx_t *self, char *scheme, zthread_attached_fn *driver_fn, Bool verbose)
{
    assert (self);
    assert (scheme);
    assert (driver_fn);

    //  Driver scheme cannot already exist
    int rc = 0;
    vtx_driver_t *driver = (vtx_driver_t *) zhash_lookup (self->drivers, scheme);
    if (!driver)
        driver = s_driver_new (self, scheme, driver_fn, verbose);
    else {
        rc = -1;
        errno = ENOTUNIQ;
    }
    return rc;
}


//  ---------------------------------------------------------------------
//  Create a new socket. At this stage we're not yet talking to a driver,
//  so we bind the socket to our VTX endpoint and store the emulated
//  socket type so we can give that to a driver when we connect/bind.

void *
vtx_socket (vtx_t *self, int type)
{
    //  Create socket frontend for caller and bind it
    assert (self);

    void *socket = zsocket_new (self->ctx, ZMQ_DEALER);
    //  Socket may be null if we're shutting down 0MQ
    if (socket) {
        //  Bind socket to our side of pipe
        char *socket_key = s_socket_key (socket);
        zsocket_bind (socket, "inproc://%s", socket_key);
        //  Create vtx_socket to hold the emulated type
        s_socket_new (self, socket, type, socket_key);
        free (socket_key);
    }
    return socket;
}


//  Send a command to a driver. We send four-frame request to
//  driver command pipe:
//
//  [command]   BIND, CONNECT, or CLOSE
//  [socktype]  0MQ type for the socket, as string
//  [vtxname]   VTX name for the socket
//  [address]   Address for command
//
//  Reply is one frame with numeric status code, 0 = OK

static int
s_driver_call (vtx_t *self, void *socket, char *command, char *endpoint)
{
    char *socket_key = s_socket_key (socket);
    vtx_socket_t *vtx_socket
        = (vtx_socket_t *) zhash_lookup (self->sockets, socket_key);

    //  VTX socket must exist
    if (!vtx_socket) {
        errno = EINVAL;
        return -1;
    }
    //  Resolve endpoint if provided
    char *protocol = NULL;
    char *address = "";
    if (endpoint) {
        //  Take a copy because we modify this string
        protocol = strdup (endpoint);
        char *scheme_end = strstr (protocol, "://");
        if (!scheme_end) {
            errno = EINVAL;
            return -1;
        }
        //  Split address from protocol
        *scheme_end = 0;
        address = scheme_end + 3;

        //  Look up driver by protocol
        vtx_driver_t *driver =
            (vtx_driver_t *) zhash_lookup (self->drivers, protocol);
        if (!driver) {
            free (protocol);
            errno = ENOPROTOOPT;
            return -1;
        }
        //  Don't allow multiple drivers per socket
        if (vtx_socket->driver && vtx_socket->driver != driver) {
            errno = ENOTSUP;
            return -1;
        }
        vtx_socket->driver = driver;
    }
    zmsg_t *request = zmsg_new ();
    zmsg_addstr (request, command);
    zmsg_addstr (request, "%d", vtx_socket->type);
    zmsg_addstr (request, "%s", socket_key);
    zmsg_addstr (request, address);
    zmsg_send (&request, vtx_socket->driver->commands);
    free (socket_key);

    char *reply = zstr_recv (vtx_socket->driver->commands);
    int rc = 0;
    if (reply) {
        rc = atoi (reply);
        free (reply);
    }
    free (protocol);
    return rc;
}


//  ---------------------------------------------------------------------
//  Bind socket; lookup driver and send it BIND request, wait for
//  reply and provide to caller. Endpoint syntax is protocol://address.

int
vtx_bind (vtx_t *self, void *socket, const char *format, ...)
{
    assert (self);
    assert (socket);
    assert (format);

    char endpoint [256];
    va_list argptr;
    va_start (argptr, format);
    vsnprintf (endpoint, 256, format, argptr);
    va_end (argptr);

    return s_driver_call (self, socket, "BIND", endpoint);
}


//  ---------------------------------------------------------------------
//  Connect socket; lookup driver and send it CONNECT request, wait for
//  reply and provide to caller. Endpoint syntax is protocol://address.

int
vtx_connect (vtx_t *self, void *socket, const char *format, ...)
{
    assert (self);
    assert (socket);
    assert (format);

    char endpoint [256];
    va_list argptr;
    va_start (argptr, format);
    vsnprintf (endpoint, 256, format, argptr);
    va_end (argptr);

    return s_driver_call (self, socket, "CONNECT", endpoint);
}


//  ---------------------------------------------------------------------
//  Get meta data about a socket, returns string that the caller should
//  free when finished with.

char *
vtx_getmeta (vtx_t *self, void *socket, const char *metaname)
{
    char *socket_key = s_socket_key (socket);
    vtx_socket_t *vtx_socket = (vtx_socket_t *)
        zhash_lookup (self->sockets, socket_key);

    assert (vtx_socket);
    assert (vtx_socket->driver);

    zmsg_t *request = zmsg_new ();
    zmsg_addstr (request, "GETMETA");
    zmsg_addstr (request, "0");
    zmsg_addstr (request, "%s", socket_key);
    zmsg_addstr (request, metaname);
    zmsg_send (&request, vtx_socket->driver->commands);
    free (socket_key);

    char *reply = zstr_recv (vtx_socket->driver->commands);
    return reply;
}


//  ---------------------------------------------------------------------
//  Close a socket

int
vtx_close (vtx_t *self, void *socket)
{
    assert (self);
    assert (socket);
    return s_driver_call (self, socket, "CLOSE", NULL);
    zsocket_destroy (self->ctx, socket);
}


//  ---------------------------------------------------------------------
//  Driver & socket manipulation

static vtx_driver_t *
s_driver_new (vtx_t *vtx, char *protocol, zthread_attached_fn *driver_fn, Bool verbose)
{
    vtx_driver_t *self = (vtx_driver_t *) zmalloc (sizeof (vtx_driver_t));
    self->protocol = strdup (protocol);
    self->commands = zthread_fork (vtx->ctx, driver_fn, (void *) verbose);
    zhash_insert (vtx->drivers, protocol, self);
    zhash_freefn (vtx->drivers, protocol, s_driver_destroy);
    return self;
}

//  Destroy driver object, when driver is removed from vtx->drivers
static void
s_driver_destroy (void *argument)
{
    vtx_driver_t *self = (vtx_driver_t *) argument;
    free (self->protocol);
    free (self);
}

//  Socket methods

static vtx_socket_t *
s_socket_new (vtx_t *vtx, void *socket, int type, char *socket_key)
{
    vtx_socket_t *self = (vtx_socket_t *) zmalloc (sizeof (vtx_socket_t));
    self->socket = socket;
    self->type = type;
    zhash_insert (vtx->sockets, socket_key, self);
    zhash_freefn (vtx->sockets, socket_key, s_socket_destroy);
    return self;
}

//  Destroy socket object, when socket is removed from vtx->sockets
static void
s_socket_destroy (void *argument)
{
    vtx_socket_t *self = (vtx_socket_t *) argument;
    free (self);
}

//  Return formatted socket key
static char *
s_socket_key (void *self)
{
    char socket_key [30];
    sprintf (socket_key, "vtx-%p", self);
    return strdup (socket_key);
}
