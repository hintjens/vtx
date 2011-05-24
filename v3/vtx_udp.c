/*  =====================================================================
    VTX - 0MQ virtual transport interface - UDP driver

    Describe how the UDP driver works, esp. connect and bind.

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

#include "vtx_udp.h"

static void derp (char *s) { perror (s); exit (1); }

//  ---------------------------------------------------------------------
//  We have one instance of this thread per registered driver.

typedef struct {
    zctx_t *ctx;                //  Own context
    zloop_t *loop;              //  zloop reactor for socket i/o
    void *pipe;                 //  Control pipe to VTX frontend
    zlist_t *sockets;           //  List of sockets to manage
    zhash_t *links;             //  All links, indexed by UUID
    uint link_id;               //  Link identity number
} driver_t;

//  Driver manages a list of high-level socket objects

#define PUBLIC_MAX      10      //  Max. public sockets, i.e. binds

#define ROUTING_NONE     0      //  No output routing allowed
#define ROUTING_REPLY    1      //  Reply to specific link
#define ROUTING_BALANCE  2      //  Load-balance to links in turn
#define ROUTING_COPY     3      //  Carbon-copy to each link
#define ROUTING_DIRECT   4      //  Direct to single link

#define FLOW_ASYNC       0      //  Async message tranefers
#define FLOW_SYNREQ      1      //  Synchronous requests
#define FLOW_SYNREP      2      //  Synchronous replies

typedef struct {
    driver_t *driver;           //  Parent driver object
    void *backend;              //  Socket backend (data pipe)
    int routing;                //  Routing mechanism
    int max_links;              //  Maximum allowed links
    int flow_control;           //  Flow control mechanism
    uint publics;               //  Number of public sockets
    int public [PUBLIC_MAX];    //  Public bound sockets
    int private;                //  Private inout socket
    zlist_t *links;             //  List of links for socket
} socket_t;

//  Each socket has a list of active links to peers

typedef struct {
    socket_t *socket;           //  Parent socket object
    int64_t expiry;             //  Link expires at this time
    char id [10];               //  Unique ID for this link
    struct sockaddr_in addr;    //  Peer address
    int req_sequence;           //  Last request sequence
    byte *reply;                //  Last sent SYNREP if any
} link_t;

//  Create, destroy driver, socket, and link objects
static driver_t *
    driver_new (zctx_t *ctx, void *pipe);
static void
    driver_destroy (driver_t **self_p);
static socket_t *
    socket_new (driver_t *driver, int frontend_type, char *frontend_addr);
static void
    socket_destroy (socket_t **self_p);
static link_t *
    link_new (socket_t *socket, struct sockaddr_in *addr);
static void
    link_destroy (link_t **self_p);

//  Reactor handlers, we'll implement them later
static int
    s_driver_control (zloop_t *loop, zmq_pollitem_t *item, void *arg);
static int
    s_internal_input (zloop_t *loop, zmq_pollitem_t *item, void *arg);
static int
    s_external_input (zloop_t *loop, zmq_pollitem_t *item, void *arg);


//  ---------------------------------------------------------------------
//  Main driver thread is minimal, all work is done by reactor

void vtx_udp_driver (void *args, zctx_t *ctx, void *pipe)
{
    //  Create driver instance
    driver_t *driver = driver_new (ctx, pipe);
    //  Run reactor until we exit from failure or interrupt
    zloop_start (driver->loop);
    //  Destroy driver instance
    driver_destroy (&driver);
}


//  ---------------------------------------------------------------------
//  Constructor and destructor for driver

static driver_t *
driver_new (zctx_t *ctx, void *pipe)
{
    driver_t *self = (driver_t *) zmalloc (sizeof (driver_t));
    self->ctx = ctx;
    self->pipe = pipe;
    self->sockets = zlist_new ();
    self->links = zhash_new ();
    self->loop = zloop_new ();

    //  Reactor starts by monitoring the driver control pipe
    zloop_set_verbose (self->loop, TRUE);
    zmq_pollitem_t poll_item = { self->pipe, 0, ZMQ_POLLIN, 0 };
    zloop_poller (self->loop, &poll_item, s_driver_control, self);
    return self;
}

static void
driver_destroy (driver_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        driver_t *self = *self_p;
        while (zlist_size (self->sockets)) {
            socket_t *socket = (socket_t *) zlist_pop (self->sockets);
            socket_destroy (&socket);
        }
        zlist_destroy (&self->sockets);
        zhash_destroy (&self->links);
        zloop_destroy (&self->loop);
        free (self);
        *self_p = NULL;
    }
}

//  ---------------------------------------------------------------------
//  Constructor and destructor for socket

static socket_t *
socket_new (driver_t *driver, int frontend_type, char *frontend_addr)
{
    assert (driver);
    socket_t *self = (socket_t *) zmalloc (sizeof (socket_t));

    int backend_type;
    self->driver = driver;
    self->links = zlist_new ();
    self->flow_control = FLOW_ASYNC;
    switch (frontend_type) {
        case ZMQ_REQ:
            backend_type = ZMQ_DEALER;
            self->routing = ROUTING_BALANCE;
            self->flow_control = FLOW_SYNREQ;
            break;
        case ZMQ_REP:
            backend_type = ZMQ_DEALER;
            self->routing = ROUTING_REPLY;
            self->flow_control = FLOW_SYNREP;
            break;
        case ZMQ_DEALER:
            backend_type = ZMQ_DEALER;
            self->routing = ROUTING_REPLY;
            break;
        case ZMQ_PUB:
            backend_type = ZMQ_SUB;
            self->routing = ROUTING_COPY;
            break;
        case ZMQ_SUB:
            backend_type = ZMQ_PUB;
            self->routing = ROUTING_NONE;
            break;
        case ZMQ_PUSH:
            backend_type = ZMQ_PULL;
            self->routing = ROUTING_BALANCE;
            break;
        case ZMQ_PULL:
            backend_type = ZMQ_PUSH;
            self->routing = ROUTING_NONE;
            break;
        case ZMQ_PAIR:
            backend_type = ZMQ_PAIR;
            self->routing = ROUTING_DIRECT;
            self->max_links = 1;
            break;
        default:
            printf ("E: invalid socket type %d\n", frontend_type);
            exit (1);
    }
    //  Create backend socket and connect over inproc to frontend
    self->backend = zsocket_new (driver->ctx, backend_type);
    assert (self->backend);
    zsocket_connect (self->backend, "inproc://%s", frontend_addr);

    //  Ask reactor to start monitoring backend socket
    zmq_pollitem_t poll_item = { self->backend, 0, ZMQ_POLLIN, 0 };
    zloop_poller (driver->loop, &poll_item, s_internal_input, self);

    //  Store this socket in driver list of sockets so that driver
    //  can cleanly destroy all its sockets when it is destroyed.
    zlist_push (driver->sockets, self);
    return self;
}

static void
socket_destroy (socket_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        socket_t *self = *self_p;
        driver_t *driver = self->driver;

        //  Close any public handles we have open
        while (self->publics) {
            int fd = self->public [self->publics - 1];
            zloop_cancel (driver->loop, NULL, fd);
            close (fd);
            self->publics--;
        }
        //  Close private handle, if we have it open
        if (self->private) {
            zloop_cancel (driver->loop, NULL, self->private);
            close (self->private);
        }
        //  Destroy any active links for this socket
        while (zlist_size (self->links)) {
            link_t *link = (link_t *) zlist_pop (self->links);
            link_destroy (&link);
        }
        zlist_destroy (&self->links);
        free (self);
        *self_p = NULL;
    }
}

//  ---------------------------------------------------------------------
//  Constructor and destructor for link

static link_t *
link_new (socket_t *socket, struct sockaddr_in *addr)
{
    assert (socket);
    link_t *self = (link_t *) zmalloc (sizeof (link_t));

    driver_t *driver = socket->driver;
    self->socket = socket;
    self->expiry = zclock_time () + VTX_UDP_LINKTTL;
    snprintf (self->id, 10, "%ud", ++driver->link_id);
    memcpy (&self->addr, addr, sizeof (self->addr));

    zlist_push (socket->links, self);
    zhash_insert (driver->links, self->id, self);
    return self;
}

static void
link_destroy (link_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        link_t *self = *self_p;
        free (self->reply);
        free (self);
        *self_p = NULL;
    }
}


//  ---------------------------------------------------------------------
//  Reactor handlers

static int
s_driver_control (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    driver_t *self = (driver_t *) arg;
    char *command = zstr_recv (item->socket);
    puts (command);
    exit (1);
    return 0;
}

static int
s_internal_input (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    socket_t *self = (socket_t *) arg;
    return 0;
}

static int
s_external_input (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    socket_t *self = (socket_t *) arg;
    return 0;
}



#if 0
    //  Handle readers ad-infinitum
    while (!zctx_interrupted) {
        driver_poll ()
        if (rc == -1)
            break;              //  Context has been shut down

        //  Item 0 is always our pipe pipe
        if (items [0].revents & ZMQ_POLLIN) {
            char *command = zstr_recv (pipe);
            char *frontend = strchr (command, ':');
            assert (frontend);
            *frontend++ = 0;
            char *address = strchr (frontend, ':');
            assert (address);
            *address++ = 0;
            char *port = strchr (address, ':');
            if (port)
                *port++ = 0;

            //  Create inproc PAIR socket, for every command
            zlist_push (readers, s_reader_new (backend, NULL);

            //  Create UDP socket (for now, for every command)
            int handle = socket (AF_INET, SOCK_DGRAM, IPPROTO_UDP);
            if (handle == -1)
                derp ("socket");
            memset (&peer, 0, sizeof (peer));
            peer.sin_family = AF_INET;
            zlist_push (readers, s_reader_new (NULL, handle);


            //  Handle CONNECT command
            if (streq (command, "CONNECT")) {
                if (streq (address, "*")) {
                    //  Enable broadcast mode
                    int broadcast_on = 1;
                    if (setsockopt (handle, SOL_SOCKET, SO_BROADCAST,
                        &broadcast_on, sizeof (int)) == -1)
                        derp ("setsockopt (SO_BROADCAST)");

                    //  Set address to LAN broadcast
                    //  Will send only to first interface...
                    peer.sin_addr.s_addr = htonl (INADDR_BROADCAST);
                }
                else
                if (inet_aton (address, &peer.sin_addr) == 0)
                    derp ("inet_aton");

                peer.sin_port = htons (atoi (port));
                if (connect (handle, (const struct sockaddr *) &addr, sizeof (addr)) == -1)
                    derp ("connect");
            }
            else
            //  Handle BIND command
            if (streq (command, "BIND")) {
                struct sockaddr_in addr = { 0 };
                addr.sin_family = AF_INET;
                if (streq (address, "*"))
                    addr.sin_addr.s_addr = htonl (INADDR_ANY);
                else
                if (inet_aton (address, &addr.sin_addr) == 0)
                    derp ("inet_aton");

                addr.sin_port = htons (atoi (port));
                if (bind (handle, (const struct sockaddr *) &addr, sizeof (addr)) == -1)
                    derp ("bind");
            }
            else
                printf ("Invalid command: %s\n", command);

            zstr_send (pipe, "0");
            free (command);
        }

//  Process data message
static void
driver_data (driver_t *self)
{
    //  Handle only single-part messages for now
    zframe_t *frame = zframe_recv (data);
    assert (!zframe_more (frame));
    assert (inet_ntoa (peer.sin_addr));
    byte *data = zframe_data (frame);
    size_t size = zframe_size (frame);
    //  Discard over-long messages silently
    if (size <= ZVUDP_MSGMAX) {
        if (sendto (handle, data, size, 0,
            (const struct sockaddr *) &peer, sizeof (peer)) == -1)
            derp ("sendto");
    }
    zframe_destroy (&frame);
}


//  Process UDP socket input
static void
driver_handle (driver_t *self)
{
    char buffer [ZVUDP_MSGMAX];
    socklen_t addr_len = sizeof (struct sockaddr_in);
    ssize_t size = recvfrom (handle, buffer, ZVUDP_MSGMAX, 0,
                            (struct sockaddr *) &peer, &addr_len);
    if (size == -1)
        derp ("recvfrom");

//    printf ("Received from %s:%d\n",
//        inet_ntoa (peer.sin_addr), ntohs (peer.sin_port));
    zframe_t *frame = zframe_new (buffer, size);
    zframe_send (&frame, data, 0);
}


    //  After 10 msecs, send a ping message to output
    zloop_timer (driver->loop, 10, 1,  s_timer_event, output);

#endif