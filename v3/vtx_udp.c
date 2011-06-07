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
    char *frontend;             //  Frontend address
    void *backend;              //  Socket backend (data pipe)
    int routing;                //  Routing mechanism
    int max_links;              //  Maximum allowed links
    int flow_control;           //  Flow control mechanism
    uint publics;               //  Number of public sockets
    int public [PUBLIC_MAX];    //  Public bound sockets
    int private;                //  Private inout socket
    zlist_t *links;             //  List of links for socket

    //  Hack until I figure out where to hold these
    //  Set on incoming request on public socket
    struct sockaddr_in addr;    //  Peer address
} socket_t;

//  Each socket has a list of active links to peers

#define LINK_INIT        0      //  New link, no connect attempt
#define LINK_WAIT        1      //  Send CONNECT, waiting for STATUS
#define LINK_OKAY        2      //  STATUS OK received, link is ready

typedef struct {
    socket_t *socket;           //  Parent socket object
    int state;                  //  Link state
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
    socket_new (driver_t *driver, int socktype, char *frontend);
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
static int
    s_connect_link (zloop_t *loop, zmq_pollitem_t *item, void *arg);


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
socket_new (driver_t *driver, int socktype, char *frontend)
{
    assert (driver);
    socket_t *self = (socket_t *) zmalloc (sizeof (socket_t));

    self->driver = driver;
    self->frontend = strdup (frontend);
    self->links = zlist_new ();
    switch (socktype) {
        case ZMQ_REQ:
            self->routing = ROUTING_BALANCE;
            self->flow_control = FLOW_SYNREQ;
            break;
        case ZMQ_REP:
            self->routing = ROUTING_REPLY;
            self->flow_control = FLOW_SYNREP;
            break;
        case ZMQ_DEALER:
            self->routing = ROUTING_REPLY;
            self->flow_control = FLOW_ASYNC;
            break;
        case ZMQ_PUB:
            self->routing = ROUTING_COPY;
            self->flow_control = FLOW_ASYNC;
            break;
        case ZMQ_SUB:
            self->routing = ROUTING_NONE;
            self->flow_control = FLOW_ASYNC;
            break;
        case ZMQ_PUSH:
            self->routing = ROUTING_BALANCE;
            self->flow_control = FLOW_ASYNC;
            break;
        case ZMQ_PULL:
            self->routing = ROUTING_NONE;
            self->flow_control = FLOW_ASYNC;
            break;
        case ZMQ_PAIR:
            self->routing = ROUTING_DIRECT;
            self->flow_control = FLOW_ASYNC;
            self->max_links = 1;
            break;
        default:
            printf ("E: invalid socket type %d\n", socktype);
            exit (1);
    }
    //  Create backend socket and connect over inproc to frontend
    self->backend = zsocket_new (driver->ctx, ZMQ_PAIR);
    assert (self->backend);
    zsocket_connect (self->backend, "inproc://%s", frontend);

    //  Ask reactor to start monitoring backend socket
    zmq_pollitem_t poll_backend = { self->backend, 0, ZMQ_POLLIN, 0 };
    zloop_poller (driver->loop, &poll_backend, s_internal_input, self);

    //  Create private socket, used for outbound connections
    self->private = socket (AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (self->private == -1)
        derp ("socket");

    //  Enable broadcast mode on private socket, always
    int broadcast_on = 1;
    if (setsockopt (self->private, SOL_SOCKET, SO_BROADCAST,
        &broadcast_on, sizeof (int)) == -1)
        derp ("setsockopt (SO_BROADCAST)");

    //  Ask reactor to start monitoring private socket
    zmq_pollitem_t poll_private = { NULL, self->private, ZMQ_POLLIN, 0 };
    zloop_poller (driver->loop, &poll_private, s_external_input, self);

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
        free (self->frontend);

        //  Close any public handles we have open
        while (self->publics) {
            int fd = self->public [self->publics - 1];
            zmq_pollitem_t poll_item = { 0, 0, fd, 0 };
            zloop_cancel (driver->loop, &poll_item);
            close (fd);
            self->publics--;
        }
        //  Close private handle, if we have it open
        if (self->private) {
            zmq_pollitem_t poll_item = { 0, 0, self->private, 0 };
            zloop_cancel (driver->loop, &poll_item);
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
    self->state = LINK_INIT;
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

//  Handle bind/connect from caller
//  Format is "CONNECT:3:vtx-0x94ad620:*:32000"
//  Fields are: command, socktype, frontend, address, port

static int
s_driver_control (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    driver_t *driver = (driver_t *) arg;
    char *mesg = zstr_recv (item->socket);
    puts (mesg);
    char *context = NULL;
    char *command  = strtok_r (mesg, ":", &context);
    char *socktype = strtok_r (NULL, ":", &context);
    char *frontend = strtok_r (NULL, ":", &context);
    char *address  = strtok_r (NULL, ":", &context);
    char *port     = strtok_r (NULL, ":", &context);

    //  Lookup socket for this frontend, create if necessary
    socket_t *self = (socket_t *) zlist_first (driver->sockets);
    while (self) {
        if (streq (self->frontend, frontend))
            break;
        self = (socket_t *) zlist_next (driver->sockets);
    }
    if (!self)
        self = socket_new (driver, atoi (socktype), frontend);

    struct sockaddr_in addr = { 0 };
    addr.sin_family = AF_INET;
    addr.sin_port = htons (atoi (port));

    //  Handle BIND command
    if (streq (command, "BIND")) {
        //  Create new public UDP socket
        assert (self->publics < PUBLIC_MAX);
        int handle = socket (AF_INET, SOCK_DGRAM, IPPROTO_UDP);
        if (handle == -1)
            derp ("socket");

        //  Bind to specific local address, or *
        if (streq (address, "*"))
            addr.sin_addr.s_addr = htonl (INADDR_ANY);
        else
        if (inet_aton (address, &addr.sin_addr) == 0)
            derp ("inet_aton");
        if (bind (handle, (const struct sockaddr *) &addr, sizeof (addr)) == -1)
            derp ("bind");

        //  Store as new public socket
        self->public [self->publics++] = handle;

        //  Ask reactor to start monitoring backend socket
        zmq_pollitem_t poll_public = { NULL, handle, ZMQ_POLLIN, 0 };
        zloop_poller (driver->loop, &poll_public, s_external_input, self);

        //  Set reactor timer to connect link
        zloop_timer (driver->loop, 1000, 1, s_connect_link, link);
    }
    else
    //  Handle CONNECT command
    if (streq (command, "CONNECT")) {
        //  Connect to specific remote address, or *
        if (streq (address, "*"))
            addr.sin_addr.s_addr = htonl (INADDR_BROADCAST);
        else
        if (inet_aton (address, &addr.sin_addr) == 0)
            derp ("inet_aton");
        //  TODO: bring up link by trying every 100msec
        link_new (self, &addr);
    }
    else
        printf ("Invalid command: %s\n", command);

    zstr_send (item->socket, "0");
    free (command);
    return 0;
}

static int
s_internal_input (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    puts ("INTERNAL INPUT");
    socket_t *self = (socket_t *) arg;

    //  Handle only single-part messages for now
    assert (item->socket == self->backend);
    zframe_t *frame = zframe_recv (self->backend);
    assert (!zframe_more (frame));
    assert (zframe_size (frame) <= VTX_UDP_MSGMAX);

    //  Routing for now, send to first link address or to public
    link_t *link = zlist_first (self->links);
    struct sockaddr_in *addr;
    if (link)
        addr = &link->addr;
    else
        addr = &self->addr;

    //  TODO: 255.255.255.255 causes "Network is unreachable"
    //  am using 127.0.0.255 for client test for now...
    //  Should broadcast to interface broadcast values...
    printf ("Sending to %s:%d\n",
        inet_ntoa (addr->sin_addr), ntohs (addr->sin_port));

    //  TODO: What address do we really send this to?
    //  Have to choose a link, and use private socket
    int rc = sendto (self->private,
        zframe_data (frame), zframe_size (frame), 0,
        (const struct sockaddr *) addr,
        sizeof (struct sockaddr_in));
    if (rc == -1)
        derp ("sendto");
    zframe_destroy (&frame);

    return 0;
}

static int
s_external_input (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    puts ("EXTERNAL INPUT");
    socket_t *self = (socket_t *) arg;

    char buffer [VTX_UDP_MSGMAX];
    socklen_t addr_len = sizeof (struct sockaddr_in);
    ssize_t size = recvfrom (item->fd, buffer, VTX_UDP_MSGMAX, 0,
                            (struct sockaddr *) &self->addr, &addr_len);
    if (size == -1)
        derp ("recvfrom");

    //  TODO: if we connected to *, set link addr now...
    //  Look for first link with '*' and set that address
    //  If no link has this specific address, IOW...
    printf ("Received from %s:%d\n",
        inet_ntoa (self->addr.sin_addr), ntohs (self->addr.sin_port));
    zframe_t *frame = zframe_new (buffer, size);
    zframe_send (&frame, self->backend, 0);
    return 0;
}

static int
s_connect_link (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    link_t *self = (link_t *) arg;
    return 0;
}
