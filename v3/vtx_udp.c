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

//  Report a fatal error and exit the program without cleaning up
//  Use of derp() should be gradually reduced to real failures.
static void derp (char *s) { perror (s); exit (1); }

//  The driver->public table might eventually be made dynamic
#define PUBLIC_MAX      10      //  Max. public sockets, i.e. binds
#define IN_ADDR_SIZE    sizeof (struct sockaddr_in)

static struct {
    char *name;                 //  Command name
} s_command [] = {
    { "ERROR" },
    { "CONNECT" },
    { "CONNECT-OK" },
    { "PING" },
    { "PING-OK" },
    { "SYNC" },
    { "SYNC-OK" },
    { "ASYNC" },
    { "END" }
};

//  ---------------------------------------------------------------------
//  These are the objects we play with in our driver

typedef struct _driver_t driver_t;
typedef struct _socket_t socket_t;
typedef struct _link_t   link_t;


//  ---------------------------------------------------------------------
//  We have one instance of this thread per registered driver.

struct _driver_t {
    zctx_t *ctx;                //  Own context
    zloop_t *loop;              //  zloop reactor for socket i/o
    void *pipe;                 //  Control pipe to VTX vtxname
    zlist_t *sockets;           //  List of sockets to manage
    int64_t errors;             //  Number of errors we hit
};

//  Driver manages a list of high-level socket objects

struct _socket_t {
    driver_t *driver;           //  Parent driver object
    char *vtxname;              //  Message pipe VTX address
    void *msgpipe;              //  Message pipe (0MQ socket)
    int routing;                //  Routing mechanism
    int flow_control;           //  Flow control mechanism
    int max_links;              //  Maximum allowed links
    uint publics;               //  Number of public sockets
    int public [PUBLIC_MAX];    //  Public bound sockets
    int private;                //  Private inout socket
    zhash_t *link_hash;         //  All links, indexed by address
    zlist_t *link_list;         //  List of live links for socket
};

//  Socket manages a list of links to other peers

struct _link_t {
    socket_t *socket;           //  Parent socket object
    Bool alive;                 //  Is link raised and alive?
    Bool outgoing;              //  Connected outwards?
    Bool broadcast;             //  Is link connected to BROADCAST?
    int64_t expiry;             //  Link expires at this time
    struct sockaddr_in addr;    //  Peer address as sockaddr_in
    struct sockaddr_in bcast;   //  Broadcast address, if any
    char *address;              //  Peer address as nnn.nnn.nnn.nnn:nnnnn
};

//  Create, destroy driver, socket, and link objects
static driver_t *
    driver_new (zctx_t *ctx, void *pipe);
static void
    driver_destroy (driver_t **self_p);
static socket_t *
    socket_new (driver_t *driver, int socktype, char *vtxname);
static void
    socket_destroy (socket_t **self_p);
static link_t *
    link_require (socket_t *socket, struct sockaddr_in *addr);
static void
    link_delete (void *argument);
static void
    link_send (link_t *self, int command, byte *data, size_t size);

//  Reactor handlers
static int
    s_driver_control (zloop_t *loop, zmq_pollitem_t *item, void *arg);
static int
    s_internal_input (zloop_t *loop, zmq_pollitem_t *item, void *arg);
static int
    s_external_input (zloop_t *loop, zmq_pollitem_t *item, void *arg);
static int
    s_monitor_link (zloop_t *loop, zmq_pollitem_t *item, void *arg);

//  Utility functions
static char *
    s_broadcast_addr (void);
static char *
    s_sin_addr_to_str (struct sockaddr_in *addr);


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
    self->loop = zloop_new ();

    //  Reactor starts by monitoring the driver control pipe
    zloop_set_verbose (self->loop, FALSE);
    zmq_pollitem_t poll_item = { self->pipe, 0, ZMQ_POLLIN };
    zloop_poller (self->loop, &poll_item, s_driver_control, self);
    return self;
}

static void
driver_destroy (driver_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        driver_t *self = *self_p;
        zclock_log ("I: shutting down driver, %" PRId64 " errors", self->errors);
        while (zlist_size (self->sockets)) {
            socket_t *socket = (socket_t *) zlist_pop (self->sockets);
            socket_destroy (&socket);
        }
        zlist_destroy (&self->sockets);
        zloop_destroy (&self->loop);
        free (self);
        *self_p = NULL;
    }
}

//  ---------------------------------------------------------------------
//  Constructor and destructor for socket

static socket_t *
socket_new (driver_t *driver, int socktype, char *vtxname)
{
    assert (driver);
    socket_t *self = (socket_t *) zmalloc (sizeof (socket_t));

    self->driver = driver;
    self->vtxname = strdup (vtxname);
    self->link_hash = zhash_new ();
    self->link_list = zlist_new ();

    switch (socktype) {
        case ZMQ_REQ:
            self->routing = VTX_ROUTING_ROTATE;
            self->flow_control = VTX_FLOW_SYNREQ;
            break;
        case ZMQ_REP:
            self->routing = VTX_ROUTING_REPLY;
            self->flow_control = VTX_FLOW_SYNREP;
            break;
        case ZMQ_ROUTER:
            self->routing = VTX_ROUTING_REPLY;
            self->flow_control = VTX_FLOW_ASYNC;
            break;
        case ZMQ_DEALER:
            self->routing = VTX_ROUTING_ROTATE;
            self->flow_control = VTX_FLOW_ASYNC;
            break;
        case ZMQ_PUB:
            self->routing = VTX_ROUTING_CCEACH;
            self->flow_control = VTX_FLOW_ASYNC;
            break;
        case ZMQ_SUB:
            self->routing = VTX_ROUTING_NONE;
            self->flow_control = VTX_FLOW_ASYNC;
            break;
        case ZMQ_PUSH:
            self->routing = VTX_ROUTING_ROTATE;
            self->flow_control = VTX_FLOW_ASYNC;
            break;
        case ZMQ_PULL:
            self->routing = VTX_ROUTING_NONE;
            self->flow_control = VTX_FLOW_ASYNC;
            break;
        case ZMQ_PAIR:
            self->routing = VTX_ROUTING_ROTATE;
            self->flow_control = VTX_FLOW_ASYNC;
            self->max_links = 1;
            break;
        default:
            zclock_log ("E: invalid socket type %d", socktype);
            exit (1);
    }
    //  Create msgpipe socket and connect over inproc to vtxname
    self->msgpipe = zsocket_new (driver->ctx, ZMQ_PAIR);
    assert (self->msgpipe);
    zsocket_connect (self->msgpipe, "inproc://%s", vtxname);

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
        free (self->vtxname);

        //  Close any public handles we have open
        while (self->publics) {
            int fd = self->public [self->publics - 1];
            assert (fd);
            zmq_pollitem_t poll_item = { 0, fd };
            zloop_cancel (driver->loop, &poll_item);
            close (fd);
            self->publics--;
        }
        //  Close private handle, if we have it open
        if (self->private) {
            zmq_pollitem_t poll_item = { 0, self->private };
            zloop_cancel (driver->loop, &poll_item);
            close (self->private);
        }
        //  Destroy all links for this socket
        zhash_destroy (&self->link_hash);
        zlist_destroy (&self->link_list);
        free (self);
        *self_p = NULL;
    }
}

//  ---------------------------------------------------------------------
//  Constructor and destructor for link
//  Links are held per socket, indexed by peer hostname:port

static link_t *
link_require (socket_t *socket, struct sockaddr_in *addr)
{
    assert (socket);

    char *address = s_sin_addr_to_str (addr);
    link_t *self = (link_t *) zhash_lookup (socket->link_hash, address);

    if (self == NULL) {
        self = (link_t *) zmalloc (sizeof (link_t));
        self->socket = socket;
        self->address = strdup (address);
        memcpy (&self->addr, addr, IN_ADDR_SIZE);
        zhash_insert (socket->link_hash, address, self);
        zhash_freefn (socket->link_hash, address, link_delete);
        s_monitor_link (socket->driver->loop, NULL, self);
        zclock_log ("I: create link=%p - %s", self, address);
    }
    return self;
}

//  Destroy link object, when link is removed from socket->link_hash
static void
link_delete (void *argument)
{
    link_t *self = (link_t *) argument;
    zclock_log ("I: delete link=%p - %s:%d", self,
        inet_ntoa (self->addr.sin_addr), ntohs (self->addr.sin_port));
    free (self->address);
    free (self);
}

//  Send a buffer of data to link, prefixed by command header

static void
link_send (link_t *self, int command, byte *data, size_t size)
{
    zclock_log ("I: send [%s] - %zd bytes to %s:%d",
        s_command [command].name, size,
        inet_ntoa (self->addr.sin_addr), ntohs (self->addr.sin_port));

    if ((size + VTX_UDP_HEADER) <= VTX_UDP_MSGMAX) {
        byte buffer [size + VTX_UDP_HEADER];
        buffer [0] = VTX_UDP_VERSION << 4;
        buffer [1] = command << 4;
        if (size)
            memcpy (buffer + VTX_UDP_HEADER, data, size);
        int rc = sendto (self->socket->private,
            buffer, size + VTX_UDP_HEADER, 0,
            (const struct sockaddr *) &self->addr, IN_ADDR_SIZE);
        if (rc == -1)
            derp ("sendto");
    }
    else
        zclock_log ("W: over-long message, %d bytes, dropping it", size);
}

//  Link is now active

static void
link_raise (link_t *self)
{
    zclock_log ("I: bring up link=%p", self);
    if (!self->alive) {
        socket_t *socket = self->socket;
        driver_t *driver = socket->driver;
        self->alive = TRUE;
        self->expiry = zclock_time () + VTX_UDP_LINKTTL;
        zlist_append (socket->link_list, self);
        if (zlist_size (socket->link_list) == 1) {
            //  Ask reactor to start monitoring socket's msgpipe pipe
            zmq_pollitem_t poll_msgpipe = { socket->msgpipe, 0, ZMQ_POLLIN, 0 };
            zloop_poller (driver->loop, &poll_msgpipe, s_internal_input, socket);
        }
    }
}

//  Link is now inactive

static void
link_lower (link_t *self)
{
    zclock_log ("I: take down link=%p", self);
    if (self->alive) {
        socket_t *socket = self->socket;
        driver_t *driver = socket->driver;
        self->alive = FALSE;
        zlist_remove (socket->link_list, self);
        if (zlist_size (socket->link_list) == 0) {
            //  Ask reactor to start monitoring socket's msgpipe pipe
            zmq_pollitem_t poll_msgpipe = { socket->msgpipe, 0, ZMQ_POLLIN, 0 };
            zloop_cancel (driver->loop, &poll_msgpipe);
        }
    }
}

//  ---------------------------------------------------------------------
//  Reactor handlers

//  Handle bind/connect from caller:
//
//  [command]   BIND or CONNECT
//  [socktype]  0MQ socket type as ASCII number
//  [vtxname]   VTX name for the socket
//  [address]   External address to bind/connect to

static int
s_driver_control (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    driver_t *driver = (driver_t *) arg;
    zmsg_t *request = zmsg_recv (item->socket);

    char *command  = zmsg_popstr (request);
    char *socktype = zmsg_popstr (request);
    char *vtxname  = zmsg_popstr (request);
    char *address  = zmsg_popstr (request);
    zmsg_destroy (&request);

    //  Split port number off address
    char *port = strchr (address, ':');
    assert (port);
    *port++ = 0;

    //  Lookup socket with this vtxname, create if necessary
    socket_t *self = (socket_t *) zlist_first (driver->sockets);
    while (self) {
        if (streq (self->vtxname, vtxname))
            break;
        self = (socket_t *) zlist_next (driver->sockets);
    }
    if (!self)
        self = socket_new (driver, atoi (socktype), vtxname);

    struct sockaddr_in addr = { 0 };
    addr.sin_family = AF_INET;
    addr.sin_port = htons (atoi (port));

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
        if (bind (handle, (const struct sockaddr *) &addr, IN_ADDR_SIZE) == -1)
            derp ("bind");

        //  Store as new public socket
        self->public [self->publics++] = handle;

        //  Ask reactor to start monitoring this public socket
        zmq_pollitem_t poll_public = { NULL, handle, ZMQ_POLLIN, 0 };
        zloop_poller (driver->loop, &poll_public, s_external_input, self);
    }
    else
    if (streq (command, "CONNECT")) {
        //  Connect to specific remote address, or *
        char *addr_to_use = address;
        if (streq (address, "*"))
            addr_to_use = s_broadcast_addr ();
        if (inet_aton (addr_to_use, &addr.sin_addr) == 0)
            derp ("inet_aton");

        //  For each outgoing connection, we create a link object
        link_t *link = link_require (self, &addr);
        link->outgoing = TRUE;
        if (streq (address, "*")) {
            link->broadcast = TRUE;
            memcpy (&link->bcast, &addr, IN_ADDR_SIZE);
        }
    }
    else
        zclock_log ("E: invalid command: %s", command);

    zstr_send (item->socket, "0");
    free (command);
    free (socktype);
    free (vtxname);
    free (address);
    return 0;
}

//  -------------------------------------------------------------------------
//  Input message on data pipe from application socket

static int
s_internal_input (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    socket_t *socket = (socket_t *) arg;

    //  Handle only single-part messages for now
    //  We'll need to handle multipart in order to do REQ/REP routing
    assert (item->socket == socket->msgpipe);
    zframe_t *frame = zframe_recv (socket->msgpipe);
    assert (!zframe_more (frame));
    assert (zframe_size (frame) <= VTX_UDP_MSGMAX);

    //  Route message to active links as appropriate
    if (socket->routing == VTX_ROUTING_NONE)
        zclock_log ("W: send() not allowed - dropping message");
    else
    if (socket->routing == VTX_ROUTING_REPLY)
        zclock_log ("W: reply routing not implemented yet - dropping message");
    else
    if (socket->routing == VTX_ROUTING_ROTATE) {
        //  Find next live link if any
        link_t *link = (link_t *) zlist_pop (socket->link_list);
        if (link) {
            link_send (link, VTX_UDP_ASYNC,
                       zframe_data (frame), zframe_size (frame));
            zlist_append (socket->link_list, link);
        }
        else
            zclock_log ("W: no live links - dropping message");
    }
    else
    if (socket->routing == VTX_ROUTING_CCEACH) {
        link_t *link = (link_t *) zlist_first (socket->link_list);
        while (link) {
            link_send (link, VTX_UDP_ASYNC,
                       zframe_data (frame), zframe_size (frame));
            link = (link_t *) zlist_next (socket->link_list);
        }
    }
    else
        zclock_log ("E: unknown routing mechanism - dropping message");

    zframe_destroy (&frame);
    return 0;
}


//  -------------------------------------------------------------------------
//  Input message on public UDP socket
//  This implements the receiver side of the UDP protocol-without-a-name

static int
s_external_input (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    socket_t *socket = (socket_t *) arg;
    driver_t *driver = socket->driver;

    byte buffer [VTX_UDP_MSGMAX];
    struct sockaddr_in addr;
    socklen_t addr_len = IN_ADDR_SIZE;
    ssize_t size = recvfrom (item->fd, buffer, VTX_UDP_MSGMAX, 0,
                            (struct sockaddr *) &addr, &addr_len);
    if (size == -1)
        derp ("recvfrom");

    //  Parse incoming protocol command
    int version  = buffer [0] >> 4;
    int command  = buffer [1] >> 4;
    int sequence = buffer [1] && 0xf;
    byte *body = buffer + VTX_UDP_HEADER;
    size_t body_size = size - VTX_UDP_HEADER;

    if (version != VTX_UDP_VERSION) {
        zclock_log ("W: garbage version '%d' - dropping message", version);
        driver->errors++;
        return 0;
    }
    if (command >= VTX_UDP_CMDLIMIT) {
        zclock_log ("W: garbage command '%d' - dropping message", command);
        driver->errors++;
        return 0;
    }
    char *address = s_sin_addr_to_str (&addr);
    zclock_log ("I: recv [%s] - %zd bytes from %s",
        s_command [command].name, size, address);

    if (command == VTX_UDP_ERROR) {
        //  What are the errors, and how do we handle them?
        link_t *link = (link_t *) zhash_lookup (socket->link_hash, address);
        if (!link) {
            zclock_log ("W: unknown peer - dropping message");
            driver->errors++;
            return 0;
        }
    }
    else
    if (command == VTX_UDP_CONNECT) {
        //  Create new link if this peer isn't known to us
        link_t *link = link_require (socket, &addr);
        link_send (link, VTX_UDP_CONNECT_OK, body, body_size);
        link_raise (link);
    }
    else
    if (command == VTX_UDP_CONNECT_OK) {
        //  Command body has address we asked to connect to
        buffer [size] = 0;
        link_t *link = (link_t *) zhash_lookup (socket->link_hash, (char *) body);
        if (link) {
            if (strneq (address, (char *) body)) {
                zclock_log ("I: rename link from %s to %s", (char *) body, address);
                int rc = zhash_rename (socket->link_hash, (char *) body, address);
                assert (rc == 0);
                memcpy (&link->addr, &addr, IN_ADDR_SIZE);
                free (link->address);
                link->address = strdup (address);
            }
            link_raise (link);
        }
        else {
            zclock_log ("W: no such link '%s' - dropping message", body);
            driver->errors++;
            return 0;
        }
    }
    else
    if (command == VTX_UDP_PING) {
        //  If we don't know this peer, don't create a new link
        link_t *link = (link_t *) zhash_lookup (socket->link_hash, address);
        if (link)
            link_send (link, VTX_UDP_PING_OK, NULL, 0);
        else {
            zclock_log ("W: unknown peer - dropping message");
            driver->errors++;
            return 0;
        }
    }
    else
    if (command == VTX_UDP_PING_OK) {
        //  If we don't know this peer, don't create a new link
        link_t *link = (link_t *) zhash_lookup (socket->link_hash, address);
        if (link)
            link->expiry = zclock_time () + VTX_UDP_LINKTTL;
        else {
            zclock_log ("W: unknown peer - dropping message");
            driver->errors++;
            return 0;
        }
    }
    else
    if (command == VTX_UDP_SYNC) {
    }
    else
    if (command == VTX_UDP_SYNC_OK) {
    }
    else
    if (command == VTX_UDP_ASYNC) {
        //  Accept input only from known and connected peers
        link_t *link = (link_t *) zhash_lookup (socket->link_hash, address);
        if (link) {
            zframe_t *frame = zframe_new (body, body_size);
            zframe_send (&frame, socket->msgpipe, 0);
        }
        else {
            zclock_log ("W: unknown peer - dropping message");
            driver->errors++;
            return 0;
        }
    }
    return 0;
}


//  Monitor link for connectivity
//  If link is alive, send PING, if it's pending send CONNECT
static int
s_monitor_link (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    link_t *link = (link_t *) arg;
    int interval = 1000;        //  Unless otherwise specified
    if (link->alive) {
        if (zclock_time () > link->expiry) {
            link_lower (link);
            //  If this was a broadcast link, reset it to BROADCAST
            if (link->broadcast) {
                char *address = s_sin_addr_to_str (&link->bcast);
                zclock_log ("I: rename link from %s to %s", link->address, address);
                int rc = zhash_rename (link->socket->link_hash, link->address, address);
                assert (rc == 0);
                memcpy (&link->addr, &link->bcast, IN_ADDR_SIZE);
                free (link->address);
                link->address = strdup (address);
            }
            else
            if (!link->outgoing) {
                zhash_delete (link->socket->link_hash, link->address);
                interval = 0;           //  Don't reset timer
            }
        }
        else {
            link->expiry = zclock_time () + VTX_UDP_LINKTTL;
            link_send (link, VTX_UDP_PING, NULL, 0);
            interval = VTX_UDP_LINKTTL - VTX_UDP_LATENCY;
        }
    }
    else
    if (link->outgoing)
        link_send (link, VTX_UDP_CONNECT,
                   (byte *) link->address, strlen (link->address));

    if (interval)
        zloop_timer (loop, interval, 1, s_monitor_link, link);
    return 0;
}


//  Returns (last valid) broadcast address for LAN
//  On Windows we just force SO_BROADCAST, getting the interfaces
//  via win32 is too ugly to put into this code...

static char *
s_broadcast_addr (void)
{
    char *address = "255.255.255.255";
#if defined (__UNIX__)
    struct ifaddrs *interfaces;
    if (getifaddrs (&interfaces) == 0) {
        struct ifaddrs *interface = interfaces;
        while (interface) {
            struct sockaddr *sa = interface->ifa_broadaddr;
            if (sa && sa->sa_family == AF_INET) {
                address = inet_ntoa (((struct sockaddr_in *) sa)->sin_addr);
            }
            interface = interface->ifa_next;
        }
    }
    freeifaddrs (interfaces);
#endif
    return address;
}


//  Converts a sockaddr_in to a string, returns static result

static char *
s_sin_addr_to_str (struct sockaddr_in *addr)
{
    static char
        address [24];
    snprintf (address, 24, "%s:%d",
        inet_ntoa (addr->sin_addr), ntohs (addr->sin_port));
    return address;
}
