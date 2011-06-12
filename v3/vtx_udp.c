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

static char *s_command_name [] = {
    "ERROR",
    "CONNECT", "CONNECT-OK",
    "PING", "PING-OK",
    "SYNC", "SYNC-OK",
    "ASYNC"
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
    zhash_t *links;             //  All links, indexed by link id
    link_t *link_table [VTX_UDP_LINKMAX];
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
    zlist_t *links;             //  List of live links for socket
};

//  Socket manages a list of links to other peers

struct _link_t {
    socket_t *socket;           //  Parent socket object
    int alive;                  //  Is link up and connected?
    int64_t expiry;             //  Link expires at this time
    int this_id;                //  Link ID of this side (8 bits)
    int peer_id;                //  Link ID of peer side (8 bits)
    struct sockaddr_in addr;    //  Peer address as sockaddr_in
    //  Not used yet
    int request_seq;            //  Last request sequence
    byte *reply;                //  Last sent SYNREP if any
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
    link_new (socket_t *socket, struct sockaddr_in *addr, int peer_id);
static void
    link_destroy (link_t **self_p);
static void
    link_send (link_t *self, int socket, int command, byte *data, size_t size);

//  Reactor handlers
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
socket_new (driver_t *driver, int socktype, char *vtxname)
{
    assert (driver);
    socket_t *self = (socket_t *) zmalloc (sizeof (socket_t));

    self->driver = driver;
    self->vtxname = strdup (vtxname);
    self->links = zlist_new ();
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
        int link_nbr;
        for (link_nbr = 0; link_nbr < VTX_UDP_LINKMAX; link_nbr++) {
            link_t *link = driver->link_table [link_nbr];
            if (link && link->socket == self)
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
link_new (socket_t *socket, struct sockaddr_in *addr, int peer_id)
{
    assert (socket);
    driver_t *driver = socket->driver;

    //  Current design allows up to 256 links in/out combined
    link_t *self = NULL;
    int link_nbr;
    for (link_nbr = 0; link_nbr < VTX_UDP_LINKMAX; link_nbr++)
        if (driver->link_table [link_nbr] == NULL)
            break;
    if (link_nbr < VTX_UDP_LINKMAX) {
        self = (link_t *) zmalloc (sizeof (link_t));
        driver->link_table [link_nbr] = self;
        assert (driver->link_table [link_nbr] == self);
        self->socket = socket;
        self->expiry = zclock_time () + VTX_UDP_LINKTTL;
        self->this_id = link_nbr;
        self->peer_id = peer_id;
        memcpy (&self->addr, addr, IN_ADDR_SIZE);
    }
    zclock_log ("I: new link=%p", self);
    return self;
}

static void
link_destroy (link_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        link_t *self = *self_p;
        driver_t *driver = self->socket->driver;
        int link_nbr = self->this_id;
        assert (driver->link_table [link_nbr] == self);
        driver->link_table [self->this_id] = NULL;
        free (self->reply);
        free (self);
        *self_p = NULL;
    }
}

//  Send a buffer of data to link, prefixed by command header

static void
link_send (link_t *self, int socket, int command, byte *data, size_t size)
{
    zclock_log ("Send [%s] - %zd bytes to %s:%d",
        s_command_name [command], size,
        inet_ntoa (self->addr.sin_addr), ntohs (self->addr.sin_port));

    if ((size + VTX_UDP_HEADER) <= VTX_UDP_MSGMAX) {
        byte buffer [size + VTX_UDP_HEADER];
        buffer [0] = VTX_UDP_VERSION << 4;
        buffer [1] = command << 4;
        buffer [2] = (byte) (self->this_id);
        buffer [3] = (byte) (self->peer_id);
        if (size)
            memcpy (buffer + VTX_UDP_HEADER, data, size);
        int rc = sendto (socket, buffer, size + VTX_UDP_HEADER, 0,
            (const struct sockaddr *) &self->addr, IN_ADDR_SIZE);
        if (rc == -1)
            derp ("sendto");
    }
    else
        zclock_log ("W: over-long message, %d bytes, dropping it", size);
}

//  Link is now active

static void
link_bring_up (link_t *self)
{
    zclock_log ("I: bring up link=%p", self);
    if (!self->alive) {
        socket_t *socket = self->socket;
        driver_t *driver = socket->driver;
        self->alive = TRUE;
        zlist_append (socket->links, self);
        if (zlist_size (socket->links) == 1) {
            //  Ask reactor to start monitoring socket's msgpipe pipe
            zmq_pollitem_t poll_msgpipe = { socket->msgpipe, 0, ZMQ_POLLIN, 0 };
            zloop_poller (driver->loop, &poll_msgpipe, s_internal_input, socket);
        }
    }
}

//  Link is now inactive

static void
link_take_down (link_t *self)
{
    zclock_log ("I: take down link=%p", self);
    if (self->alive) {
        socket_t *socket = self->socket;
        driver_t *driver = socket->driver;
        self->alive = FALSE;
        zlist_remove (socket->links, self);
        if (zlist_size (socket->links) == 0) {
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
        if (streq (address, "*")) {
            //  TODO: won't work if we don't have an active interface;
            //  should find real interface broadcast values and send once to each
            addr.sin_addr.s_addr = htonl (INADDR_BROADCAST);
        }
        else
        if (inet_aton (address, &addr.sin_addr) == 0)
            derp ("inet_aton");

        //  For each outgoing connection, we create a link object
        link_t *link = link_new (self, &addr, 0);
        assert (link);
        s_connect_link (driver->loop, NULL, link);
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

static int
s_internal_input (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    socket_t *self = (socket_t *) arg;

    //  Handle only single-part messages for now
    //  We'll need to handle multipart in order to do REQ/REP routing
    assert (item->socket == self->msgpipe);
    zframe_t *frame = zframe_recv (self->msgpipe);
    assert (!zframe_more (frame));
    assert (zframe_size (frame) <= VTX_UDP_MSGMAX);

    //  Route message to active links as appropriate
    if (self->routing == VTX_ROUTING_NONE)
        zclock_log ("W: send() not allowed, dropping message");
    else
    if (self->routing == VTX_ROUTING_REPLY)
        zclock_log ("W: reply routing not implemented yet, dropping message");
    else
    if (self->routing == VTX_ROUTING_ROTATE) {
        //  Find next live link if any
        link_t *link = (link_t *) zlist_pop (self->links);
        if (link) {
            link_send (link, self->private, VTX_UDP_ASYNC,
                zframe_data (frame), zframe_size (frame));
            zlist_append (self->links, link);
        }
        else
            zclock_log ("W: no live links - dropping message");
    }
    else
    if (self->routing == VTX_ROUTING_CCEACH) {
        link_t *link = (link_t *) zlist_first (self->links);
        while (link) {
            link_send (link, self->private, VTX_UDP_ASYNC,
                zframe_data (frame), zframe_size (frame));
            link = (link_t *) zlist_next (self->links);
        }
    }
    else
        zclock_log ("E: unknown routing mechanism - dropping message");

    zframe_destroy (&frame);
    return 0;
}

static int
s_external_input (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    socket_t *self = (socket_t *) arg;
    driver_t *driver = self->driver;

    byte buffer [VTX_UDP_MSGMAX];
    struct sockaddr_in addr;
    socklen_t addr_len = IN_ADDR_SIZE;
    ssize_t size = recvfrom (item->fd, buffer, VTX_UDP_MSGMAX, 0,
                            (struct sockaddr *) &addr, &addr_len);
    if (size == -1)
        derp ("recvfrom");

    zclock_log ("Received %d bytes from %s:%d",
        size, inet_ntoa (addr.sin_addr), ntohs (addr.sin_port));

    //  Parse incoming protocol command
    int version  = buffer [0] >> 4;
    int command  = buffer [1] >> 4;
    int sequence = buffer [1] && 0xf;
    int this_id  = buffer [2];
    int peer_id  = buffer [3];

    if (version != VTX_UDP_VERSION)
        zclock_log ("W: garbage version '%d' - dropping message", version);
    else
    if (command == VTX_UDP_ERROR)
        zclock_log ("W: received ERROR from peer");
    else
    if (command == VTX_UDP_CONNECT) {
        zclock_log ("I: received CONNECT from peer");
        //  Accept connection, create new live link at our side
        link_t *link = link_new (self, &addr, peer_id);
        assert (link);
        link_bring_up (link);
        link_send (link, self->private, VTX_UDP_CONNECT_OK, NULL, 0);
    }
    else
    if (command == VTX_UDP_CONNECT_OK) {
        zclock_log ("I: received CONNECT-OK from peer");
        link_t *link = driver->link_table [peer_id];
        if (link) {
            link->peer_id = this_id;
            link_bring_up (link);
        }
        else
            zclock_log ("W: invalid peer_id, no such link");
    }
    else
    if (command == VTX_UDP_PING) {
        zclock_log ("I: received PING from peer");
    }
    else
    if (command == VTX_UDP_PING_OK) {
        zclock_log ("I: received PING OK from peer");
    }
    else
    if (command == VTX_UDP_SYNC) {
        zclock_log ("I: received SYNC from peer");
    }
    else
    if (command == VTX_UDP_SYNC_OK) {
        zclock_log ("I: received SYNC OK from peer");
    }
    else
    if (command == VTX_UDP_ASYNC) {
        zclock_log ("I: received ASYNC from peer");
        zframe_t *frame = zframe_new (buffer + VTX_UDP_HEADER,
                                      size - VTX_UDP_HEADER);
        zframe_send (&frame, self->msgpipe, 0);
    }
    return 0;
}

static int
s_connect_link (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    link_t *link = (link_t *) arg;
    //  If link is alive, stop trying to connect
    if (!link->alive) {
        link_send (link, link->socket->private, VTX_UDP_CONNECT, NULL, 0);
        zloop_timer (loop, 1000, 1, s_connect_link, link);
    }
    return 0;
}


#if 0
   // Get interfaces, for broadcast
   struct ifaddrs * ifap;
   if (getifaddrs(&ifap) == 0)
   {
      struct ifaddrs * p = ifap;
      while(p)
      {
         uint32 ifaAddr  = SockAddrToUint32(p->ifa_addr);
         uint32 dstAddr  = SockAddrToUint32(p->ifa_dstaddr);
         if (ifaAddr > 0)
         {
            char ifaAddrStr[32];  Inet_NtoA(ifaAddr, ifaAddrStr);
            char dstAddrStr[32];  Inet_NtoA(dstAddr, dstAddrStr);
            printf ("name=[%s] address=[%s] broadcastAddr=[%s]\n", p->ifa_name, ifaAddrStr, dstAddrStr);
         }
         p = p->ifa_next;
      }
      freeifaddrs(ifap);
   }
#endif

