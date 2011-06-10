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
    int link_id;                //  Link identity sequence number
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
    zlist_t *wait_links;        //  List of waiting links for socket
    zlist_t *live_links;        //  List of live links for socket
};


struct _link_t {
    socket_t *socket;           //  Parent socket object
    int connected;              //  Is link connected?
    int64_t expiry;             //  Link expires at this time
    int id;                     //  Link ID (16 bits max)
    char idstr [5];             //  ID as printed hex number
    char *address;              //  Peer address as string
    struct sockaddr_in addr;    //  Peer address as sockaddr_in
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
    link_new (socket_t *socket, char *address, struct sockaddr_in *addr);
static void
    link_delete (void *data);

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
    self->wait_links = zlist_new ();
    self->live_links = zlist_new ();
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

    //  Ask reactor to start monitoring msgpipe socket
    zmq_pollitem_t poll_msgpipe = { self->msgpipe, 0, ZMQ_POLLIN, 0 };
    zloop_poller (driver->loop, &poll_msgpipe, s_internal_input, self);

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
        //  Destroy all live links for this socket
        while (zlist_size (self->live_links)) {
            link_t *link = (link_t *) zlist_pop (self->live_links);
            zhash_delete (driver->links, link->idstr);
        }
        zlist_destroy (&self->live_links);

        //  Destroy all waiting links for this socket
        while (zlist_size (self->wait_links)) {
            link_t *link = (link_t *) zlist_pop (self->wait_links);
            zhash_delete (driver->links, link->idstr);
        }
        zlist_destroy (&self->wait_links);
        free (self);
        *self_p = NULL;
    }
}

//  ---------------------------------------------------------------------
//  Constructor and destructor for link

static link_t *
link_new (socket_t *socket, char *address, struct sockaddr_in *addr)
{
    assert (socket);
    driver_t *driver = socket->driver;

    //  Current design allows up to 64K links and then stops working
    link_t *self = NULL;
    if (driver->link_id < VTX_UDP_MAX_LINK) {
        self = (link_t *) zmalloc (sizeof (link_t));

        self->socket = socket;
        self->expiry = zclock_time () + VTX_UDP_LINKTTL;
        self->address = strdup (address);
        self->id = driver->link_id++;
        snprintf (self->idstr, 5, "%x", self->id);
        memcpy (&self->addr, addr, IN_ADDR_SIZE);

        zlist_push (socket->wait_links, self);
        zhash_insert (driver->links, self->idstr, self);
        zhash_freefn (driver->links, self->idstr, link_delete);
    }
    return self;
}

static void
link_delete (void *data)
{
    link_t *self = (link_t *) data;
    free (self->address);
    free (self->reply);
    free (self);
}

static void
link_send (link_t *self, int socket, zframe_t *frame)
{
    byte *data = zframe_data (frame);
    size_t size = zframe_size (frame);

    zclock_log ("Sending %zd bytes to %s:%d", size,
        inet_ntoa (self->addr.sin_addr), ntohs (self->addr.sin_port));
    int rc = sendto (socket, data, size, 0,
        (const struct sockaddr *) &self->addr, IN_ADDR_SIZE);
    if (rc == -1)
        derp ("sendto");
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
            //  TODO: should find real interface broadcast values
            addr.sin_addr.s_addr = htonl (INADDR_BROADCAST);
        }
        else
        if (inet_aton (address, &addr.sin_addr) == 0)
            derp ("inet_aton");

        //  For each outgoing connection, we create a link object
        link_t *link = link_new (self, address, &addr);
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
    assert (item->socket == self->msgpipe);
    zframe_t *frame = zframe_recv (self->msgpipe);
    assert (!zframe_more (frame));
    assert (zframe_size (frame) <= VTX_UDP_MSGMAX);

    //TODO
        - format message as ASYNC command
        - insert link value into header? at send time...

    //  Route message to live links as appropriate
    if (self->routing == VTX_ROUTING_NONE)
        zclock_log ("W: send() not allowed, dropping message");
    else
    if (self->routing == VTX_ROUTING_REPLY)
        zclock_log ("W: reply routing not implemented yet, dropping message");
    else
    if (self->routing == VTX_ROUTING_ROTATE) {
        //  Find next live link if any
        link_t *link = (link_t *) zlist_pop (self->live_links);
        if (link) {
            link_send (link, self->private, frame);
            zlist_append (self->live_links, link);
        }
        else
            zclock_log ("W: no live links - dropping message");
    }
    else
    if (self->routing == VTX_ROUTING_CCEACH) {
        link_t *link = (link_t *) zlist_first (self->live_links);
        while (link) {
            link_send (link, self->private, frame);
            link = (link_t *) zlist_next (self->live_links);
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

    char buffer [VTX_UDP_MSGMAX];
    struct sockaddr_in addr;
    socklen_t addr_len = IN_ADDR_SIZE;
    ssize_t size = recvfrom (item->fd, buffer, VTX_UDP_MSGMAX, 0,
                            (struct sockaddr *) &addr, &addr_len);
    if (size == -1)
        derp ("recvfrom");

    zclock_log ("Received from %s:%d",
        inet_ntoa (addr.sin_addr), ntohs (addr.sin_port));

    //  TODO: get link id from message, use to find link, then update
    //  peer address in link if that's sensible, else discard message.

    //TODO: parse incoming command
    - if CONNECT-OK, link becomes active
        - and set peer addr in link
        - iff link isn't already connected



    zframe_t *frame = zframe_new (buffer, size);
    zframe_send (&frame, self->msgpipe, 0);
    return 0;
}

static int
s_connect_link (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    //TODO:
    - if link is not live, send CONNECT message
    - we will keep sending CONNECT every 1 second
        - could be 100msec to match 0MQ?



    link_t *link = (link_t *) arg;
    zclock_log ("CONNECT LINK to %s", link->address);
    zloop_timer (loop, 1000, 1, s_connect_link, link);
    return 0;
}


#if 0
   // BSD-style implementation
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
