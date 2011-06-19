/*  =====================================================================
    VTX - 0MQ virtual transport interface - ZMTP / TCP driver

    Implements the VTX virtual socket interface using the ZMTP protocol
    over TCP. This is for sanity testing with existing 0MQ applications.

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

#include "vtx_tcp.h"

//  Report a fatal error and exit the program without cleaning up
//  Use of derp() should be gradually reduced to real failures.
static void derp (char *s) { perror (s); exit (1); }

#define IN_ADDR_SIZE    sizeof (struct sockaddr_in)

//  ---------------------------------------------------------------------
//  These are the objects we play with in our driver

typedef struct _driver_t driver_t;
typedef struct _vocket_t vocket_t;
typedef struct _binding_t binding_t;
typedef struct _peering_t peering_t;


//  ---------------------------------------------------------------------
//  A driver_t holds the context for one driver thread, which matches
//  one registered driver. We create a driver by calling vtx_tcp_driver,
//  and the thread runs until the process is interrupted. A driver works
//  with a list of vockets, which are virtual 0MQ sockets.

struct _driver_t {
    zctx_t *ctx;                //  Own context
    char *scheme;               //  Driver scheme
    zloop_t *loop;              //  zloop reactor for socket I/O
    zlist_t *vockets;           //  List of vockets per driver
    void *pipe;                 //  Control pipe to/from VTX frontend
};

//  A vocket_t holds the context for one virtual socket, which implements
//  the semantics of a 0MQ socket. We create a vocket when first binding
//  or connecting a VTX name. We destroy vockets at shutdown, or when the
//  caller calls vtx_close. A vocket manages a set of bindings for
//  incoming connections, a set of peerings to other nodes, and other
//  transport-specific properties.
//

struct _vocket_t {
    driver_t *driver;           //  Parent driver object
    char *vtxname;              //  Message pipe VTX address
    void *msgpipe;              //  Message pipe (0MQ socket)
    zhash_t *binding_hash;      //  Bindings, indexed by address
    zhash_t *peering_hash;      //  Peerings, indexed by address
    zlist_t *peering_list;      //  Peerings, in simple list
    zlist_t *live_peerings;     //  Peerings that are alive
    peering_t *reply_to;        //  For reply routing
    int peerings;               //  Current number of peerings
    //  These properties control the vocket routing semantics
    int routing;                //  Routing mechanism
    Bool nomnom;                //  Accepts NOM commands
    int min_peerings;           //  Minimum peerings for routing
    int max_peerings;           //  Maximum allowed peerings
    //  hwm strategy
    //  filter on input messages
    //  ZMTP specific properties
};

//  This maps 0MQ socket types to the VTX emulation
static struct {
    int socktype;
    int routing;
    Bool nomnom;
    int min_peerings;
    int max_peerings;
} s_vocket_config [] = {
    { ZMQ_REQ,    VTX_ROUTING_REQUEST, TRUE,  1, VTX_MAX_PEERINGS },
    { ZMQ_REP,    VTX_ROUTING_REPLY,   TRUE,  1, VTX_MAX_PEERINGS },
    { ZMQ_ROUTER, VTX_ROUTING_ROUTER,  TRUE,  0, VTX_MAX_PEERINGS },
    { ZMQ_DEALER, VTX_ROUTING_DEALER,  TRUE,  1, VTX_MAX_PEERINGS },
    { ZMQ_PUB,    VTX_ROUTING_PUBLISH, FALSE, 0, VTX_MAX_PEERINGS },
    { ZMQ_SUB,    VTX_ROUTING_NONE,    TRUE,  1, VTX_MAX_PEERINGS },
    { ZMQ_PUSH,   VTX_ROUTING_DEALER,  FALSE, 1, VTX_MAX_PEERINGS },
    { ZMQ_PULL,   VTX_ROUTING_NONE,    TRUE,  1, VTX_MAX_PEERINGS },
    { ZMQ_PAIR,   VTX_ROUTING_SINGLE,  TRUE,  1, 1 }
};


//  A binding_t holds the context for a single binding.
//  For ZMTP, this is includes the native TCP socket handle.

struct _binding_t {
    driver_t *driver;           //  Parent driver object
    vocket_t *vocket;           //  Parent vocket object
    char *address;              //  Local address:port bound to
    //  ZMTP specific properties
    int handle;                 //  TCP socket handle
};

//  A peering_t holds the context for a peering to another node across
//  our transport. Peerings can be outgoing (will try to reconnect if
//  lowered) or incoming (will be destroyed when lowered).
//  For ZMTP, this includes the actual TCP address to talk to,
//  and the broadcast address if this was a broadcast connection.

struct _peering_t {
    driver_t *driver;           //  Parent driver object
    vocket_t *vocket;           //  Parent vocket object
    Bool alive;                 //  Is peering raised and alive?
    Bool outgoing;              //  Connected handles?
    int64_t expiry;             //  Peering expires at this time
    char *address;              //  Peer address as nnn.nnn.nnn.nnn:nnnnn
    //  ZMTP specific properties
    struct sockaddr_in addr;    //  Peer address as sockaddr_in
    zmsg_t *request;            //  Pending request NOM, if any
    zmsg_t *reply;              //  Last reply NOM, if any
    int handle;                 //  Handle for input/output
};

//  Basic methods for each of our object types (it's not really a clean
//  abstraction since objects are not opaque, but it works pretty well.)
//
static driver_t *
    driver_new (zctx_t *ctx, void *pipe);
static void
    driver_destroy (driver_t **self_p);
static vocket_t *
    vocket_new (driver_t *driver, int socktype, char *vtxname);
static void
    vocket_destroy (vocket_t **self_p);
static binding_t *
    binding_require (vocket_t *vocket, char *address);
static void
    binding_delete (void *argument);
static peering_t *
    peering_require (vocket_t *vocket, char *address, Bool outgoing);
static void
    peering_delete (void *argument);
static int
    peering_send_msg (peering_t *self, zmsg_t *msg);

//  Reactor handlers
static int
    s_driver_control (zloop_t *loop, zmq_pollitem_t *item, void *arg);
static int
    s_route_from_app (zloop_t *loop, zmq_pollitem_t *item, void *arg);
static int
    s_accept_peering (zloop_t *loop, zmq_pollitem_t *item, void *arg);
static int
    s_route_off_network (zloop_t *loop, zmq_pollitem_t *item, void *arg);
static int
    s_monitor_peering (zloop_t *loop, zmq_pollitem_t *item, void *arg);

//  Utility functions
static char *
    s_sin_addr_to_str (struct sockaddr_in *addr);
static void
    s_str_to_sin_addr (struct sockaddr_in *addr, char *address);
static int
    s_handle_io_error (driver_t *driver, char *reason);

//  ---------------------------------------------------------------------
//  Main driver thread is minimal, all work is done by reactor

void vtx_tcp_driver (void *args, zctx_t *ctx, void *pipe)
{
    //  Create driver instance
    driver_t *driver = driver_new (ctx, pipe);
    //  Run reactor until we exit from failure or interrupt
    zloop_start (driver->loop);
    //  Destroy driver instance
    driver_destroy (&driver);
}

//  ---------------------------------------------------------------------
//  Registers our protocol driver with the VTX engine

int vtx_tcp_load (vtx_t *vtx)
{
    return vtx_register (vtx, VTX_TCP_SCHEME, vtx_tcp_driver);
}


//  ---------------------------------------------------------------------
//  Constructor and destructor for driver

static driver_t *
driver_new (zctx_t *ctx, void *pipe)
{
    driver_t *self = (driver_t *) zmalloc (sizeof (driver_t));
    self->ctx = ctx;
    self->pipe = pipe;
    self->vockets = zlist_new ();
    self->loop = zloop_new ();
    self->scheme = VTX_TCP_SCHEME;

    //  Reactor starts by monitoring the driver control pipe
    zloop_set_verbose (self->loop, FALSE);
    zmq_pollitem_t poll_control = { self->pipe, 0, ZMQ_POLLIN };
    zloop_poller (self->loop, &poll_control, s_driver_control, self);
    return self;
}

static void
driver_destroy (driver_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        driver_t *self = *self_p;
        zclock_log ("I: shutting down driver");
        while (zlist_size (self->vockets)) {
            vocket_t *vocket = (vocket_t *) zlist_pop (self->vockets);
            vocket_destroy (&vocket);
        }
        zlist_destroy (&self->vockets);
        zloop_destroy (&self->loop);
        free (self);
        *self_p = NULL;
    }
}

//  ---------------------------------------------------------------------
//  Constructor and destructor for vocket

static vocket_t *
vocket_new (driver_t *driver, int socktype, char *vtxname)
{
    assert (driver);
    vocket_t *self = (vocket_t *) zmalloc (sizeof (vocket_t));

    self->driver = driver;
    self->vtxname = strdup (vtxname);
    self->binding_hash = zhash_new ();
    self->peering_hash = zhash_new ();
    self->peering_list = zlist_new ();
    self->live_peerings = zlist_new ();

    uint index;
    for (index = 0; index < tblsize (s_vocket_config); index++)
        if (socktype == s_vocket_config [index].socktype)
            break;

    if (index < tblsize (s_vocket_config)) {
        self->routing = s_vocket_config [index].routing;
        self->nomnom = s_vocket_config [index].nomnom;
        self->min_peerings = s_vocket_config [index].min_peerings;
        self->max_peerings = s_vocket_config [index].max_peerings;
    }
    else {
        zclock_log ("E: invalid vocket type %d", socktype);
        exit (1);
    }
    //  Create msgpipe vocket and connect over inproc to vtxname
    self->msgpipe = zsocket_new (driver->ctx, ZMQ_PAIR);
    assert (self->msgpipe);
    zsocket_connect (self->msgpipe, "inproc://%s", vtxname);

    //  If we drop on no peerings, start routing input now
    if (self->min_peerings == 0) {
        //  Ask reactor to start monitoring vocket's msgpipe pipe
        zmq_pollitem_t poll_msgpipe = { self->msgpipe, 0, ZMQ_POLLIN, 0 };
        zloop_poller (driver->loop, &poll_msgpipe, s_route_from_app, self);
    }
    //  Store this vocket per driver so that driver can cleanly destroy
    //  all its vockets when it is destroyed.
    zlist_push (driver->vockets, self);

    //* Start transport-specific work
    //* End transport-specific work

    return self;
}

static void
vocket_destroy (vocket_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        vocket_t *self = *self_p;
        driver_t *driver = self->driver;
        free (self->vtxname);

        //  Close message msgpipe socket
        zsocket_destroy (driver->ctx, self->msgpipe);

        //  Destroy all bindings for this vocket
        zhash_destroy (&self->binding_hash);

        //  Destroy all peerings for this vocket
        zhash_destroy (&self->peering_hash);
        zlist_destroy (&self->peering_list);
        zlist_destroy (&self->live_peerings);

        //  Remove vocket from driver list of vockets
        zlist_remove (driver->vockets, self);

        free (self);
        *self_p = NULL;
    }
}

//  ---------------------------------------------------------------------
//  Constructor and destructor for binding
//  Bindings are held per vocket, indexed by peer hostname:port

static binding_t *
binding_require (vocket_t *vocket, char *address)
{
    assert (vocket);
    binding_t *self = (binding_t *) zhash_lookup (vocket->binding_hash, address);

    if (self == NULL) {
        //  Create new binding for this hostname:port address
        self = (binding_t *) zmalloc (sizeof (binding_t));

        self->vocket = vocket;
        self->driver = vocket->driver;
        self->address = strdup (address);
        //  Split port number off address
        char *port = strchr (address, ':');
        assert (port);
        *port++ = 0;

        //  Store new peering in vocket containers
        zhash_insert (self->vocket->binding_hash, address, self);
        zhash_freefn (self->vocket->binding_hash, address, binding_delete);
        zclock_log ("I: create binding to %s", self->address);

        //* Start transport-specific work
        //  Create new bound TCP socket handle
        self->handle = socket (AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (self->handle == -1)
            derp ("socket");

        //  Get sockaddr_in structure for address
        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons (atoi (port));

        //  Bind handle to specific local address, or *
        if (streq (address, "*"))
            addr.sin_addr.s_addr = htonl (INADDR_ANY);
        else
        if (inet_aton (address, &addr.sin_addr) == 0) {
            zclock_log ("E: bind failed: invalid address '%s'", address);
            binding_delete (self);
            return NULL;
        }
        if (bind (self->handle, (const struct sockaddr *) &addr, IN_ADDR_SIZE) == -1) {
            zclock_log ("E: bind failed: '%s'", strerror (errno));
            binding_delete (self);
            return NULL;
        }
        //  Ask reactor to start monitoring this binding handle
        zmq_pollitem_t poll_binding = { NULL, self->handle, ZMQ_POLLIN, 0 };
        zloop_poller (self->driver->loop, &poll_binding, s_accept_peering, vocket);
        //* End transport-specific work
    }
    return self;
}

//  Destroy binding object, when binding is removed from vocket->binding_hash

static void
binding_delete (void *argument)
{
    binding_t *self = (binding_t *) argument;
    zclock_log ("I: delete binding %s", self->address);
    //* Start transport-specific work
    zmq_pollitem_t poll_binding = { 0, self->handle };
    zloop_poller_end (self->driver->loop, &poll_binding);
    close (self->handle);
    //* End transport-specific work
    free (self->address);
    free (self);
}

//  ---------------------------------------------------------------------
//  Constructor and destructor for peering
//  Peerings are held per vocket, indexed by peer hostname:port

static peering_t *
peering_require (vocket_t *vocket, char *address, Bool outgoing)
{
    assert (vocket);
    peering_t *self = (peering_t *) zhash_lookup (vocket->peering_hash, address);

    if (self == NULL) {
        //  Create new peering for this hostname:port address
        self = (peering_t *) zmalloc (sizeof (peering_t));
        self->vocket = vocket;
        self->driver = vocket->driver;
        self->address = strdup (address);
        self->outgoing = outgoing;

        //* Start transport-specific work
        //  Translate hostname:port into sockaddr_in structure
        s_str_to_sin_addr (&self->addr, address);
        //  Ask reactor to start monitoring handle socket
        zmq_pollitem_t poll_handle = { NULL, self->handle, ZMQ_POLLIN, 0 };
        zloop_poller (self->driver->loop, &poll_handle, s_route_off_network, self);
        //* End transport-specific work

        //  Store new peering in vocket containers
        zclock_log ("I: create peering to %s", address);
        zhash_insert (self->vocket->peering_hash, address, self);
        zhash_freefn (self->vocket->peering_hash, address, peering_delete);
        zlist_append (self->vocket->peering_list, self);
        vocket->peerings++;

        //  Add peering to reactor so we monitor it
        s_monitor_peering (self->driver->loop, NULL, self);
    }
    return self;
}

//  Destroy peering object, when peering is removed from vocket->peering_hash

static void
peering_delete (void *argument)
{
    peering_t *self = (peering_t *) argument;
    zclock_log ("I: delete peering %s", self->address);
    zmsg_destroy (&self->request);
    zmsg_destroy (&self->reply);
    zlist_remove (self->vocket->peering_list, self);
    zlist_remove (self->vocket->live_peerings, self);
    zloop_timer_end (self->driver->loop, self);
    free (self->address);
    free (self);
    self->vocket->peerings--;
}

//  Send frame data to peering as formatted command. If there was a
//  network error, destroys the peering and returns -1.

static int
peering_send_msg (peering_t *self, zmsg_t *msg)
{
    //  Should queue the message and write when peering handle is ready
    //  for output...
    int rc = 0;
    rc = send (self->handle, "NULL", 4, 0);
    if (rc == -1) {
        if (s_handle_io_error (self->driver, "send") == -1)
            zhash_delete (self->vocket->peering_hash, self->address);
    }
    return rc;
}

//  Peering is now active

static void
peering_raise (peering_t *self)
{
    zclock_log ("I: bring up peering to %s", self->address);
    vocket_t *vocket = self->vocket;
    if (!self->alive) {
        self->alive = TRUE;
        self->expiry = zclock_time () + VTX_TCP_TIMEOUT;
        zlist_append (vocket->live_peerings, self);
        if (zlist_size (vocket->live_peerings) == vocket->min_peerings) {
            //  Ask reactor to start monitoring vocket's msgpipe pipe
            zmq_pollitem_t poll_msgpipe = { vocket->msgpipe, 0, ZMQ_POLLIN, 0 };
            zloop_poller (self->driver->loop, &poll_msgpipe, s_route_from_app, vocket);
        }
    }
}

//  Peering is now inactive

static void
peering_lower (peering_t *self)
{
    zclock_log ("I: take down peering to %s", self->address);
    vocket_t *vocket = self->vocket;
    if (self->alive) {
        self->alive = FALSE;
        zlist_remove (vocket->live_peerings, self);
        if (zlist_size (vocket->live_peerings) < vocket->min_peerings) {
            //  Ask reactor to stop monitoring vocket's msgpipe pipe
            zmq_pollitem_t poll_msgpipe = { vocket->msgpipe, 0, ZMQ_POLLIN, 0 };
            zloop_poller_end (self->driver->loop, &poll_msgpipe);
        }
    }
}

//  ---------------------------------------------------------------------
//  Reactor handlers

//  Handle bind/connect from caller:
//
//  [command]   BIND or CONNECT
//  [socktype]  0MQ socket type as ASCII number
//  [vtxname]   VTX name for the 0MQ socket
//  [address]   External address to bind/connect to

static int
s_driver_control (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    int rc = 0;
    driver_t *driver = (driver_t *) arg;
    zmsg_t *request = zmsg_recv (item->socket);

    char *command  = zmsg_popstr (request);
    char *socktype = zmsg_popstr (request);
    char *vtxname  = zmsg_popstr (request);
    char *address  = zmsg_popstr (request);
    zmsg_destroy (&request);

    //  Lookup vocket with this vtxname, create if necessary
    vocket_t *vocket = (vocket_t *) zlist_first (driver->vockets);
    while (vocket) {
        if (streq (vocket->vtxname, vtxname))
            break;
        vocket = (vocket_t *) zlist_next (driver->vockets);
    }
    if (!vocket)
        vocket = vocket_new (driver, atoi (socktype), vtxname);

    //  Multiple binds or connects to same address are idempotent
    if (streq (command, "BIND")) {
        if (!binding_require (vocket, address))
            rc = 1;
    }
    else
    if (streq (command, "CONNECT")) {
        if (vocket->peerings < vocket->max_peerings)
            peering_require (vocket, address, TRUE);
        else {
            zclock_log ("E: connect failed: too many peerings");
            rc = 1;
        }
    }
    else
    if (streq (command, "CLOSE"))
        vocket_destroy (&vocket);
    else {
        zclock_log ("E: invalid command: %s", command);
        rc = 1;
    }
    zstr_sendf (item->socket, "%d", rc);
    free (command);
    free (socktype);
    free (vtxname);
    free (address);
    return 0;
}

//  -------------------------------------------------------------------------
//  Input message on data pipe from application 0MQ socket

static int
s_route_from_app (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    vocket_t *vocket = (vocket_t *) arg;
    driver_t *driver = vocket->driver;

    //  It's remotely possible we just lost a peering, in which case
    //  don't take the message off the pipe, leave it for next time
    if (zlist_size (vocket->live_peerings) < vocket->min_peerings)
        return 0;

    //  Pull message frames off socket
    assert (item->socket == vocket->msgpipe);
    zmsg_t *msg = zmsg_recv (vocket->msgpipe);

    //  Route message to active peerings as appropriate
    if (vocket->routing == VTX_ROUTING_NONE) {
        zclock_log ("W: send() not allowed - dropping message");
    }
    else
    if (vocket->routing == VTX_ROUTING_REQUEST) {
        //  Find next live peering if any
        peering_t *peering = (peering_t *) zlist_pop (vocket->live_peerings);
        if (peering) {
            if (peering->request == NULL) {
                peering->request = msg;
                msg = NULL;         //  Peering now owns message
                peering_send_msg (peering, peering->request);
            }
            else
                zclock_log ("E: illegal send() without recv() from REQ socket");
            zlist_append (vocket->live_peerings, peering);
        }
        else
            zclock_log ("W: no live peerings - dropping message");
    }
    else
    if (vocket->routing == VTX_ROUTING_REPLY) {
        peering_t *peering = vocket->reply_to;
        assert (peering);
        zmsg_destroy (&peering->reply);
        peering->reply = msg;
        msg = NULL;         //  Peering now owns message
        peering_send_msg (peering, peering->reply);
    }
    else
    if (vocket->routing == VTX_ROUTING_DEALER) {
        //  Find next live peering if any
        peering_t *peering = (peering_t *) zlist_pop (vocket->live_peerings);
        if (peering) {
            zlist_append (vocket->live_peerings, peering);
            peering_send_msg (peering, msg);
        }
        else
            zclock_log ("W: no live peerings - dropping message");
    }
    else
    if (vocket->routing == VTX_ROUTING_ROUTER) {
        //  First frame is address of peering
        char *address = zmsg_popstr (msg);
        //  Parse and check scheme
        int scheme_size = strlen (driver->scheme);
        if (memcmp (address, driver->scheme, scheme_size) == 0
        &&  memcmp (address + scheme_size, "://", 3) == 0) {
            peering_t *peering = (peering_t *)
                zhash_lookup (vocket->peering_hash, address + scheme_size + 3);
            if (peering && peering->alive)
                peering_send_msg (peering, msg);
            else
                zclock_log ("W: no route to '%s' - dropping message", address);
        }
        else
            zclock_log ("E: invalid address '%s' - dropping message", address);
        free (address);
    }
    else
    if (vocket->routing == VTX_ROUTING_PUBLISH) {
        peering_t *peering = (peering_t *) zlist_first (vocket->live_peerings);
        while (peering) {
            peering_send_msg (peering, msg);
            peering = (peering_t *) zlist_next (vocket->live_peerings);
        }
    }
    else
    if (vocket->routing == VTX_ROUTING_SINGLE) {
        //  We expect a single live peering and we should not have read
        //  a message otherwise...
        peering_t *peering = (peering_t *) zlist_first (vocket->peering_list);
        assert (peering->alive);
        peering_send_msg (peering, msg);
    }
    else
        zclock_log ("E: unknown routing mechanism - dropping message");

    zmsg_destroy (&msg);
    return 0;
}


//  -------------------------------------------------------------------------
//  Accept incoming TCP connection request on binding handle
//  Creates a new peering, if successful

static int
s_accept_peering (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    vocket_t *vocket = (vocket_t *) arg;
    driver_t *driver = vocket->driver;

    struct sockaddr_in addr;        //  Peer address
    socklen_t addr_len = sizeof (addr);

    int handle = accept (item->fd, (struct sockaddr *) &addr, &addr_len);
    if (handle >= 0) {
#ifdef __WINDOWS__
        u_long noblock = 1;
        ioctlsocket (handle, FIONBIO, &noblock);
#else
        fcntl (handle, F_SETFL, O_NONBLOCK | fcntl (handle, F_GETFL, 0));
#endif
        if (vocket->peerings < vocket->max_peerings) {
            char *address = s_sin_addr_to_str (&addr);
            peering_t *peering = peering_require (vocket, address, FALSE);
            peering->handle = handle;
        }
        else
            zclock_log ("W: Max peerings reached for socket");
    }
    else
        s_handle_io_error (driver, "accept");

    return 0;
}


//  -------------------------------------------------------------------------
//  Input message on binding handle

static int
s_route_off_network (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    peering_t *peering = (peering_t *) arg;
    vocket_t *vocket = peering->vocket;
    driver_t *driver = peering->driver;

    //  Read into buffer and dump what we got
    byte buffer [VTX_TCP_BUFSIZE];
    ssize_t size = recv (item->fd, buffer, VTX_TCP_BUFSIZE, MSG_DONTWAIT);
    if (size == -1) {
        if (s_handle_io_error (driver, "recv") == -1)
            zhash_delete (vocket->peering_hash, peering->address);
        return 0;
    }
    zclock_log ("I: recv %zd bytes from %s", size, peering->address);

    //  Dump to stderr
    uint char_nbr;
    fprintf (stderr, "[%03d] ", (int) size);
    for (char_nbr = 0; char_nbr < size; char_nbr++)
        fprintf (stderr, "%02X", buffer [char_nbr]);
    fprintf (stderr, "\n");

    peering->expiry = zclock_time () + VTX_TCP_TIMEOUT;
    if (!peering->alive)
        peering_raise (peering);
    return 0;
}


//  Monitor peering for connectivity

static int
s_monitor_peering (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    peering_t *peering = (peering_t *) arg;
    int interval = VTX_TCP_RECONNECT_IVL;
    if (peering->alive) {
        int64_t time_now = zclock_time ();
        if (time_now > peering->expiry) {
            peering_lower (peering);
            if (!peering->outgoing) {
                //  Delete the peering, by removal from the vocket peering hash
                zhash_delete (peering->vocket->peering_hash, peering->address);
                interval = 0;           //  Don't reset timer
            }
        }
    }
    if (interval)
        zloop_timer (loop, interval, 1, s_monitor_peering, peering);
    return 0;
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

//  Converts a hostname:port into a sockaddr_in, returns static result
//  Asserts on badly formatted address.

static void
s_str_to_sin_addr (struct sockaddr_in *addr, char *address)
{
    memset (addr, 0, IN_ADDR_SIZE);

    //  Take copy of address, then split into hostname and port
    char *hostname = strdup (address);
    char *port = strchr (hostname, ':');
    assert (port);
    *port++ = 0;

    addr->sin_family = AF_INET;
    addr->sin_port = htons (atoi (port));

    if (!inet_aton (hostname, &addr->sin_addr))
        derp ("inet_aton");

    free (hostname);
}

//  Handle error from I/O operation, return 0 if the caller should
//  retry, -1 to abandon the operation.

static int
s_handle_io_error (driver_t *driver, char *reason)
{
#ifdef __WINDOWS__
    switch (WSAGetLastError ()) {
        case WSAEINTR:        errno = EINTR;      break;
        case WSAEBADF:        errno = EBADF;      break;
        case WSAEWOULDBLOCK:  errno = EAGAIN;     break;
        case WSAEINPROGRESS:  errno = EAGAIN;     break;
        case WSAENETDOWN:     errno = ENETDOWN;   break;
        case WSAECONNRESET:   errno = ECONNRESET; break;
        case WSAECONNABORTED: errno = EPIPE;      break;
        case WSAESHUTDOWN:    errno = ECONNRESET; break;
        case WSAEINVAL:       errno = EPIPE;      break;
        default:              errno = GetLastError ();
    }
#endif
    if (errno == EAGAIN
    ||  errno == ENETDOWN
    ||  errno == EPROTO
    ||  errno == ENOPROTOOPT
    ||  errno == EHOSTDOWN
    ||  errno == ENONET
    ||  errno == EHOSTUNREACH
    ||  errno == EOPNOTSUPP
    ||  errno == ENETUNREACH
    ||  errno == EWOULDBLOCK
    ||  errno == EINTR)
        return 0;           //  Ignore error and try again
    else
    if (errno == EPIPE
    ||  errno == ECONNRESET)
        return -1;          //  Peer closed socket, abandon
    else {
        zclock_log ("I: error '%s' on %s", strerror (errno), reason);
        return -1;
    }
}
