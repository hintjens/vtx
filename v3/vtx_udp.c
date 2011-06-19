/*  =====================================================================
    VTX - 0MQ virtual transport interface - NOM-1 / UDP driver

    Implements the VTX virtual socket interface using the NOM-1 protocol
    over UDP. This lets you use UDP as a transport for 0MQ applications.

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

#define IN_ADDR_SIZE    sizeof (struct sockaddr_in)

//  These are the NOM-1 commands
static char *s_command_name [] = {
    "ROTFL",                    //  Command rejected
    "OHAI", "OHAI-OK",          //  Request/acknowledge new peering
    "HUGZ", "HUGZ-OK",          //  Send/receive sign of life
    "NOM"                       //  Send message asynchronously
};

//  ---------------------------------------------------------------------
//  These are the objects we play with in our driver

typedef struct _driver_t driver_t;
typedef struct _vocket_t vocket_t;
typedef struct _binding_t binding_t;
typedef struct _peering_t peering_t;


//  ---------------------------------------------------------------------
//  A driver_t holds the context for one driver thread, which matches
//  one registered driver. We create a driver by calling vtx_udp_driver,
//  and the thread runs until the process is interrupted. A driver works
//  with a list of vockets, which are virtual 0MQ sockets.

struct _driver_t {
    zctx_t *ctx;                //  Own context
    zloop_t *loop;              //  zloop reactor for socket I/O
    zlist_t *vockets;           //  List of vockets per driver
    void *pipe;                 //  Control pipe to/from VTX frontend
    int64_t sends;              //  Number of messages sent
    int64_t recvs;              //  Number of messages received
    int64_t errors;             //  Number of transport errors
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
    int handle;                 //  Handle for outgoing commands
    zhash_t *binding_hash;      //  Bindings, indexed by address
    zhash_t *peering_hash;      //  Peerings, indexed by address
    zlist_t *peering_list;      //  Peerings, in simple list
    zlist_t *live_peerings;     //  Peerings that are alive
    peering_t *reply_to;        //  For reply routing
    char *reason;               //  Last error reason, if any
    int peerings;               //  Current number of peerings
    //  These properties control the vocket routing semantics
    int routing;                //  Routing mechanism
    Bool nomnom;                //  Accepts NOM commands
    int min_peerings;           //  Minimum peerings for routing
    int max_peerings;           //  Maximum allowed peerings
    //  hwm strategy
    //  filter on input messages
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
//  For NOM-1, this is includes the native UDP socket handle.

struct _binding_t {
    driver_t *driver;           //  Parent driver object
    vocket_t *vocket;           //  Parent vocket object
    char *address;              //  Local address:port bound to
    //  NOM-1 specific properties
    struct sockaddr_in addr;    //  Address as sockaddr_in
    int handle;                 //  UDP socket handle
};

//  A peering_t holds the context for a peering to another node across
//  our transport. Peerings can be outgoing (will try to reconnect if
//  lowered) or incoming (will be destroyed when lowered).
//  For NOM-1, this includes the actual UDP address to talk to,
//  and the broadcast address if this was a broadcast connection.

struct _peering_t {
    driver_t *driver;           //  Parent driver object
    vocket_t *vocket;           //  Parent vocket object
    Bool alive;                 //  Is peering raised and alive?
    Bool outgoing;              //  Connected handles?
    int64_t silent;             //  Peering goes silent at this time
    int64_t expiry;             //  Peering expires at this time
    char *address;              //  Peer address as nnn.nnn.nnn.nnn:nnnnn
    uint sequence;              //  Request/reply sequence number
    //  NOM-1 specific properties
    Bool broadcast;             //  Is peering connected to BROADCAST?
    struct sockaddr_in addr;    //  Peer address as sockaddr_in
    struct sockaddr_in bcast;   //  Broadcast address, if any
    zmsg_t *request;            //  Pending request NOM, if any
    zmsg_t *reply;              //  Last reply NOM, if any
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
    peering_send (peering_t *self, int command, byte *data, size_t size);
static int
    peering_send_msg (peering_t *self, int command, zmsg_t *msg);

//  Reactor handlers
static int
    s_driver_control (zloop_t *loop, zmq_pollitem_t *item, void *arg);
static int
    s_route_from_app (zloop_t *loop, zmq_pollitem_t *item, void *arg);
static int
    s_route_off_network (zloop_t *loop, zmq_pollitem_t *item, void *arg);
static int
    s_peering_monitor (zloop_t *loop, zmq_pollitem_t *item, void *arg);
static int
    s_resend_request (zloop_t *loop, zmq_pollitem_t *item, void *arg);

//  Utility functions
static uint32_t
    s_broadcast_addr (void);
static char *
    s_sin_addr_to_str (struct sockaddr_in *addr);
static void
    s_str_to_sin_addr (struct sockaddr_in *addr, char *address, uint32_t wildcard);
static int
    s_handle_io_error (driver_t *driver, char *reason);

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
//  Registers our protocol driver with the VTX engine

int vtx_udp_load (vtx_t *vtx)
{
    return vtx_register (vtx, VTX_UDP_SCHEME, vtx_udp_driver);
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
        zclock_log ("I: shutting down driver, %" PRId64 " errors", self->errors);
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

    //  Create UDP socket handle, used for outbound connections
    self->handle = socket (AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (self->handle == -1)
        derp ("socket");

    //  Enable broadcast mode on handle socket, always
    int broadcast_on = 1;
    if (setsockopt (self->handle, SOL_SOCKET, SO_BROADCAST,
        &broadcast_on, sizeof (int)) == -1)
        derp ("setsockopt (SO_BROADCAST)");

    //  Ask reactor to start monitoring handle socket
    zmq_pollitem_t poll_handle = { NULL, self->handle, ZMQ_POLLIN, 0 };
    zloop_poller (driver->loop, &poll_handle, s_route_off_network, self);

    //  If we drop on no peerings, start routing input now
    if (self->min_peerings == 0) {
        puts ("START ROUTING");
        //  Ask reactor to start monitoring vocket's msgpipe pipe
        zmq_pollitem_t poll_msgpipe = { self->msgpipe, 0, ZMQ_POLLIN, 0 };
        zloop_poller (driver->loop, &poll_msgpipe, s_route_from_app, self);
    }
    //  Store this vocket per driver so that driver can cleanly destroy
    //  all its vockets when it is destroyed.
    zlist_push (driver->vockets, self);
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

        //  Close handle, if we have it open
        if (self->handle) {
            zmq_pollitem_t poll_handle = { 0, self->handle };
            zloop_poller_end (driver->loop, &poll_handle);
            close (self->handle);
        }
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

        //  Create new bound UDP socket handle
        self->handle = socket (AF_INET, SOCK_DGRAM, IPPROTO_UDP);
        if (self->handle == -1)
            derp ("socket");

        //  Split port number off address
        char *port = strchr (address, ':');
        assert (port);
        *port++ = 0;

        //  Get sockaddr_in structure for address
        self->addr.sin_family = AF_INET;
        self->addr.sin_port = htons (atoi (port));

        //  Bind handle to specific local address, or *
        if (streq (address, "*"))
            self->addr.sin_addr.s_addr = htonl (INADDR_ANY);
        else
        if (inet_aton (address, &self->addr.sin_addr) == 0) {
            zclock_log ("E: bind failed: invalid address '%s'", address);
            binding_delete (self);
            return NULL;
        }
        if (bind (self->handle, (const struct sockaddr *) &self->addr, IN_ADDR_SIZE) == -1) {
            zclock_log ("E: bind failed: '%s'", strerror (errno));
            binding_delete (self);
            return NULL;
        }
        //  Store new peering in vocket containers
        zhash_insert (self->vocket->binding_hash, address, self);
        zhash_freefn (self->vocket->binding_hash, address, binding_delete);
        zclock_log ("I: create binding to %s", self->address);

        //  Ask reactor to start monitoring this binding handle
        zmq_pollitem_t poll_binding = { NULL, self->handle, ZMQ_POLLIN, 0 };
        zloop_poller (self->driver->loop, &poll_binding, s_route_off_network, vocket);
    }
    return self;
}

//  Destroy binding object, when binding is removed from vocket->binding_hash

static void
binding_delete (void *argument)
{
    binding_t *self = (binding_t *) argument;
    zclock_log ("I: delete binding %s", self->address);
    zmq_pollitem_t poll_binding = { 0, self->handle };
    zloop_poller_end (self->driver->loop, &poll_binding);
    close (self->handle);
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

        //  Translate hostname:port into sockaddr_in structure
        //  Wildcard is broadcast for outgoing, ANY for incoming
        s_str_to_sin_addr (&self->addr, address,
            outgoing? s_broadcast_addr (): INADDR_ANY);

        //  Store broadcast address if needed
        if (outgoing && *address == '*') {
            self->broadcast = TRUE;
            self->bcast = self->addr;
        }
        //  Store new peering in vocket containers
        zhash_insert (self->vocket->peering_hash, address, self);
        zhash_freefn (self->vocket->peering_hash, address, peering_delete);
        zlist_append (self->vocket->peering_list, self);
        zclock_log ("I: create peering to %s", address);

        //  Add peering to reactor so we monitor it
        s_peering_monitor (self->driver->loop, NULL, self);
        vocket->peerings++;
    }
    return self;
}

//  Destroy peering object, when peering is removed from vocket->peering_hash

static void
peering_delete (void *argument)
{
    peering_t *self = (peering_t *) argument;
    zclock_log ("I: delete peering %s", self->address);
    self->vocket->peerings--;
    zmsg_destroy (&self->request);
    zmsg_destroy (&self->reply);
    zlist_remove (self->vocket->peering_list, self);
    zlist_remove (self->vocket->live_peerings, self);
    zloop_timer_end (self->driver->loop, self);
    free (self->address);
    free (self);
}

//  Send a buffer of data to peering, prefixed by command header. If there
//  was a network error, destroys the peering and returns -1.

static int
peering_send (peering_t *self, int command, byte *data, size_t size)
{
    zclock_log ("I: send [%s:%x] - %zd bytes to %s",
        s_command_name [command], self->sequence & 15,
        size, s_sin_addr_to_str (&self->addr));

    int rc = 0;
    if ((size + VTX_UDP_HEADER) <= VTX_UDP_MSGMAX) {
        byte buffer [size + VTX_UDP_HEADER];
        buffer [0] = VTX_UDP_VERSION << 4;
        buffer [1] = (command << 4) + (self->sequence & 15);
        if (size)
            memcpy (buffer + VTX_UDP_HEADER, data, size);
        rc = sendto (self->vocket->handle,
            buffer, size + VTX_UDP_HEADER, 0,
            (const struct sockaddr *) &self->addr, IN_ADDR_SIZE);
        if (rc == 0) {
            //  Calculate when we'd need to start sending HUGZ
            self->silent = zclock_time () + VTX_UDP_TIMEOUT / 3;
            self->driver->sends++;
        }
        else {
            rc = s_handle_io_error (self->driver, "sendto");
            if (rc == -1)
                zhash_delete (self->vocket->peering_hash, self->address);
        }
    }
    else
        zclock_log ("W: over-long message, %d bytes, dropping it", size);

    return rc;
}

//  Send frame data to peering as formatted command. If there was a
//  network error, destroys the peering and returns -1.

static int
peering_send_msg (peering_t *self, int command, zmsg_t *msg)
{
    //  TODO: format all message parts into one buffer
    assert (zmsg_size (msg) == 1);
    zframe_t *frame = zmsg_first (msg);
    return peering_send (self, command, zframe_data (frame), zframe_size (frame));
}

//  Peering is now active

static void
peering_raise (peering_t *self)
{
    zclock_log ("I: bring up peering to %s", self->address);
    vocket_t *vocket = self->vocket;
    if (!self->alive) {
        self->alive = TRUE;
        self->expiry = zclock_time () + VTX_UDP_TIMEOUT;
        self->silent = zclock_time () + VTX_UDP_TIMEOUT / 3;
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
                peering->sequence++;
                peering->request = msg;
                msg = NULL;         //  Peering now owns message
                s_resend_request (vocket->driver->loop, NULL, peering);
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
        peering_send_msg (peering, VTX_UDP_NOM, peering->reply);
    }
    else
    if (vocket->routing == VTX_ROUTING_DEALER) {
        //  Find next live peering if any
        peering_t *peering = (peering_t *) zlist_pop (vocket->live_peerings);
        if (peering) {
            zlist_append (vocket->live_peerings, peering);
            peering_send_msg (peering, VTX_UDP_NOM, msg);
        }
        else
            zclock_log ("W: no live peerings - dropping message");
    }
    else
    if (vocket->routing == VTX_ROUTING_ROUTER) {
        //  First frame is address of peering
        char *address = zmsg_popstr (msg);
        //  Parse and check scheme
        if (memcmp (address, VTX_UDP_SCHEME, 3) == 0
        &&  memcmp (address + 3, "://", 3) == 0) {
            peering_t *peering = (peering_t *)
                zhash_lookup (vocket->peering_hash, address + 6);
            if (peering && peering->alive)
                peering_send_msg (peering, VTX_UDP_NOM, msg);
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
            peering_send_msg (peering, VTX_UDP_NOM, msg);
            peering = (peering_t *) zlist_next (vocket->live_peerings);
        }
    }
    else
    if (vocket->routing == VTX_ROUTING_SINGLE) {
        //  We expect a single live peering and we should not have read
        //  a message otherwise...
        peering_t *peering = (peering_t *) zlist_first (vocket->peering_list);
        assert (peering->alive);
        peering_send_msg (peering, VTX_UDP_NOM, msg);
    }
    else
        zclock_log ("E: unknown routing mechanism - dropping message");

    zmsg_destroy (&msg);
    return 0;
}


//  -------------------------------------------------------------------------
//  Input message on binding handle
//  This implements the receiver side of the UDP protocol-without-a-name
//  I'd like to implement this as a neat little finite-state machine.

static int
s_route_off_network (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    vocket_t *vocket = (vocket_t *) arg;
    driver_t *driver = vocket->driver;

    //  Buffer can hold longest valid message plus terminating null in
    //  case it's a string and we want to make it printable.
    byte buffer [VTX_UDP_MSGMAX + 1];
    struct sockaddr_in addr;
    socklen_t addr_len = IN_ADDR_SIZE;
    ssize_t size = recvfrom (item->fd, buffer, VTX_UDP_MSGMAX, 0,
                            (struct sockaddr *) &addr, &addr_len);
    if (size == -1) {
        s_handle_io_error (driver, "recvfrom");
        return 0;
    }
    driver->recvs++;

    //  Parse incoming protocol command
    int version  = buffer [0] >> 4;
    int command  = buffer [1] >> 4;
    int sequence = buffer [1] & 0xf;
    byte *body = buffer + VTX_UDP_HEADER;
    size_t body_size = size - VTX_UDP_HEADER;
    //  If body is a string, make it a valid C string
    body [body_size] = 0;

    if (randof (5) == 0) {
        zclock_log ("I: simulating UDP breakage - dropping message", version);
        return 0;
    }
    else
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
    zclock_log ("I: recv [%s:%x] - %zd bytes from %s",
        s_command_name [command], sequence & 15, body_size, address);

    //  If possible, find peering using address as peering ID
    peering_t *peering = (peering_t *) zhash_lookup (vocket->peering_hash, address);
    if (command == VTX_UDP_ROTFL)
        zclock_log ("W: got ROTFL: %s", body);
    else
    if (peering)
        //  Any input at all from a peer counts as activity
        peering->expiry = zclock_time () + VTX_UDP_TIMEOUT;
    else {
        //  OHAI command creates a new peering if needed
        if (command == VTX_UDP_OHAI) {
            peering = peering_require (vocket, address, FALSE);
            if (vocket->peerings > vocket->max_peerings) {
                char *reason = "Max peerings reached for socket";
                peering_send (peering, VTX_UDP_ROTFL,
                    (byte *) reason, strlen (reason));
                zhash_delete (vocket->peering_hash, address);
                driver->errors++;
                return 0;
            }
        }
        else
        //  OHAI-OK command uses command body as peering key so it can
        //  resolve broadcast replies (peering key will still be the
        //  broadcast address, not the actual peer address).
        if (command == VTX_UDP_OHAI_OK) {
            peering = (peering_t *) zhash_lookup (vocket->peering_hash, (char *) body);
            if (!peering) {
                zclock_log ("W: unknown peer %s - dropping message", body);
                driver->errors++;
                return 0;
            }
        }
        else {
            zclock_log ("W: unknown peer %s - dropping message", address);
            driver->errors++;
            return 0;
        }
    }

    //  Now do command-specific work
    if (command == VTX_UDP_OHAI) {
        if (peering_send (peering, VTX_UDP_OHAI_OK, body, body_size) == 0)
            peering_raise (peering);
    }
    else
    if (command == VTX_UDP_OHAI_OK) {
        if (strneq (address, (char *) body)) {
            zclock_log ("I: focus peering from %s to %s", (char *) body, address);
            int rc = zhash_rename (vocket->peering_hash, (char *) body, address);
            assert (rc == 0);
            peering->addr = addr;
            free (peering->address);
            peering->address = strdup (address);
        }
        peering_raise (peering);
    }
    else
    if (command == VTX_UDP_HUGZ)
        peering_send (peering, VTX_UDP_HUGZ_OK, NULL, 0);
    else
    if (command == VTX_UDP_NOM) {
        //  TODO: decode body into all frames as necessary
        zmsg_t *msg = zmsg_new ();
        zmsg_addmem (msg, body, body_size);
        if (vocket->routing == VTX_ROUTING_REQUEST)
            //  Clear pending request, allow another
            zmsg_destroy (&peering->request);
        else
        if (vocket->routing == VTX_ROUTING_REPLY) {
            if (peering->sequence == sequence) {
                peering_send_msg (peering, VTX_UDP_NOM, peering->reply);
                zmsg_destroy (&msg);    //  Don't pass to application
            }
            else {
                //  Track peering for eventual reply routing
                vocket->reply_to = peering;
                peering->sequence = sequence;
            }
        }
        else
        if (vocket->routing == VTX_ROUTING_ROUTER) {
            //  Send schemed identity envelope
            zmsg_pushstr (msg, "%s://%s", VTX_UDP_SCHEME, address);
            peering->sequence = sequence;
        }
        //  Now pass message onto application if required
        if (vocket->nomnom)
            zmsg_send (&msg, vocket->msgpipe);
        else {
            zclock_log ("W: unexpected NOM from %s - dropping message", address);
            driver->errors++;
        }
    }
    return 0;
}


//  Monitor peering for connectivity and send OHAIs and HUGZ as needed

static int
s_peering_monitor (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    peering_t *peering = (peering_t *) arg;
    int interval = VTX_UDP_OHAI_IVL;
    if (peering->alive) {
        int64_t time_now = zclock_time ();
        if (time_now > peering->expiry) {
            peering_lower (peering);
            //  If this was a broadcast peering, reset it to BROADCAST
            if (peering->broadcast) {
                char *address = s_sin_addr_to_str (&peering->bcast);
                zclock_log ("I: unfocus peering from %s to %s", peering->address, address);
                int rc = zhash_rename (peering->vocket->peering_hash, peering->address, address);
                assert (rc == 0);
                peering->addr = peering->bcast;
                free (peering->address);
                peering->address = strdup (address);
            }
            else
            if (!peering->outgoing) {
                //  Delete the peering, by removal from the vocket peering hash
                zhash_delete (peering->vocket->peering_hash, peering->address);
                interval = 0;           //  Don't reset timer
            }
        }
        else
        if (time_now > peering->silent) {
            if (peering_send (peering, VTX_UDP_HUGZ, NULL, 0) == 0) {
                interval = VTX_UDP_TIMEOUT / 3;
                peering->silent = zclock_time () + interval;
            }
        }
    }
    else
    if (peering->outgoing)
        peering_send (peering, VTX_UDP_OHAI,
            (byte *) peering->address, strlen (peering->address));

    if (interval)
        zloop_timer (loop, interval, 1, s_peering_monitor, peering);
    return 0;
}


//  Resend request NOM if peering is alive and no response received

static int
s_resend_request (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    peering_t *peering = (peering_t *) arg;
    if (peering->request && peering->alive) {
        peering_send_msg (peering, VTX_UDP_NOM, peering->request);
        zloop_timer (loop, VTX_UDP_RESEND_IVL, 1, s_resend_request, peering);
    }
    return 0;
}


//  Returns (last valid) broadcast address for LAN
//  On Windows we just force SO_BROADCAST, getting the interfaces
//  via win32 is too ugly to put into this code...

static uint32_t
s_broadcast_addr (void)
{
    uint32_t address = INADDR_ANY;
#if defined (__UNIX__)
    struct ifaddrs *interfaces;
    if (getifaddrs (&interfaces) == 0) {
        struct ifaddrs *interface = interfaces;
        while (interface) {
            struct sockaddr *sa = interface->ifa_broadaddr;
            if (sa && sa->sa_family == AF_INET)
                address = ntohl (((struct sockaddr_in *) sa)->sin_addr.s_addr);
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

//  Converts a hostname:port into a sockaddr_in, returns static result
//  If hostname is '*', uses the wildcard value for the host address.
//  Asserts on badly formatted address.

static void
s_str_to_sin_addr (struct sockaddr_in *addr, char *address, uint32_t wildcard)
{
    memset (addr, 0, IN_ADDR_SIZE);

    //  Take copy of address, then split into hostname and port
    char *hostname = strdup (address);
    char *port = strchr (hostname, ':');
    assert (port);
    *port++ = 0;

    addr->sin_family = AF_INET;
    addr->sin_port = htons (atoi (port));

    if (*hostname == '*') {
        assert (streq (hostname, "*"));
        addr->sin_addr.s_addr = htonl (wildcard);
    }
    else
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
    if (errno == EINTR
    ||  errno == EWOULDBLOCK
    ||  errno == EAGAIN
    ||  errno == ENETUNREACH
    ||  errno == ENETDOWN)
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
