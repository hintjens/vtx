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
    "ICANHAZ", "ICANHAZ-OK",    //  Send request/reply synchronously
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
    zlist_t *peering_list;      //  Peerings that are alive
    peering_t *reply_to;        //  For reply routing
    int peerings;               //  Current number of peerings
    //  These properties control the vocket routing semantics
    int routing;                //  Routing mechanism
    Bool nomnom;                //  Accepts NOM commands
    int max_peerings;           //  Maximum allowed peerings
    //  hwm strategy
    //  filter on input messages
};

//  This maps 0MQ socket types to the VTX emulation
static struct {
    int socktype;
    int routing;
    Bool nomnom;
    int max_peerings;
} s_vocket_config [] = {
    { ZMQ_REQ,    VTX_ROUTING_REQUEST, FALSE, VTX_MAX_PEERINGS },
    { ZMQ_REP,    VTX_ROUTING_REPLY,   FALSE, VTX_MAX_PEERINGS },
    { ZMQ_ROUTER, VTX_ROUTING_ROUTER,  TRUE,  VTX_MAX_PEERINGS },
    { ZMQ_DEALER, VTX_ROUTING_DEALER,  TRUE,  VTX_MAX_PEERINGS },
    { ZMQ_PUB,    VTX_ROUTING_PUBLISH, FALSE, VTX_MAX_PEERINGS },
    { ZMQ_SUB,    VTX_ROUTING_NONE,    TRUE,  VTX_MAX_PEERINGS },
    { ZMQ_PUSH,   VTX_ROUTING_DEALER,  FALSE, VTX_MAX_PEERINGS },
    { ZMQ_PULL,   VTX_ROUTING_NONE,    TRUE,  VTX_MAX_PEERINGS },
    { ZMQ_PAIR,   VTX_ROUTING_SINGLE,  TRUE,  1 },
    { 0, -1 }
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
    zframe_t *request;          //  Pending ICANHAZ request, if any
    zframe_t *reply;            //  Last ICANHAZ-OK reply, if any
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
static void
    peering_send (peering_t *self, int command, byte *data, size_t size);

//  Reactor handlers
static int
    s_driver_control (zloop_t *loop, zmq_pollitem_t *item, void *arg);
static int
    s_internal_input (zloop_t *loop, zmq_pollitem_t *item, void *arg);
static int
    s_external_input (zloop_t *loop, zmq_pollitem_t *item, void *arg);
static int
    s_monitor_peering (zloop_t *loop, zmq_pollitem_t *item, void *arg);

//  Utility functions
static uint32_t
    s_broadcast_addr (void);
static char *
    s_sin_addr_to_str (struct sockaddr_in *addr);
static void
    s_str_to_sin_addr (struct sockaddr_in *addr, char *address, uint32_t wildcard);


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

    uint index;
    for (index = 0; index < tblsize (s_vocket_config); index++)
        if (socktype == s_vocket_config [index].socktype)
            break;

    if (index < tblsize (s_vocket_config)) {
        self->routing = s_vocket_config [index].routing;
        self->nomnom = s_vocket_config [index].nomnom;
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
    zloop_poller (driver->loop, &poll_handle, s_external_input, self);

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

        //  Close handle handle, if we have it open
        if (self->handle) {
            zmq_pollitem_t poll_handle = { 0, self->handle };
            zloop_cancel (driver->loop, &poll_handle);
            close (self->handle);
        }
        //  Destroy all bindings for this vocket
        zhash_destroy (&self->binding_hash);
        //  Destroy all peerings for this vocket
        zhash_destroy (&self->peering_hash);
        zlist_destroy (&self->peering_list);
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
        if (inet_aton (address, &self->addr.sin_addr) == 0)
            derp ("inet_aton");
        if (bind (self->handle, (const struct sockaddr *) &self->addr, IN_ADDR_SIZE) == -1)
            derp ("bind");

        //  Store new peering in vocket containers
        zhash_insert (self->vocket->binding_hash, address, self);
        zhash_freefn (self->vocket->binding_hash, address, binding_delete);
        zclock_log ("I: create binding to %s", self->address);

        //  Ask reactor to start monitoring this binding handle
        zmq_pollitem_t poll_binding = { NULL, self->handle, ZMQ_POLLIN, 0 };
        zloop_poller (self->driver->loop, &poll_binding, s_external_input, vocket);
    }
    return self;
}

//  Destroy binding object, when binding is removed from vocket->binding_hash

static void
binding_delete (void *argument)
{
    binding_t *self = (binding_t *) argument;
    zclock_log ("I: delete binding to %s", self->address);
    zmq_pollitem_t poll_binding = { 0, self->handle };
    zloop_cancel (self->driver->loop, &poll_binding);
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

    if (self == NULL
    &&  vocket->peerings < vocket->max_peerings) {
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
        zclock_log ("I: create peering to %s", address);

        //  Add peering to reactor so we monitor it
        s_monitor_peering (self->driver->loop, NULL, self);
        vocket->peerings++;
    }
    return self;
}

//  Destroy peering object, when peering is removed from vocket->peering_hash

static void
peering_delete (void *argument)
{
    peering_t *self = (peering_t *) argument;
    zclock_log ("I: delete peering to %s", self->address);
    self->vocket->peerings--;
    zframe_destroy (&self->request);
    zframe_destroy (&self->reply);
    free (self->address);
    free (self);
}

//  Send a buffer of data to peering, prefixed by command header

static void
peering_send (peering_t *self, int command, byte *data, size_t size)
{
    zclock_log ("I: send [%s:%x] - %zd bytes to %s",
        s_command_name [command], self->sequence & 15,
        size, s_sin_addr_to_str (&self->addr));

    if ((size + VTX_UDP_HEADER) <= VTX_UDP_MSGMAX) {
        byte buffer [size + VTX_UDP_HEADER];
        buffer [0] = VTX_UDP_VERSION << 4;
        buffer [1] = (command << 4) + (self->sequence & 15);
        if (size)
            memcpy (buffer + VTX_UDP_HEADER, data, size);
        int rc = sendto (self->vocket->handle,
            buffer, size + VTX_UDP_HEADER, 0,
            (const struct sockaddr *) &self->addr, IN_ADDR_SIZE);
        if (rc == -1)
            derp ("sendto");

        //  Calculate when we'd need to start sending HUGZ
        self->silent = zclock_time () + VTX_UDP_TIMEOUT / 3;
        self->driver->sends++;
    }
    else
        zclock_log ("W: over-long message, %d bytes, dropping it", size);
}

//  Peering is now active

static void
peering_raise (peering_t *self)
{
    zclock_log ("I: bring up peering to %s", self->address);
    if (!self->alive) {
        self->alive = TRUE;
        self->expiry = zclock_time () + VTX_UDP_TIMEOUT;
        zlist_append (self->vocket->peering_list, self);
        if (zlist_size (self->vocket->peering_list) == 1) {
            //  Ask reactor to start monitoring vocket's msgpipe pipe
            zmq_pollitem_t poll_msgpipe = { self->vocket->msgpipe, 0, ZMQ_POLLIN, 0 };
            zloop_poller (self->driver->loop, &poll_msgpipe, s_internal_input, self->vocket);
        }
    }
}

//  Peering is now inactive

static void
peering_lower (peering_t *self)
{
    zclock_log ("I: take down peering to %s", self->address);
    if (self->alive) {
        self->alive = FALSE;
        zlist_remove (self->vocket->peering_list, self);
        if (zlist_size (self->vocket->peering_list) == 0) {
            //  Ask reactor to start monitoring vocket's msgpipe pipe
            zmq_pollitem_t poll_msgpipe = { self->vocket->msgpipe, 0, ZMQ_POLLIN, 0 };
            zloop_cancel (self->driver->loop, &poll_msgpipe);
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
    if (streq (command, "BIND"))
        binding_require (vocket, address);
    else
    if (streq (command, "CONNECT"))
        peering_require (vocket, address, TRUE);
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
s_internal_input (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    vocket_t *vocket = (vocket_t *) arg;

    //  Handle only single-part messages for now
    //  We'll need to handle multipart in order to do REQ/REP routing
    assert (item->socket == vocket->msgpipe);
    zframe_t *frame = zframe_recv (vocket->msgpipe);
    assert (!zframe_more (frame));
    assert (zframe_size (frame) <= VTX_UDP_MSGMAX);

    //  Route message to active peerings as appropriate
    if (vocket->routing == VTX_ROUTING_NONE) {
        zclock_log ("W: send() not allowed - dropping message");
    }
    else
    if (vocket->routing == VTX_ROUTING_REQUEST) {
        //  Find next live peering if any
        peering_t *peering = (peering_t *) zlist_pop (vocket->peering_list);
        if (peering) {
            if (peering->request == NULL) {
                peering->sequence++;
                peering_send (peering, VTX_UDP_ICANHAZ,
                    zframe_data (frame), zframe_size (frame));
                assert (peering->reply == NULL);
                peering->request = frame;
                frame = NULL;       //  Don't destroy frame
            }
            else
                zclock_log ("E: illegal send() without recv() from REQ socket");
            zlist_append (vocket->peering_list, peering);
        }
        else
            zclock_log ("W: no live peerings - dropping message");
    }
    else
    if (vocket->routing == VTX_ROUTING_REPLY) {
        peering_t *peering = vocket->reply_to;
        assert (peering);
        peering_send (peering, VTX_UDP_ICANHAZ_OK,
            zframe_data (frame), zframe_size (frame));
        zframe_destroy (&peering->reply);
        peering->reply = frame;
        frame = NULL;       //  Don't destroy frame
    }
    else
    if (vocket->routing == VTX_ROUTING_DEALER) {
        //  Find next live peering if any
        peering_t *peering = (peering_t *) zlist_pop (vocket->peering_list);
        if (peering) {
            peering_send (peering, VTX_UDP_NOM,
                zframe_data (frame), zframe_size (frame));
            zlist_append (vocket->peering_list, peering);
        }
        else
            zclock_log ("W: no live peerings - dropping message");
    }
    else
    if (vocket->routing == VTX_ROUTING_ROUTER) {
        zclock_log ("W: ROUTER not implemented yet - dropping message");
    }
    else
    if (vocket->routing == VTX_ROUTING_PUBLISH) {
        peering_t *peering = (peering_t *) zlist_first (vocket->peering_list);
        while (peering) {
            peering_send (peering, VTX_UDP_NOM,
                zframe_data (frame), zframe_size (frame));
            peering = (peering_t *) zlist_next (vocket->peering_list);
        }
    }
    else
    if (vocket->routing == VTX_ROUTING_SINGLE) {
        zclock_log ("W: SINGLE not implemented yet - dropping message");
    }
    else
        zclock_log ("E: unknown routing mechanism - dropping message");

    zframe_destroy (&frame);
    return 0;
}


//  -------------------------------------------------------------------------
//  Input message on binding handle
//  This implements the receiver side of the UDP protocol-without-a-name
//  I'd like to implement this as a neat little finite-state machine.

static int
s_external_input (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    vocket_t *vocket = (vocket_t *) arg;
    driver_t *driver = vocket->driver;

    byte buffer [VTX_UDP_MSGMAX];
    struct sockaddr_in addr;
    socklen_t addr_len = IN_ADDR_SIZE;
    ssize_t size = recvfrom (item->fd, buffer, VTX_UDP_MSGMAX, 0,
                            (struct sockaddr *) &addr, &addr_len);
    if (size == -1)
        derp ("recvfrom");
    driver->recvs++;

    //  Parse incoming protocol command
    int version  = buffer [0] >> 4;
    int command  = buffer [1] >> 4;
    int sequence = buffer [1] & 0xf;
    byte *body = buffer + VTX_UDP_HEADER;
    size_t body_size = size - VTX_UDP_HEADER;

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
    if (peering)
        //  Any input at all from a peer counts as activity
        peering->expiry = zclock_time () + VTX_UDP_TIMEOUT;
    else {
        //  OHAI command creates a new peering if needed
        if (command == VTX_UDP_OHAI) {
            peering = peering_require (vocket, address, FALSE);
            if (!peering) {
                zclock_log ("W: could not create new peering");
                driver->errors++;
                return 0;
            }
        }
        else
        //  OHAI-OK command uses command body as peering key so it can
        //  resolve broadcast replies (peering key will still be the
        //  broadcast address, not the actual peer address).
        if (command == VTX_UDP_OHAI_OK) {
            body [body_size] = 0;
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
    if (command == VTX_UDP_ROTFL) {
        //  What are the errors, and how do we handle them?
    }
    else
    if (command == VTX_UDP_OHAI) {
        peering_send (peering, VTX_UDP_OHAI_OK, body, body_size);
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
    if (command == VTX_UDP_HUGZ) {
        peering_send (peering, VTX_UDP_HUGZ_OK, NULL, 0);
    }
    else
    if (command == VTX_UDP_HUGZ_OK) {
        //  We don't need to do anything more
    }
    else
    if (command == VTX_UDP_ICANHAZ) {
        if (vocket->routing == VTX_ROUTING_REPLY) {
            //  should fail when icanhaz is resent
            //  then, resend ICANHAZ_OK
            assert (peering->sequence != sequence);
            //  We want to detect duplicate requests
            peering->sequence = sequence;
            //  Track peering for eventual reply routing
            vocket->reply_to = peering;
            //  Pass message on to application
            zframe_t *frame = zframe_new (body, body_size);
            zframe_send (&frame, vocket->msgpipe, 0);
        }
        else {
            zclock_log ("W: unexpected ICANHAZ from %s - dropping message", address);
            driver->errors++;
        }
    }
    else
    if (command == VTX_UDP_ICANHAZ_OK) {
        if (vocket->routing == VTX_ROUTING_REQUEST) {
            //  Clear reply routing, allow another request
            vocket->reply_to = NULL;
            zframe_destroy (&peering->request);
            //  Pass message on to application
            zframe_t *frame = zframe_new (body, body_size);
            zframe_send (&frame, vocket->msgpipe, 0);
        }
        else {
            zclock_log ("W: unexpected ICANHAZ-OK from %s - dropping message", address);
            driver->errors++;
        }
    }
    else
    if (command == VTX_UDP_NOM) {
        if (vocket->nomnom) {
            //  Pass message on to application
            zframe_t *frame = zframe_new (body, body_size);
            zframe_send (&frame, vocket->msgpipe, 0);
        }
        else {
            zclock_log ("W: unexpected NOM from %s - dropping message", address);
            driver->errors++;
        }
    }
    return 0;
}


//  Monitor peering for connectivity
//  If peering is alive, send HUGZ, if it's pending send OHAI
//
static int
s_monitor_peering (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    peering_t *peering = (peering_t *) arg;
    int interval = 1000;        //  1000 msecs unless otherwise specified
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
            peering_send (peering, VTX_UDP_HUGZ, NULL, 0);
            interval = VTX_UDP_TIMEOUT / 3;
            peering->silent = zclock_time () + interval;
        }
    }
    else
    if (peering->outgoing)
        peering_send (peering, VTX_UDP_OHAI,
            (byte *) peering->address, strlen (peering->address));

    if (interval)
        zloop_timer (loop, interval, 1, s_monitor_peering, peering);
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
