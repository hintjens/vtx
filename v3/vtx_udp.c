/*  =====================================================================
    VTX - 0MQ virtual transport interface - NOM-1 / UDP driver

    Implements the VTX virtual socket interface using the NOM-1 protocol
    over UDP.

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

static struct {
    char *name;                 //  Command name
} s_command [] = {
    { "ROTFL" },
    { "OHAI" },
    { "OHAI-OK" },
    { "HUGZ" },
    { "HUGZ-OK" },
    { "ICANHAZ" },
    { "ICANHAZ-OK" },
    { "NOM" }
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
    int outward;                //  Handle for outgoing commands
    zlist_t *binding_list;      //  Bindings for incoming commands
    zhash_t *peering_hash;      //  Peerings, indexed by address
    zlist_t *peering_list;      //  Peerings that are alive
    //  These properties control the vocket routing semantics
    int routing;                //  Routing mechanism
    int flow_control;           //  Flow control mechanism
    int max_peerings;           //  Maximum allowed peerings
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
    Bool outgoing;              //  Connected outwards?
    int64_t expiry;             //  Link expires at this time
    char *address;              //  Peer address as nnn.nnn.nnn.nnn:nnnnn
    //  NOM-1 specific properties
    Bool broadcast;             //  Is peering connected to BROADCAST?
    struct sockaddr_in addr;    //  Peer address as sockaddr_in
    struct sockaddr_in bcast;   //  Broadcast address, if any
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
    binding_new (vocket_t *vocket, char *address);
static void
    binding_destroy (binding_t **self_p);
static peering_t *
    peering_require (vocket_t *vocket, struct sockaddr_in *addr);
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
    self->binding_list = zlist_new ();
    self->peering_hash = zhash_new ();
    self->peering_list = zlist_new ();

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
            self->max_peerings = 1;
            break;
        default:
            zclock_log ("E: invalid vocket type %d", socktype);
            exit (1);
    }
    //  Create msgpipe vocket and connect over inproc to vtxname
    self->msgpipe = zsocket_new (driver->ctx, ZMQ_PAIR);
    assert (self->msgpipe);
    zsocket_connect (self->msgpipe, "inproc://%s", vtxname);

    //  Create outward vocket, used for outbound connections
    self->outward = socket (AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (self->outward == -1)
        derp ("socket");

    //  Enable broadcast mode on outward socket, always
    int broadcast_on = 1;
    if (setsockopt (self->outward, SOL_SOCKET, SO_BROADCAST,
        &broadcast_on, sizeof (int)) == -1)
        derp ("setsockopt (SO_BROADCAST)");

    //  Ask reactor to start monitoring outward socket
    zmq_pollitem_t poll_outward = { NULL, self->outward, ZMQ_POLLIN, 0 };
    zloop_poller (driver->loop, &poll_outward, s_external_input, self);

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

        //  Destroy all bindings
        binding_t *binding = (binding_t *) zlist_pop (self->binding_list);
        while (binding) {
            binding_destroy (&binding);
            binding = (binding_t *) zlist_pop (self->binding_list);
        }
        zlist_destroy (&self->binding_list);

        //  Close outward handle, if we have it open
        if (self->outward) {
            zmq_pollitem_t poll_outward = { 0, self->outward };
            zloop_cancel (driver->loop, &poll_outward);
            close (self->outward);
        }
        //  Destroy all peerings for this vocket
        zhash_destroy (&self->peering_hash);
        zlist_destroy (&self->peering_list);
        free (self);
        *self_p = NULL;
    }
}

//  ---------------------------------------------------------------------
//  Constructor and destructor for binding

static binding_t *
binding_new (vocket_t *vocket, char *address)
{
    assert (vocket);
    binding_t *self = (binding_t *) zmalloc (sizeof (binding_t));

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

    //  Ask reactor to start monitoring this binding handle
    zmq_pollitem_t poll_binding = { NULL, self->handle, ZMQ_POLLIN, 0 };
    zloop_poller (self->driver->loop, &poll_binding, s_external_input, vocket);

    //  Store this binding per vocket so that vocket can cleanly destroy
    //  all its bindings when it is destroyed.
    zlist_append (self->vocket->binding_list, self);
    return self;
}

static void
binding_destroy (binding_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        binding_t *self = *self_p;
        free (self->address);

        zmq_pollitem_t poll_binding = { 0, self->handle };
        zloop_cancel (self->driver->loop, &poll_binding);
        close (self->handle);

        free (self);
        *self_p = NULL;
    }
}

//  ---------------------------------------------------------------------
//  Constructor and destructor for peering
//  Links are held per vocket, indexed by peer hostname:port

static peering_t *
peering_require (vocket_t *vocket, struct sockaddr_in *addr)
{
    assert (vocket);

    char *address = s_sin_addr_to_str (addr);
    peering_t *self = (peering_t *) zhash_lookup (vocket->peering_hash, address);

    if (self == NULL) {
        self = (peering_t *) zmalloc (sizeof (peering_t));
        self->address = strdup (address);
        self->vocket = vocket;
        self->driver = vocket->driver;
        memcpy (&self->addr, addr, IN_ADDR_SIZE);
        zhash_insert (self->vocket->peering_hash, address, self);
        zhash_freefn (self->vocket->peering_hash, address, peering_delete);
        s_monitor_peering (self->driver->loop, NULL, self);
        zclock_log ("I: create peering=%p - %s", self, address);
    }
    return self;
}

//  Destroy peering object, when peering is removed from vocket->peering_hash

static void
peering_delete (void *argument)
{
    peering_t *self = (peering_t *) argument;
    zclock_log ("I: delete peering=%p - %s:%d", self,
        inet_ntoa (self->addr.sin_addr), ntohs (self->addr.sin_port));
    free (self->address);
    free (self);
}

//  Send a buffer of data to peering, prefixed by command header

static void
peering_send (peering_t *self, int command, byte *data, size_t size)
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
        int rc = sendto (self->vocket->outward,
            buffer, size + VTX_UDP_HEADER, 0,
            (const struct sockaddr *) &self->addr, IN_ADDR_SIZE);
        if (rc == -1)
            derp ("sendto");

        self->driver->sends++;
    }
    else
        zclock_log ("W: over-long message, %d bytes, dropping it", size);
}

//  Link is now active

static void
peering_raise (peering_t *self)
{
    zclock_log ("I: bring up peering=%p", self);
    if (!self->alive) {
        self->alive = TRUE;
        self->expiry = zclock_time () + VTX_UDP_LINKTTL;
        zlist_append (self->vocket->peering_list, self);
        if (zlist_size (self->vocket->peering_list) == 1) {
            //  Ask reactor to start monitoring vocket's msgpipe pipe
            zmq_pollitem_t poll_msgpipe = { self->vocket->msgpipe, 0, ZMQ_POLLIN, 0 };
            zloop_poller (self->driver->loop, &poll_msgpipe, s_internal_input, self->vocket);
        }
    }
}

//  Link is now inactive

static void
peering_lower (peering_t *self)
{
    zclock_log ("I: take down peering=%p", self);
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


    if (streq (command, "BIND")) {
        binding_t *binding = binding_new (vocket, address);
    }
    else
    if (streq (command, "CONNECT")) {
        //  Connect to specific remote address, or *
        //  Split port number off address
        char *port = strchr (address, ':');
        assert (port);
        *port++ = 0;
        struct sockaddr_in addr = { 0 };
        addr.sin_family = AF_INET;
        addr.sin_port = htons (atoi (port));

        char *addr_to_use = address;
        if (streq (address, "*"))
            addr_to_use = s_broadcast_addr ();
        if (inet_aton (addr_to_use, &addr.sin_addr) == 0)
            derp ("inet_aton");

        //  For each outgoing connection, we create a peering object
        peering_t *peering = peering_require (vocket, &addr);
        peering->outgoing = TRUE;
        if (streq (address, "*")) {
            peering->broadcast = TRUE;
            memcpy (&peering->bcast, &addr, IN_ADDR_SIZE);
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
    if (vocket->routing == VTX_ROUTING_NONE)
        zclock_log ("W: send() not allowed - dropping message");
    else
    if (vocket->routing == VTX_ROUTING_REPLY)
        zclock_log ("W: reply routing not implemented yet - dropping message");
    else
    if (vocket->routing == VTX_ROUTING_ROTATE) {
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
    if (vocket->routing == VTX_ROUTING_CCEACH) {
        peering_t *peering = (peering_t *) zlist_first (vocket->peering_list);
        while (peering) {
            peering_send (peering, VTX_UDP_NOM,
                       zframe_data (frame), zframe_size (frame));
            peering = (peering_t *) zlist_next (vocket->peering_list);
        }
    }
    else
        zclock_log ("E: unknown routing mechanism - dropping message");

    zframe_destroy (&frame);
    return 0;
}


//  -------------------------------------------------------------------------
//  Input message on binding handle
//  This implements the receiver side of the UDP protocol-without-a-name

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

    if (command == VTX_UDP_ROTFL) {
        //  What are the errors, and how do we handle them?
        peering_t *peering = (peering_t *) zhash_lookup (vocket->peering_hash, address);
        if (!peering) {
            zclock_log ("W: unknown peer - dropping message");
            driver->errors++;
            return 0;
        }
    }
    else
    if (command == VTX_UDP_OHAI) {
        //  Create new peering if this peer isn't known to us
        peering_t *peering = peering_require (vocket, &addr);
        peering_send (peering, VTX_UDP_OHAI_OK, body, body_size);
        peering_raise (peering);
    }
    else
    if (command == VTX_UDP_OHAI_OK) {
        //  Command body has address we asked to connect to
        buffer [size] = 0;
        peering_t *peering = (peering_t *) zhash_lookup (vocket->peering_hash, (char *) body);
        if (peering) {
            if (strneq (address, (char *) body)) {
                zclock_log ("I: rename peering from %s to %s", (char *) body, address);
                int rc = zhash_rename (vocket->peering_hash, (char *) body, address);
                assert (rc == 0);
                memcpy (&peering->addr, &addr, IN_ADDR_SIZE);
                free (peering->address);
                peering->address = strdup (address);
            }
            peering_raise (peering);
        }
        else {
            zclock_log ("W: no such peering '%s' - dropping message", body);
            driver->errors++;
            return 0;
        }
    }
    else
    if (command == VTX_UDP_HUGZ) {
        //  If we don't know this peer, don't create a new peering
        peering_t *peering = (peering_t *) zhash_lookup (vocket->peering_hash, address);
        if (peering)
            peering_send (peering, VTX_UDP_HUGZ_OK, NULL, 0);
        else {
            zclock_log ("W: unknown peer - dropping message");
            driver->errors++;
            return 0;
        }
    }
    else
    if (command == VTX_UDP_HUGZ_OK) {
        //  If we don't know this peer, don't create a new peering
        peering_t *peering = (peering_t *) zhash_lookup (vocket->peering_hash, address);
        if (peering)
            peering->expiry = zclock_time () + VTX_UDP_LINKTTL;
        else {
            zclock_log ("W: unknown peer - dropping message");
            driver->errors++;
            return 0;
        }
    }
    else
    if (command == VTX_UDP_ICANHAZ) {
    }
    else
    if (command == VTX_UDP_ICANHAZ_OK) {
    }
    else
    if (command == VTX_UDP_NOM) {
        //  Accept input only from known and connected peers
        peering_t *peering = (peering_t *) zhash_lookup (vocket->peering_hash, address);
        if (peering) {
            zframe_t *frame = zframe_new (body, body_size);
            zframe_send (&frame, vocket->msgpipe, 0);
        }
        else {
            zclock_log ("W: unknown peer - dropping message");
            driver->errors++;
            return 0;
        }
    }
    return 0;
}


//  Monitor peering for connectivity
//  If peering is alive, send PING, if it's pending send CONNECT
static int
s_monitor_peering (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    peering_t *peering = (peering_t *) arg;
    int interval = 1000;        //  Unless otherwise specified
    if (peering->alive) {
        if (zclock_time () > peering->expiry) {
            peering_lower (peering);
            //  If this was a broadcast peering, reset it to BROADCAST
            if (peering->broadcast) {
                char *address = s_sin_addr_to_str (&peering->bcast);
                zclock_log ("I: rename peering from %s to %s", peering->address, address);
                int rc = zhash_rename (peering->vocket->peering_hash, peering->address, address);
                assert (rc == 0);
                memcpy (&peering->addr, &peering->bcast, IN_ADDR_SIZE);
                free (peering->address);
                peering->address = strdup (address);
            }
            else
            if (!peering->outgoing) {
                zhash_delete (peering->vocket->peering_hash, peering->address);
                interval = 0;           //  Don't reset timer
            }
        }
        else {
            peering->expiry = zclock_time () + VTX_UDP_LINKTTL;
            peering_send (peering, VTX_UDP_HUGZ, NULL, 0);
            interval = VTX_UDP_LINKTTL - VTX_UDP_LATENCY;
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
