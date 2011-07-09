/*  =====================================================================
    VTX - 0MQ virtual transport interface - NOM-1 / UDP driver

    Implements the VTX virtual socket interface using the NOM-1 protocol
    over UDP. This lets you use UDP as a transport for 0MQ applications.

        NOM-1           = open-peering *use-peering

        open-peering    = C:OHAI ( S:OHAI-OK / S:ROTFL )

        use-peering     = C:OHAI ( S:OHAI-OK / S:ROTFL )
                        / C:HUGZ S:HUGZ-OK
                        / S:HUGZ C:HUGZ-OK
                        / C:NOM
                        / S:NOM

        ROTFL           = version flags %b0000 %b0000 reason-text
        version         = %b0001
        flags           = %b000 resend-flag
        resend-flag     = 1*BIT
        reason-text     = *VCHAR

        OHAI            = version flags %b0001 %b0000 address
        address         = scheme "://" ( broadcast / hostname / hostnumber )
                          ":" port
        scheme          = "udp"
        broadcast       = "*"
        hostname        = label *( "." label )
        label           = 1*( %x61-7A / DIGIT / "-" )
        hostnumber      = 1*DIGIT "." 1*DIGIT "." 1*DIGIT "." 1*DIGIT
        port            = 1*DIGIT

        OHAI-OK         = version flags %b0010 %b0000 address

        HUGZ            = version flags %b0011 %b0000
        HUGZ-OK         = version flags %b0100 %b0000

        NOM             = version flags %b0111 sequence zmq-payload
        sequence        = 4BIT          ; Request sequencing
        zmq-payload     = 1*zmq-frame
        zmq-frame       = tiny-frame / short-frame / long-frame
        tiny-frame      = 1OCTET frame-body
        short-frame     = %xFE 2OCTET frame-body
        long-frame      = %xFF 4OCTET frame-body
        frame-body      = *OCTET

    The UDP driver is not high-speed. Currently it uses a single socket
    for all output (vocket->handle) and blocks when the socket is busy.
    For a faster model, create a handle for each peering, and poll for
    OUTPUT on the peering handle, and then queue outgoing messages per
    peering. This approaches the design of the TCP driver.

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
#define VOCKET_STATS    0
#undef  VOCKET_STATS

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
    char *scheme;               //  Driver scheme
    zloop_t *loop;              //  zloop reactor for socket I/O
    zlist_t *vockets;           //  List of vockets per driver
    void *pipe;                 //  Control pipe to/from VTX frontend
    int64_t errors;             //  Number of transport errors
    Bool verbose;               //  Trace activity?
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
    uint peerings;              //  Current number of peerings
    //  Vocket metadata, available via getmeta call
    char sender [16];           //  Address of last message sender
    //  These properties control the vocket routing semantics
    uint routing;               //  Routing mechanism
    Bool nomnom;                //  Accepts incoming messages
    uint min_peerings;          //  Minimum peerings for routing
    uint max_peerings;          //  Maximum allowed peerings
    //  hwm strategy
    //  filter on input messages
    //  NOM-1 specific properties
    int handle;                 //  Handle for outgoing commands
    //  Statistics and reporting
    int socktype;               //  0MQ socket type
    uint outgoing;              //  Messages sent
    uint incoming;              //  Messages received
    uint outpiped;              //  Messages sent from pipe
    uint inpiped;               //  Messages sent to pipe
    uint dropped;               //  Incoming messages dropped
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
    Bool exception;             //  Binding could not be initialized
    //  NOM-1 specific properties
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
    char *address;              //  Peer address as nnn.nnn.nnn.nnn:nnnnn
    Bool exception;             //  Peering could not be initialized
    //  NOM-1 specific properties
    int64_t expiry;             //  Peering expires at this time
    Bool broadcast;             //  Is peering connected to BROADCAST?
    int64_t silent;             //  Peering goes silent at this time
    struct sockaddr_in addr;    //  Peer address as sockaddr_in
    struct sockaddr_in bcast;   //  Broadcast address, if any
    zmsg_t *request;            //  Pending request NOM, if any
    zmsg_t *reply;              //  Last reply NOM, if any
    uint sendseq;               //  Request sequence number
    uint recvseq;               //  Reply sequence number
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
    peering_destroy (peering_t **self_p);
static void
    peering_delete (void *argument);
static int
    peering_send_msg (peering_t *self, zmsg_t *msg, int flags);
static int
    peering_send (peering_t *self, int command, byte *data, size_t size, int flags);
static void
    peering_raise (peering_t *self);
static void
    peering_lower (peering_t *self);

//  Reactor handlers
static int
    s_driver_control (zloop_t *loop, zmq_pollitem_t *item, void *arg);
static int
    s_vocket_input (zloop_t *loop, zmq_pollitem_t *item, void *arg);
static int
    s_binding_input (zloop_t *loop, zmq_pollitem_t *item, void *arg);
static int
    s_peering_monitor (zloop_t *loop, zmq_pollitem_t *item, void *arg);
static int
    s_resend_timer (zloop_t *loop, zmq_pollitem_t *item, void *arg);

//  Utility functions
static uint32_t
    s_broadcast_addr (void);
static char *
    s_sin_addr_to_str (struct sockaddr_in *addr);
static int
    s_str_to_sin_addr (struct sockaddr_in *addr, char *address, uint32_t wildcard);
static void
    s_close_handle (int handle, driver_t *driver);
static int
    s_handle_io_error (char *reason);

//  ---------------------------------------------------------------------
//  Main driver thread is minimal, all work is done by reactor

void vtx_udp_driver (void *args, zctx_t *ctx, void *pipe)
{
    //  Create driver instance
    driver_t *driver = driver_new (ctx, pipe);
    driver->verbose = atoi (zstr_recv (pipe));
    zloop_set_verbose (driver->loop, driver->verbose);
    //  Run reactor until we exit from failure or interrupt
    zloop_start (driver->loop);
    //  Destroy driver instance
    driver_destroy (&driver);
}

//  ---------------------------------------------------------------------
//  Registers our protocol driver with the VTX engine

int vtx_udp_load (vtx_t *vtx, Bool verbose)
{
    return vtx_register (vtx, VTX_UDP_SCHEME, vtx_udp_driver, verbose);
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
    self->scheme = VTX_UDP_SCHEME;

    //  Reactor starts by monitoring the driver control pipe
    zmq_pollitem_t item = { self->pipe, 0, ZMQ_POLLIN };
    zloop_poller (self->loop, &item, s_driver_control, self);
    return self;
}

static void
driver_destroy (driver_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        driver_t *self = *self_p;
        if (self->verbose)
            zclock_log ("I: (udp) shutting down driver");
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
    self->socktype = socktype;

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
    self->msgpipe = zsocket_new (driver->ctx, ZMQ_DEALER);
    assert (self->msgpipe);
    zsocket_connect (self->msgpipe, "inproc://%s", vtxname);

    //  If we drop on no peerings, start routing input now
    if (self->min_peerings == 0) {
        //  Ask reactor to start monitoring vocket's msgpipe pipe
        zmq_pollitem_t item = { self->msgpipe, 0, ZMQ_POLLIN, 0 };
        zloop_poller (driver->loop, &item, s_vocket_input, self);
    }
    //  Store this vocket per driver so that driver can cleanly destroy
    //  all its vockets when it is destroyed.
    zlist_push (driver->vockets, self);

    //* Start transport-specific work
    //  Create UDP socket handle, used for outbound connections
    self->handle = socket (AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (self->handle == -1)
        derp ("socket");

    //  Enable broadcast mode on handle, always
    int broadcast_on = 1;
    if (setsockopt (self->handle, SOL_SOCKET, SO_BROADCAST,
        &broadcast_on, sizeof (int)) == -1)
        derp ("setsockopt (SO_BROADCAST)");

    //  Catch input on handle
    zmq_pollitem_t item = { NULL, self->handle, ZMQ_POLLIN, 0 };
    zloop_poller (driver->loop, &item, s_binding_input, self);
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

        //* Start transport-specific work
        s_close_handle (self->handle, driver);
        //* End transport-specific work

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

#ifdef VOCKET_STATS
        char *type_name [] = {
            "PAIR", "PUB", "SUB", "REQ", "REP",
            "DEALER", "ROUTER", "PULL", "PUSH",
            "XPUB", "XSUB"
        };
        printf ("I: type=%s sent=%d recd=%d outp=%d inp=%d drop=%d\n",
            type_name [self->socktype],
            self->outgoing, self->incoming,
            self->outpiped, self->inpiped,
            self->dropped);
#endif
        free (self->vtxname);
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

        //* Start transport-specific work
        //  Create new bound UDP socket handle
        self->handle = socket (AF_INET, SOCK_DGRAM, IPPROTO_UDP);
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
            self->exception = TRUE;
        }
        if (!self->exception) {
            if (bind (self->handle,
                (const struct sockaddr *) &addr, IN_ADDR_SIZE) == -1) {
                zclock_log ("E: bind failed: '%s'", strerror (errno));
                self->exception = TRUE;
            }
        }
        if (!self->exception) {
            //  Catch input on handle
            zmq_pollitem_t item = { NULL, self->handle, ZMQ_POLLIN, 0 };
            zloop_poller (self->driver->loop, &item, s_binding_input, vocket);
        }
        //* End transport-specific work
        if (self->exception) {
            free (self->address);
            free (self);
        }
        else {
            //  Store new binding in vocket containers
            zhash_insert (vocket->binding_hash, address, self);
            zhash_freefn (vocket->binding_hash, address, binding_delete);
            if (self->driver->verbose)
                zclock_log ("I: (udp) create binding to %s", self->address);
        }
    }
    return self;
}

//  Destroy binding object, when binding is removed from vocket->binding_hash

static void
binding_delete (void *argument)
{
    binding_t *self = (binding_t *) argument;
    if (self->driver->verbose)
        zclock_log ("I: (udp) delete binding %s", self->address);

    //* Start transport-specific work
    s_close_handle (self->handle, self->driver);
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
        if (self->driver->verbose)
            zclock_log ("I: (udp) create peering to %s", address);

        //* Start transport-specific work
        //  Translate hostname:port into sockaddr_in structure
        //  Wildcard is broadcast for outgoing, ANY for incoming
        if (s_str_to_sin_addr (&self->addr, address,
            outgoing? s_broadcast_addr (): INADDR_ANY))
            self->exception = TRUE;
        else {
            //  Store broadcast address if needed
            if (outgoing && *address == '*') {
                self->broadcast = TRUE;
                self->bcast = self->addr;
            }
            //  Start peering monitor (reactor timer)
            s_peering_monitor (self->driver->loop, NULL, self);
            //  Start resend timer for this peering
            zloop_timer (self->driver->loop, VTX_UDP_RESEND_IVL,
                         0, s_resend_timer, self);
        }
        //* End transport-specific work

        if (self->exception) {
            free (self->address);
            free (self);
        }
        else {
            //  Store new peering in vocket containers
            zhash_insert (vocket->peering_hash, address, self);
            zhash_freefn (vocket->peering_hash, address, peering_delete);
            zlist_append (vocket->peering_list, self);
            vocket->peerings++;
        }
    }
    return self;
}

//  Destroy peering, indirect by removing from peering hash

static void
peering_destroy (peering_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        peering_t *self = *self_p;
        //  All destruction is done in delete method
        zhash_delete (self->vocket->peering_hash, self->address);
    }
}

//  Destroy peering, when it's removed from vocket->peering_hash

static void
peering_delete (void *argument)
{
    peering_t *self = (peering_t *) argument;
    vocket_t *vocket = self->vocket;
    driver_t *driver = self->driver;
    if (self->driver->verbose)
        zclock_log ("I: (udp) delete peering %s", self->address);

    //* Start transport-specific work
    zmsg_destroy (&self->request);
    zmsg_destroy (&self->reply);
    //* End transport-specific work

    peering_lower (self);
    zlist_remove (vocket->peering_list, self);
    zloop_timer_end (driver->loop, self);
    free (self->address);
    free (self);
    vocket->peerings--;
}

//  Send frame data to peering as formatted command. If there was a
//  network error, destroys the peering and returns -1.

static int
peering_send_msg (peering_t *self, zmsg_t *msg, int flags)
{
    assert (self);
    byte *data;
    size_t size = zmsg_encode (msg, &data);
    int rc = peering_send (self, VTX_UDP_NOM, data, size, flags);
    self->vocket->outgoing++;
    free (data);
    return rc;
}

//  Send a buffer of data to peering, prefixed by command header. If there
//  was a network error, destroys the peering and returns -1.

static int
peering_send (peering_t *self, int command, byte *data, size_t size, int flags)
{
    vocket_t *vocket = self->vocket;
    driver_t *driver = self->driver;
    if (self->driver->verbose) {
        char *address = s_sin_addr_to_str (&self->addr);
        zclock_log ("I: (udp) send [%s:%x] - %zd bytes to %s",
            s_command_name [command], self->sendseq & 15,
            size, address);
        free (address);
    }
    int rc = 0;
    if ((size + VTX_UDP_HEADER) <= VTX_UDP_MSGMAX) {
        byte buffer [size + VTX_UDP_HEADER];
        buffer [0] = (VTX_UDP_VERSION << 4) + (flags & 15);
        buffer [1] = (command << 4) + (self->sendseq & 15);
        if (size)
            memcpy (buffer + VTX_UDP_HEADER, data, size);
        rc = sendto (vocket->handle,
            buffer, size + VTX_UDP_HEADER, 0,
            (const struct sockaddr *) &self->addr, IN_ADDR_SIZE);
        if (rc > 0) {
            //  Calculate when we'd need to start sending HUGZ
            self->silent = zclock_time () + VTX_UDP_TIMEOUT / 3;
            rc = 0;
        }
        else
        if (s_handle_io_error ("sendto") == -1)
            peering_destroy (&self);
    }
    else
    if (self->driver->verbose)
        zclock_log ("W: over-long message, %d bytes, dropping", size);

    return rc;
}

//  Peering is now active

static void
peering_raise (peering_t *self)
{
    vocket_t *vocket = self->vocket;
    driver_t *driver = self->driver;
    if (!self->alive) {
        if (self->driver->verbose)
            zclock_log ("I: (udp) bring up peering to %s", self->address);
        self->alive = TRUE;
        self->expiry = zclock_time () + VTX_UDP_TIMEOUT;
        self->silent = zclock_time () + VTX_UDP_TIMEOUT / 3;
        zlist_append (vocket->live_peerings, self);
        if (zlist_size (vocket->live_peerings) == vocket->min_peerings) {
            //  Ask reactor to start monitoring vocket's msgpipe pipe
            zmq_pollitem_t item = { vocket->msgpipe, 0, ZMQ_POLLIN, 0 };
            zloop_poller (driver->loop, &item, s_vocket_input, vocket);
        }
    }
}

//  Peering is now inactive

static void
peering_lower (peering_t *self)
{
    vocket_t *vocket = self->vocket;
    driver_t *driver = self->driver;
    if (self->alive) {
        if (self->driver->verbose)
            zclock_log ("I: (udp) take down peering to %s", self->address);
        self->alive = FALSE;
        zlist_remove (vocket->live_peerings, self);
        if (zlist_size (vocket->live_peerings) < vocket->min_peerings) {
            //  Ask reactor to stop monitoring vocket's msgpipe pipe
            zmq_pollitem_t item = { vocket->msgpipe, 0, ZMQ_POLLIN, 0 };
            zloop_poller_end (driver->loop, &item);
        }
    }
}

//  ---------------------------------------------------------------------
//  Reactor handlers

//  Handle bind/connect from caller:
//
//  [command]   BIND, CONNECT, GETMETA, CLOSE, SHUTDOWN
//  [socktype]  0MQ socket type as ASCII number
//  [vtxname]   VTX name for the 0MQ socket
//  [address]   External address to bind/connect to, or meta name

static int
s_driver_control (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    int rc = 0;
    char *reply = "0";
    driver_t *driver = (driver_t *) arg;
    zmsg_t *request = zmsg_recv (item->socket);

    char *command  = zmsg_popstr (request);
    char *socktype = zmsg_popstr (request);
    char *vtxname  = zmsg_popstr (request);
    char *address  = zmsg_popstr (request);
    zmsg_destroy (&request);

    //  Lookup vocket with this vtxname, create if necessary
    vocket_t *vocket = NULL;
    if (vtxname) {
        vocket = (vocket_t *) zlist_first (driver->vockets);
        while (vocket) {
            if (streq (vocket->vtxname, vtxname))
                break;
            vocket = (vocket_t *) zlist_next (driver->vockets);
        }
        if (!vocket)
            vocket = vocket_new (driver, atoi (socktype), vtxname);
    }
    //  Multiple binds or connects to same address are idempotent
    if (streq (command, "BIND")) {
        assert (vocket);
        if (!binding_require (vocket, address))
            reply = "1";
    }
    else
    if (streq (command, "CONNECT")) {
        assert (vocket);
        if (vocket->peerings < vocket->max_peerings)
            peering_require (vocket, address, TRUE);
        else {
            zclock_log ("E: connect failed: too many peerings");
            reply = "1";
        }
    }
    else
    if (streq (command, "GETMETA")) {
        assert (vocket);
        if (streq (address, "sender"))
            reply = vocket->sender;
        else
            reply = "Unknown name";
    }
    else
    if (streq (command, "CLOSE")) {
        assert (vocket);
        vocket_destroy (&vocket);
    }
    else
    if (streq (command, "SHUTDOWN"))
        rc = -1;                //  Shutdown driver
    else {
        zclock_log ("E: invalid command: %s", command);
        reply = "1";
    }
    zstr_sendf (item->socket, reply);
    free (command);
    free (socktype);
    free (vtxname);
    free (address);
    return rc;
}

//  -------------------------------------------------------------------------
//  Input message on data pipe from application 0MQ socket

static int
s_vocket_input (zloop_t *loop, zmq_pollitem_t *item, void *arg)
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
    if (!msg)
        return 0;               //  Interrupted
    vocket->outpiped++;

    //  Route message to active peerings as appropriate
    if (vocket->routing == VTX_ROUTING_NONE)
        zclock_log ("W: send() not allowed - dropping");
    else
    if (vocket->routing == VTX_ROUTING_REQUEST) {
        //  Find next live peering if any
        //  TODO: make this code generic to all drivers
        peering_t *peering = (peering_t *) zlist_pop (vocket->live_peerings);
        assert (peering);
        if (peering->request == NULL) {
            peering->sendseq++;
            peering->request = msg;
            peering_send_msg (peering, peering->request, 0);
            msg = NULL;         //  Peering now owns message
        }
        else
            zclock_log ("E: illegal send() without recv() from REQ socket");
        zlist_append (vocket->live_peerings, peering);
    }
    else
    if (vocket->routing == VTX_ROUTING_REPLY) {
        //  TODO: make this code generic to all drivers
        peering_t *peering = vocket->reply_to;
        if (peering) {
            zmsg_destroy (&peering->reply);
            peering->reply = msg;
            msg = NULL;         //  Peering now owns message
            peering->sendseq = peering->recvseq;
            peering_send_msg (peering, peering->reply, 0);
            vocket->reply_to = NULL;
        }
        else
            zclock_log ("E: illegal send() without recv() on REP socket");
    }
    else
    if (vocket->routing == VTX_ROUTING_DEALER) {
        peering_t *peering = (peering_t *) zlist_pop (vocket->live_peerings);
        peering->sendseq = peering->recvseq;
        zmsg_destroy (&peering->reply);
        peering->reply = msg;
        msg = NULL;         //  Peering now owns message
        peering_send_msg (peering, peering->reply, 0);
        zlist_append (vocket->live_peerings, peering);
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
            if (peering && peering->alive) {
                zmsg_destroy (&peering->reply);
                peering->reply = msg;
                msg = NULL;         //  Peering now owns message
                peering->sendseq = peering->recvseq;
                peering_send_msg (peering, peering->reply, 0);
            }
            else
                zclock_log ("W: no route to '%s' - dropping", address);
        }
        else
            zclock_log ("E: invalid address '%s' - dropping", address);
        free (address);
    }
    else
    if (vocket->routing == VTX_ROUTING_PUBLISH) {
        peering_t *peering = (peering_t *) zlist_first (vocket->live_peerings);
        while (peering) {
            peering_send_msg (peering, msg, 0);
            peering = (peering_t *) zlist_next (vocket->live_peerings);
        }
    }
    else
    if (vocket->routing == VTX_ROUTING_SINGLE) {
        //  We expect a single live peering and we should not have read
        //  a message otherwise...
        peering_t *peering = (peering_t *) zlist_first (vocket->live_peerings);
        peering_send_msg (peering, msg, 0);
    }
    else
        zclock_log ("E: unknown routing mechanism - dropping");

    zmsg_destroy (&msg);
    return 0;
}


//  -------------------------------------------------------------------------
//  Input message on binding handle
//  This implements the receiver side of the UDP protocol-without-a-name
//  I'd like to implement this as a neat little finite-state machine.

static int
s_binding_input (zloop_t *loop, zmq_pollitem_t *item, void *arg)
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
        s_handle_io_error ("recvfrom");
        return 0;
    }
    //  Parse incoming protocol command
    int version = buffer [0] >> 4;
    int flags   = buffer [0] & 0xf;
    int command = buffer [1] >> 4;
    int recvseq = buffer [1] & 0xf;
    byte *body = buffer + VTX_UDP_HEADER;
    size_t body_size = size - VTX_UDP_HEADER;
    //  If body is a string, make it a valid C string
    body [body_size] = 0;

    if (version != VTX_UDP_VERSION) {
        zclock_log ("W: garbage version '%d' - dropping", version);
        return 0;
    }
    if (command >= VTX_UDP_CMDLIMIT) {
        zclock_log ("W: garbage command '%d' - dropping", command);
        return 0;
    }
    char *address = s_sin_addr_to_str (&addr);
    if (driver->verbose)
        zclock_log ("I: (udp) recv [%s:%x] - %zd bytes from %s",
            s_command_name [command], recvseq & 15, body_size, address);
    if (randof (5) == 9) {
        if (driver->verbose)
            zclock_log ("I: (udp) simulating UDP breakage - dropping");
        return 0;
    }

    //  First pass to try to resolve or create peering if needed
    peering_t *peering = (peering_t *) zhash_lookup (vocket->peering_hash, address);
    if (!peering) {
        //  OHAI-OK command uses command body as peering key so it can resolve
        //  broadcast replies (peering key will still be the broadcast address,
        //  not the actual peer address).
        if (command == VTX_UDP_OHAI_OK)
            peering = (peering_t *) zhash_lookup (vocket->peering_hash, (char *) body);
        else
        if (command == VTX_UDP_OHAI) {
            peering = peering_require (vocket, address, FALSE);
            if (vocket->peerings > vocket->max_peerings) {
                char *reason = "Max peerings reached for socket";
                peering_send (peering, VTX_UDP_ROTFL,
                    (byte *) reason, strlen (reason), 0);
                peering_destroy (&peering);
                free (address);
                return 0;
            }
        }
    }

    //  At this stage we need an active peering to continue
    if (peering)
        //  Any input at all from a peer counts as activity
        peering->expiry = zclock_time () + VTX_UDP_TIMEOUT;
    else {
        if (driver->verbose)
            zclock_log ("W: %s from unknown peer %s - dropping",
                s_command_name [command], address);
        free (address);
        return 0;
    }

    //  Now do command-specific work
    if (command == VTX_UDP_OHAI) {
        if (peering_send (peering, VTX_UDP_OHAI_OK, body, body_size, 0) == 0)
            peering_raise (peering);
    }
    else
    if (command == VTX_UDP_OHAI_OK) {
        if (strneq (address, (char *) body)) {
            if (driver->verbose)
                zclock_log ("I: (udp) focus peering from %s to %s",
                    (char *) body, address);
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
        peering_send (peering, VTX_UDP_HUGZ_OK, NULL, 0, 0);
    else
    if (command == VTX_UDP_NOM) {
        zmsg_t *msg = zmsg_decode (body, body_size);
        if (!msg) {
            zclock_log ("W: corrupt message from %s", address);
            free (address);
            return 0;
        }
        vocket->incoming++;
        if (vocket->routing == VTX_ROUTING_REQUEST) {
            //  If we got a duplicate reply, discard it
            if (recvseq == peering->recvseq) {
                zmsg_destroy (&msg);    //  Don't pass to application
                vocket->dropped++;
            }
            else {
                //  Clear pending request, allow another
                peering->recvseq = recvseq;
                zmsg_destroy (&peering->request);
            }
        }
        else
        if (vocket->routing == VTX_ROUTING_REPLY) {
            //  If we got a duplicate request, resend last reply
            if (flags && VTX_UDP_RESEND
            &&  recvseq == peering->recvseq) {
                assert (peering->reply);
                peering_send_msg (peering, peering->reply, 0);
                zmsg_destroy (&msg);    //  Don't pass to application
                vocket->dropped++;
            }
            else {
                //  TODO: this won't work when multiple peers send
                //  requests concurrently...
                //  Track peering for eventual reply routing
                vocket->reply_to = peering;
                peering->recvseq = recvseq;
            }
        }
        else
        if (vocket->routing == VTX_ROUTING_ROUTER) {
            if (flags && VTX_UDP_RESEND
            &&  recvseq == peering->recvseq) {
                assert (peering->reply);
                peering_send_msg (peering, peering->reply, 0);
                zmsg_destroy (&msg);    //  Don't pass to application
                vocket->dropped++;
            }
            else {
                //  Send schemed identity envelope
                zmsg_pushstr (msg, "%s://%s", driver->scheme, address);
                peering->recvseq = recvseq;
            }
        }
        else
        if (vocket->routing == VTX_ROUTING_DEALER) {
            if (flags && VTX_UDP_RESEND
            &&  recvseq == peering->recvseq) {
                assert (peering->reply);
                peering_send_msg (peering, peering->reply, 0);
                zmsg_destroy (&msg);    //  Don't pass to application
                vocket->dropped++;
            }
            else
                peering->recvseq = recvseq;
        }

        //  Now pass message onto application if required
        if (vocket->nomnom) {
            if (msg) {
                char *colon = strchr (address, ':');
                if (!colon)
                    puts (address);
                assert (colon);
                *colon = 0;
                strcpy (vocket->sender, address);
                zmsg_send (&msg, vocket->msgpipe);
                vocket->inpiped++;
            }
        }
        else
            zclock_log ("W: unexpected NOM from %s - dropping", address);
    }
    else
    if (command == VTX_UDP_ROTFL)
        zclock_log ("W: got ROTFL: %s", body);

    free (address);
    return 0;
}


//  -------------------------------------------------------------------------
//  Monitor peering for connectivity and send OHAIs and HUGZ as needed

static int
s_peering_monitor (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    peering_t *peering = (peering_t *) arg;
    vocket_t *vocket = peering->vocket;

    int interval = VTX_UDP_OHAI_IVL;
    if (peering->alive) {
        int64_t time_now = zclock_time ();
        if (time_now > peering->expiry) {
            peering_lower (peering);
            //  If this was a broadcast peering, reset it to BROADCAST
            if (peering->broadcast) {
                char *address = s_sin_addr_to_str (&peering->bcast);
                if (vocket->driver->verbose)
                    zclock_log ("I: (udp) unfocus peering from %s to %s",
                        peering->address, address);
                int rc = zhash_rename (vocket->peering_hash, peering->address, address);
                assert (rc == 0);
                free (peering->address);
                peering->addr = peering->bcast;
                peering->address = address;
            }
            else
            if (!peering->outgoing) {
                peering_destroy (&peering);
                interval = 0;           //  Don't reset timer
            }
        }
        else
        if (time_now > peering->silent) {
            if (peering_send (peering, VTX_UDP_HUGZ, NULL, 0, 0) == 0) {
                interval = VTX_UDP_TIMEOUT / 3;
                peering->silent = zclock_time () + interval;
            }
        }
    }
    else
    if (peering->outgoing)
        peering_send (peering, VTX_UDP_OHAI,
            (byte *) peering->address, strlen (peering->address), 0);

    if (interval)
        zloop_timer (loop, interval, 1, s_peering_monitor, peering);
    return 0;
}


//  Resend request NOM if peering is alive and no response received

static int
s_resend_timer (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    peering_t *peering = (peering_t *) arg;
    if (peering->request && peering->alive)
        peering_send_msg (peering, peering->request, VTX_UDP_RESEND);
    return 0;
}


//  Returns (last valid) broadcast address for LAN
//  On Windows we just force SO_BROADCAST, getting the interfaces
//  via win32 is too ugly to put into this code...

static uint32_t
s_broadcast_addr (void)
{
    uint32_t address = INADDR_ANY;
#   if defined (__UNIX__)
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
#   endif
    return address;
}


//  Converts a sockaddr_in to a string, returns static result

static char *
s_sin_addr_to_str (struct sockaddr_in *addr)
{
    char
        address [24];
    snprintf (address, 24, "%s:%d",
        inet_ntoa (addr->sin_addr), ntohs (addr->sin_port));
    return strdup (address);
}

//  Converts a hostname:port into a sockaddr_in, returns static result
//  If hostname is '*', uses the wildcard value for the host address.
//  Asserts on badly formatted address.

static int
s_str_to_sin_addr (struct sockaddr_in *addr, char *address, uint32_t wildcard)
{
    int rc = 0;
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
    if (!inet_aton (hostname, &addr->sin_addr)) {
        struct hostent *phe = gethostbyname (hostname);
        if (phe)
            memcpy (&addr->sin_addr, phe->h_addr, phe->h_length);
        else {
            errno = EINVAL;
            rc = -1;
        }
    }
    free (hostname);
    return rc;
}

//  Close handle, remove poller from reactor

static void
s_close_handle (int handle, driver_t *driver)
{
    if (handle > 0) {
        zmq_pollitem_t item = { 0, handle };
        zloop_poller_end (driver->loop, &item);
        close (handle);
    }
}

//  Handle error from I/O operation, return 0 if the caller should
//  retry, -1 to abandon the operation.

static int
s_handle_io_error (char *reason)
{
#   ifdef __WINDOWS__
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
#   endif
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
        zclock_log ("W: (udp) error '%s' on %s", strerror (errno), reason);
        return -1;
    }
}
