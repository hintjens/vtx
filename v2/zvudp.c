/*  =====================================================================
    zvudp - 0MQ virtual UDP transport driver

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

#include "zvudp.h"


//  =====================================================================
//  Synchronous part, works in our application thread

//  ---------------------------------------------------------------------
//  Structure of our class
//  We separate control and data over two pipe sockets

struct _zvudp_t {
    zctx_t *ctx;        //  Our context wrapper
    void *control;      //  Control pipe to zvudp agent
    void *data;         //  Data pipe to zvudp agent
};

//  This is the thread that handles the UDP virtual driver
static void zvudp_agent (void *control, zctx_t *ctx, void *data);


//  ---------------------------------------------------------------------
//  Constructor

zvudp_t *
zvudp_new (void)
{
    zvudp_t
        *self;

    self = (zvudp_t *) zmalloc (sizeof (zvudp_t));
    self->ctx = zctx_new ();

    //  Create data pipe pair and pass other end as to thread
    self->data = zsocket_new (self->ctx, ZMQ_PAIR);
    zsocket_bind (self->data, "inproc://zvudp-%p", self->data);

    void *peer_data = zsocket_new (self->ctx, ZMQ_PAIR);
    zsocket_connect (peer_data, "inproc://zvudp-%p", self->data);

    self->control = zthread_fork (self->ctx, zvudp_agent, peer_data);
    return self;
}


//  ---------------------------------------------------------------------
//  Destructor

void
zvudp_destroy (zvudp_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        zvudp_t *self = *self_p;
        zctx_destroy (&self->ctx);
        free (self);
        *self_p = NULL;
    }
}


//  ---------------------------------------------------------------------
//  Bind to local interface
//  Sends BIND:interface:port to the agent
//  "*" means all INADDR_ANY

void
zvudp_bind (zvudp_t *self, char *interface, int port)
{
    assert (self);
    assert (interface);
    zstr_sendf (self->control, "BIND:%s:%d", interface, port);
}


//  ---------------------------------------------------------------------
//  Connect to address and port
//  Sends CONNECT:address:port to the agent
//  "*" means all INADDR_BROADCAST

void
zvudp_connect (zvudp_t *self, char *address, int port)
{
    assert (self);
    assert (address);
    zstr_sendf (self->control, "CONNECT:%s:%d", address, port);
}

//  ---------------------------------------------------------------------
//  Return data socket for zvudp instance

void *
zvudp_socket (zvudp_t *self)
{
    return self->data;
}


//  =====================================================================
//  Asynchronous part, works in the background

static void
derp (char *s)
{
    perror (s);
    exit (1);
}


//  ---------------------------------------------------------------------
//  Simple class for one background agent

typedef struct {
    zctx_t *ctx;                //  Own context
    void *control;              //  Recv control commands from appl
    void *data;                 //  Send/recv data to/from application
    int udpsock;                //  UDP socket
    struct sockaddr_in peer;    //  Peer address, just 1 for now
} agent_t;

static agent_t *
agent_new (zctx_t *ctx, void *control, void *data)
{
    agent_t *self = (agent_t *) zmalloc (sizeof (agent_t));
    self->ctx = ctx;
    self->control = control;
    self->data = data;
    self->udpsock = socket (AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (self->udpsock == -1)
        derp ("socket");
    memset (&self->peer, 0, sizeof (self->peer));
    self->peer.sin_family = AF_INET;
    return self;
}

static void
agent_destroy (agent_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        agent_t *self = *self_p;
        free (self);
        *self_p = NULL;
    }
}


//  Process control command
static void
agent_control (agent_t *self)
{
    char *command = zstr_recv (self->control);
    char *value = strchr (command, ':');
    char *argument = NULL;
    if (value) {
        *value++ = 0;
        argument = strchr (value, ':');
        if (argument)
            *argument++ = 0;
    }
    if (streq (command, "CONNECT")) {
        if (streq (value, "*")) {
            //  Enable broadcast mode
            int broadcast_on = 1;
            if (setsockopt (self->udpsock, SOL_SOCKET, SO_BROADCAST,
                &broadcast_on, sizeof (int)) == -1)
                derp ("setsockopt (SO_BROADCAST)");

            //  Set address to LAN broadcast
            //  Will send only to first interface...
            self->peer.sin_addr.s_addr = htonl (INADDR_BROADCAST);
        }
        else {
            //  'Connect' to specific IP address
            //  We don't actually connect but set address for sendto()
            if (inet_aton (value, &self->peer.sin_addr) == 0)
                derp ("inet_aton");
        }
        self->peer.sin_port = htons (atoi (argument));
    }
    else
    if (streq (command, "BIND")) {
        struct sockaddr_in addr = { 0 };
        addr.sin_family = AF_INET;
        if (streq (value, "*"))
            addr.sin_addr.s_addr = htonl (INADDR_ANY);
        else
        if (inet_aton (value, &addr.sin_addr) == 0)
            derp ("inet_aton");

        addr.sin_port = htons (atoi (argument));
        if (bind (self->udpsock, (const struct sockaddr *) &addr, sizeof (addr)) == -1)
            derp ("bind");
    }
    else
        printf ("Invalid command: %s\n", command);
}


//  Process data message
static void
agent_data (agent_t *self)
{
    //  Handle only single-part messages for now
    zframe_t *frame = zframe_recv (self->data);
    assert (!zframe_more (frame));
    assert (inet_ntoa (self->peer.sin_addr));
    byte *data = zframe_data (frame);
    size_t size = zframe_size (frame);
    //  Discard over-long messages silently
    if (size <= ZVUDP_MSGMAX) {
        if (sendto (self->udpsock, data, size, 0,
            (const struct sockaddr *) &self->peer, sizeof (self->peer)) == -1)
            derp ("sendto");
    }
    zframe_destroy (&frame);
}


//  Process UDP socket input
static void
agent_udpsock (agent_t *self)
{
    char buffer [ZVUDP_MSGMAX];
    socklen_t addr_len = sizeof (struct sockaddr_in);
    ssize_t size = recvfrom (self->udpsock, buffer, ZVUDP_MSGMAX, 0,
                            (struct sockaddr *) &self->peer, &addr_len);
    if (size == -1)
        derp ("recvfrom");

//    printf ("Received from %s:%d\n",
//        inet_ntoa (self->peer.sin_addr), ntohs (self->peer.sin_port));
    zframe_t *frame = zframe_new (buffer, size);
    zframe_send (&frame, self->data, 0);
}


//  ---------------------------------------------------------------------
//  Asynchronous agent manages server pool and handles request/reply
//  dialog when the application asks for it.

static void
zvudp_agent (void *data, zctx_t *ctx, void *control)
{
    agent_t *self = agent_new (ctx, control, data);

    zmq_pollitem_t items [] = {
        { self->control, 0, ZMQ_POLLIN, 0 },
        { self->data, 0, ZMQ_POLLIN, 0 },
        { NULL, self->udpsock, ZMQ_POLLIN, 0 }
    };
    while (!zctx_interrupted) {
        int rc = zmq_poll (items, 3, -1);
        if (rc == -1)
            break;              //  Context has been shut down
        if (items [0].revents & ZMQ_POLLIN)
            agent_control (self);
        if (items [1].revents & ZMQ_POLLIN)
            agent_data (self);
        if (items [2].revents & ZMQ_POLLIN)
            agent_udpsock (self);
    }
    agent_destroy (&self);
}
