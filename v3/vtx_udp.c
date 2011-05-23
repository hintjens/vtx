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


//  ---------------------------------------------------------------------
//  We have one instance of this thread per registered driver.


//  Keep list of readers, each is either a socket or a native fd
typedef struct {
    void *socket;
    int   handle;
} reader_t;


static reader_t *
s_reader_new (void *socket, int handle)
{
    assert (socket == NULL || handle == 0);
    reader_t *reader = (reader_t *) zmalloc (sizeof (reader_t));
    reader->socket = socket;
    reader->handler = handler;
    return reader;
}

static void
derp (char *s)
{
    perror (s);
    exit (1);
}


void vtx_udp_driver (void *args, zctx_t *ctx, void *pipe)
{
    //  List of readers for zmq_poll construction
    //  Push own pipe as first reader
    zlist_t *readers = zlist_new ();
    zlist_push (readers, s_reader_new (pipe, NULL);
    zmq_pollitem_t *pollset = NULL;
    Bool rebuild_pollset = TRUE;

    //  Handle readers ad-infinitum
    while (!zctx_interrupted) {
        uint item_nbr;
        int poll_size;
        if (rebuild_pollset) {
            free (pollset);
            poll_size = zlist_size (readers);
            pollset = (zmq_pollitem_t *) zmalloc (
                poll_size * sizeof (zmq_pollitem_t));

            reader_t *reader = (reader_t *) zlist_first (readers);
            item_nbr = 0;
            while (reader) {
                pollset [item_nbr].socket = reader->socket;
                pollset [item_nbr].fd = reader->handle;
                pollset [item_nbr].events = ZMQ_POLLIN;
                reader = (reader_t *) zlist_next (readers);
                item_nbr++;
            }
        }
        rc = zmq_poll (pollset, poll_size, -1);
        if (rc == -1)
            break;              //  Context has been shut down

        //  Item 0 is always our pipe pipe
        if (items [0].revents & ZMQ_POLLIN) {
            char *command = zstr_recv (pipe);
            char *frontend = strchr (command, ':');
            assert (frontend);
            *frontend++ = 0;
            char *address = strchr (frontend, ':');
            assert (address);
            *address++ = 0;
            char *port = strchr (address, ':');
            if (port)
                *port++ = 0;

            //  Create inproc PAIR socket, for every command
            void *backend = zsocket_new (ctx, ZMQ_PAIR);
            if (zsocket_connect (backend, "inproc://%s-%p", frontend, address) == -1)
                derp ("zsocket_connect");
            zlist_push (readers, s_reader_new (backend, NULL);

            //  Create UDP socket (for now, for every command)
            int handle = socket (AF_INET, SOCK_DGRAM, IPPROTO_UDP);
            if (handle == -1)
                derp ("socket");
            memset (&peer, 0, sizeof (peer));
            peer.sin_family = AF_INET;
            zlist_push (readers, s_reader_new (NULL, handle);

            //  Pollset is dirty, rebuild next time round
            rebuild_pollset = TRUE;

            //  Handle CONNECT command
            if (streq (command, "CONNECT")) {
                if (streq (address, "*")) {
                    //  Enable broadcast mode
                    int broadcast_on = 1;
                    if (setsockopt (handle, SOL_SOCKET, SO_BROADCAST,
                        &broadcast_on, sizeof (int)) == -1)
                        derp ("setsockopt (SO_BROADCAST)");

                    //  Set address to LAN broadcast
                    //  Will send only to first interface...
                    peer.sin_addr.s_addr = htonl (INADDR_BROADCAST);
                }
                else
                if (inet_aton (address, &peer.sin_addr) == 0)
                    derp ("inet_aton");

                peer.sin_port = htons (atoi (port));
                if (connect (handle, (const struct sockaddr *) &addr, sizeof (addr)) == -1)
                    derp ("connect");
            }
            else
            //  Handle BIND command
            if (streq (command, "BIND")) {
                struct sockaddr_in addr = { 0 };
                addr.sin_family = AF_INET;
                if (streq (address, "*"))
                    addr.sin_addr.s_addr = htonl (INADDR_ANY);
                else
                if (inet_aton (address, &addr.sin_addr) == 0)
                    derp ("inet_aton");

                addr.sin_port = htons (atoi (port));
                if (bind (handle, (const struct sockaddr *) &addr, sizeof (addr)) == -1)
                    derp ("bind");
            }
            else
                printf ("Invalid command: %s\n", command);

            zstr_send (pipe, "0");
            free (command);
        }
        //  Items 1+ are always in a pair, socket then handle
        for (item_nbr = 0; item_nbr < poll_size; item_nbr++) {
            if (pollset [item_nbr].revents & ZMQ_POLLIN) {
                if (pollset [item_nbr].socket) {
                    void *socket = pollset [item_nbr].socket;
                    int handle = pollset [item_nbr + 1].fd;
                }
                else {
                    void *socket = pollset [item_nbr - 1].socket;
                    int handle = pollset [item_nbr].fd;
                }
            }
        }
    }
    //  Destroy list of readers
    while (zlist_size (readers))
        free (zlist_pop (readers));
    zlist_destroy (&readers);
    free (pollset);
}

//  Process data message
static void
agent_data (agent_t *self)
{
    //  Handle only single-part messages for now
    zframe_t *frame = zframe_recv (data);
    assert (!zframe_more (frame));
    assert (inet_ntoa (peer.sin_addr));
    byte *data = zframe_data (frame);
    size_t size = zframe_size (frame);
    //  Discard over-long messages silently
    if (size <= ZVUDP_MSGMAX) {
        if (sendto (handle, data, size, 0,
            (const struct sockaddr *) &peer, sizeof (peer)) == -1)
            derp ("sendto");
    }
    zframe_destroy (&frame);
}


//  Process UDP socket input
static void
agent_handle (agent_t *self)
{
    char buffer [ZVUDP_MSGMAX];
    socklen_t addr_len = sizeof (struct sockaddr_in);
    ssize_t size = recvfrom (handle, buffer, ZVUDP_MSGMAX, 0,
                            (struct sockaddr *) &peer, &addr_len);
    if (size == -1)
        derp ("recvfrom");

//    printf ("Received from %s:%d\n",
//        inet_ntoa (peer.sin_addr), ntohs (peer.sin_port));
    zframe_t *frame = zframe_new (buffer, size);
    zframe_send (&frame, data, 0);
}

