//
//  VTX example that locates server using UDP broadcast
//
//  This file is part of VTX, the 0MQ virtual transport interface:
//  http://vtx.zeromq.org.

#include "vtx.c"
#include "vtx_udp.c"

int main (void)
{
    //  Initialize 0MQ context and virtual transport interface
    zctx_t *ctx = zctx_new ();

    //  Initialize virtual transport interface
    vtx_t *vtx = vtx_new (ctx);
    int rc = vtx_udp_load (vtx);
    assert (rc == 0);

    //  Look for server, try several times
    int retry;
    for (retry = 0; retry < 1; retry++) {
        void *client = vtx_socket (vtx, ZMQ_REQ);
        assert (client);
        rc = vtx_connect (vtx, client, "udp://*:32000");
        assert (rc == 0);

        //  Look for name server anywhere on LAN
        printf ("I: looking for server on network...\n");
        zstr_send (client, "hello?");

        //  Wait for at most 500 msec for reply before retrying
        zmq_pollitem_t items [] = { { client, 0, ZMQ_POLLIN, 0 } };
        int rc = zmq_poll (items, 1, 500 * ZMQ_POLL_MSEC);
        if (rc == -1)
            break;              //  Context has been shut down

        if (items [0].revents & ZMQ_POLLIN) {
            char *input = zstr_recv (client);
            puts (input);
            free (input);
            break;
        }
        vtx_close (vtx, client);
    }
    vtx_destroy (&vtx);
    zctx_destroy (&ctx);
    return 0;
}
