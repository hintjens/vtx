//
//  VTX test example, client
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
    int rc = vtx_register (vtx, "udp", vtx_udp_driver);
    assert (rc == 0);

    //  Create client socket and connect to broadcast address
    void *client = vtx_socket (vtx, ZMQ_DEALER);
    assert (client);
    rc = vtx_connect (vtx, client, "udp://*:32000");
    assert (rc == 0);

    while (!zctx_interrupted) {
        //  Look for name server anywhere on LAN
        zstr_send (client, "hello?");
        zclock_log ("C: hello?");

        //  Wait for at most 5000 msec for reply before retrying
        zmq_pollitem_t items [] = { { client, 0, ZMQ_POLLIN, 0 } };
        int rc = zmq_poll (items, 1, 5000 * ZMQ_POLL_MSEC);
        if (rc == -1)
            break;              //  Context has been shut down

        if (items [0].revents & ZMQ_POLLIN) {
            char *input = zstr_recv (client);
            free (input);
            zclock_sleep (1000);
        }
    }
    vtx_destroy (&vtx);

    zctx_destroy (&ctx);
    return 0;
}
