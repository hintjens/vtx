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
    int rc = vtx_udp_load (vtx, FALSE);
    assert (rc == 0);

    //  Create DEALER socket and do broadcast connect to server
    void *client = vtx_socket (vtx, ZMQ_DEALER);
    assert (client);
    rc = vtx_connect (vtx, client, "udp://*:%d", 32000);
    assert (rc == 0);

    char *server = "not found";
    //  Ping server with messages until it responds, or we timeout
    uint64_t expiry = zclock_time () + 1000;
    while (zclock_time () < expiry) {
        //  Poll frequency is 500 msec
        zstr_send (client, "ICANHAZ?");
        zmq_pollitem_t items [] = { { client, 0, ZMQ_POLLIN, 0 } };
        int rc = zmq_poll (items, 1, 500 * ZMQ_POLL_MSEC);
        if (rc == -1)
            break;              //  Context has been shut down

        if (items [0].revents & ZMQ_POLLIN) {
            char *input = zstr_recv (client);
            if (input) {
                free (input);
                server = vtx_getmeta (vtx, client, "sender");
            }
            break;
        }
    }
    zclock_log ("I: server address: %s", server);
    vtx_destroy (&vtx);
    zctx_destroy (&ctx);
    return 0;
}
