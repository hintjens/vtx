//
//  VTX test example, PAIR client
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

    //  Create client socket and connect to broadcast address
    void *client = vtx_socket (vtx, ZMQ_PAIR);
    assert (client);
    rc = vtx_connect (vtx, client, "udp://*:32000");
    assert (rc == 0);

    while (!zctx_interrupted) {
        zmsg_t *msg = zmsg_recv (client);
        if (!msg)
            break;          //  Interrupted
        zmsg_dump (msg);
        zmsg_destroy (&msg);
    }
    vtx_destroy (&vtx);
    zctx_destroy (&ctx);
    return 0;
}
