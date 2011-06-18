//
//  VTX test example, subscriber
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

    //  Create subscriber socket and connect to broadcast address
    void *subscriber = vtx_socket (vtx, ZMQ_SUB);
    assert (subscriber);
    rc = vtx_connect (vtx, subscriber, "udp://*:32000");
    assert (rc == 0);

    while (!zctx_interrupted) {
        char *input = zstr_recv (subscriber);
        if (!input)
            break;          //  Interrupted
        zclock_log (input);
    }
    vtx_destroy (&vtx);
    zctx_destroy (&ctx);
    return 0;
}
