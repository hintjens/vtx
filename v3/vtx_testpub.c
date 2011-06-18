//
//  VTX test example, publisher
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

    //  Create publisher socket and bind to all network interfaces
    void *publisher = vtx_socket (vtx, ZMQ_PUB);
    assert (publisher);
    rc = vtx_bind (vtx, publisher, "udp://*:32000");
    assert (rc == 0);

    while (!zctx_interrupted) {
        zstr_sendf (publisher, "DATA %04x", randof (0x10000));
        sleep (1);
    }
    vtx_destroy (&vtx);

    zctx_destroy (&ctx);
    return 0;
}
