//
//  VTX TCP test, push client
//
//  This file is part of VTX, the 0MQ virtual transport interface:
//  http://vtx.zeromq.org.

#include "vtx.c"
#include "vtx_tcp.c"

int main (void)
{
    //  Initialize 0MQ context and virtual transport interface
    zctx_t *ctx = zctx_new ();

    //  Initialize virtual transport interface
    vtx_t *vtx = vtx_new (ctx);
    int rc = vtx_tcp_load (vtx);
    assert (rc == 0);

    //  Create ventilator socket and bind to all network interfaces
    void *ventilator = vtx_socket (vtx, ZMQ_PUSH);
    assert (ventilator);
    rc = vtx_bind (vtx, ventilator, "tcp://*:32000");
    assert (rc == 0);

    while (!zctx_interrupted) {
        zstr_sendf (ventilator, "DATA %04x", randof (0x10000));
        sleep (1);
    }
    vtx_destroy (&vtx);

    zctx_destroy (&ctx);
    return 0;
}
