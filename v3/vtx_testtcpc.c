//
//  VTX test example, server
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

    //  Create server socket and bind to all network interfaces
    void *server = vtx_socket (vtx, ZMQ_ROUTER);
    assert (server);
    rc = vtx_bind (vtx, server, "udp://*:32000");
    assert (rc == 0);

    while (!zctx_interrupted) {
        char *address = zstr_recv (server);
        if (!address)
            break;              //  Interrupted
        assert (zsockopt_rcvmore (server));
        char *input = zstr_recv (server);
        if (!input)
            break;              //  Interrupted
        zstr_sendm (server, address);
        zstr_send (server, "acknowledge");
        zclock_log ("S: acknowledge");
        free (address);
        free (input);
    }
    vtx_destroy (&vtx);

    zctx_destroy (&ctx);
    return 0;
}
