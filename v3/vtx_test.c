//
//  VTX test example
//
//  This file is part of VTX, the 0MQ virtual transport interface:
//  http://vtx.zeromq.org.

#include "vtx.h"
#include "vtx_udp.h"

static void
client_thread (void *args, zctx_t *ctx, void *pipe)
{
    //  Initialize virtual transport interface
    vtx_t *vtx = vtx_new (ctx);
    int rc = vtx_register (vtx, "udp", vtx_udp_driver);
    assert (rc == 0);

    //  Create client socket and connect to broadcast address
    void *client = vtx_socket (vtx, VTX_RAW);
    rc = vtx_connect (vtx, client, "udp://*:32000");
    assert (rc == 0);

    while (TRUE) {
        //  Look for name server anywhere on LAN
        zstr_send (client, "hello?");
        puts ("hello?");

        //  Wait for at most 1000msec for reply before retrying
        zmq_pollitem_t items [] = { { client, 0, ZMQ_POLLIN, 0 } };
        int rc = zmq_poll (items, 1, 1000 * ZMQ_POLL_MSEC);
        if (rc == -1)
            break;              //  Context has been shut down

        if (items [0].revents & ZMQ_POLLIN) {
            char *input = zstr_recv (client);
            puts (input);
            free (input);
            sleep (1);
        }
    }
    vtx_destroy (&vtx);
}


static void
server_thread (void *args, zctx_t *ctx, void *pipe)
{
    //  Initialize virtual transport interface
    vtx_t *vtx = vtx_new (ctx);
    int rc = vtx_register (vtx, "udp", vtx_udp_driver);
    assert (rc == 0);

    //  Create server socket and bind to all network interfaces
    void *server = vtx_socket (vtx, VTX_RAW);
    rc = vtx_bind (vtx, server, "udp://*:32000");
    assert (rc == 0);

    while (TRUE) {
        char *input = zstr_recv (server);
        if (!input)
            break;              //  Interrupted
        puts (input);
        free (input);
        zstr_send (server, "ack");
    }
    vtx_destroy (&vtx);
}


int main (void)
{
    //  Initialize 0MQ context and virtual transport interface
    zctx_t *ctx = zctx_new ();
    vtx_t *vtx = vtx_new (ctx);
    int rc = vtx_register (vtx, "udp", vtx_udp_driver);
    assert (rc == 0);

    //  Test unknown transport
    void *dummy = vtx_socket (vtx, VTX_RAW);
    rc = vtx_connect (vtx, dummy, "unknown://127.0.0.1:5555");
    assert (rc == -1);

    //  Run client and server threads
    zthread_fork (ctx, client_thread, NULL);
    zthread_fork (ctx, server_thread, NULL);

    //  Wait forever until Ctrl-C used
    while (!zctx_interrupted)
        sleep (1);

    vtx_destroy (&vtx);
    zctx_destroy (&ctx);
    return 0;
}
