//
//  VTX UDP test bench
//
//  This file is part of VTX, the 0MQ virtual transport interface:
//  http://vtx.zeromq.org.

#include "vtx.c"
#include "vtx_udp.c"

//  These are the various test tasks

static void test_udp_req    (void *args, zctx_t *ctx, void *pipe);
static void test_udp_rep    (void *args, zctx_t *ctx, void *pipe);
static void test_udp_dealer (void *args, zctx_t *ctx, void *pipe);
static void test_udp_router (void *args, zctx_t *ctx, void *pipe);
static void test_udp_pull   (void *args, zctx_t *ctx, void *pipe);
static void test_udp_push   (void *args, zctx_t *ctx, void *pipe);
static void test_udp_pub    (void *args, zctx_t *ctx, void *pipe);
static void test_udp_sub    (void *args, zctx_t *ctx, void *pipe);
static void test_udp_pair   (void *args, zctx_t *ctx, void *pipe);

int main (void)
{
    //  Initialize 0MQ context and virtual transport interface
    zctx_t *ctx = zctx_new ();
    assert (ctx);

    //  Run request-reply tests
    {
        zclock_log ("I: testing request-reply over UDP...");
        void *request = zthread_fork (ctx, test_udp_req, NULL);
        void *reply = zthread_fork (ctx, test_udp_rep, NULL);
puts ("Forked... sleeping");
        sleep (2);
puts ("Awake!");
        zstr_send (request, "END");
puts ("sent to request");
        zstr_send (reply, "END");
puts ("sent to reply");
    }
    //  Run push-pull tests
    {
        zclock_log ("I: testing push-pull over UDP...");
        void *pull1 = zthread_fork (ctx, test_udp_pull, NULL);
        void *pull2 = zthread_fork (ctx, test_udp_pull, NULL);
        void *push = zthread_fork (ctx, test_udp_push, NULL);
        sleep (2);
        zstr_send (pull1, "END");
        zstr_send (pull2, "END");
        zstr_send (push, "END");
    }

    zctx_destroy (&ctx);
    return 0;
}


static void test_udp_req (void *args, zctx_t *ctx, void *pipe)
{
    vtx_t *vtx = vtx_new (ctx);
    int rc = vtx_udp_load (vtx, FALSE);
    assert (rc == 0);

    void *input = vtx_socket (vtx, ZMQ_REQ);
    assert (input);
    rc = vtx_connect (vtx, input, "udp://*:%d", 32000);
    size_t sent = 0;
    size_t recd = 0;

puts ("before loop");
    while (!zctx_interrupted) {
puts ("before send");
        zstr_send (input, "ICANHAZ?");
        sent++;
        zmq_pollitem_t items [] = {
            { pipe, 0, ZMQ_POLLIN, 0 },
            { input, 0, ZMQ_POLLIN, 0 }
        };
puts ("before poll");
        int rc = zmq_poll (items, 2, 500 * ZMQ_POLL_MSEC);
        if (rc == -1)
            break;              //  Context has been shut down
puts ("after poll");
        if (items [0].revents & ZMQ_POLLIN) {
            free (zstr_recv (pipe));
            break;
        }
        else
        if (items [1].revents & ZMQ_POLLIN) {
            free (zstr_recv (input));
            recd++;
        }
        else {
puts ("reset socket");
            vtx_close (vtx, input);
            input = vtx_socket (vtx, ZMQ_REQ);
            rc = vtx_connect (vtx, input, "udp://*:%d", 32000);
        }
    }
    vtx_destroy (&vtx);
}

static void test_udp_rep (void *args, zctx_t *ctx, void *pipe)
{
    vtx_t *vtx = vtx_new (ctx);
    int rc = vtx_udp_load (vtx, FALSE);
    assert (rc == 0);

    free (zstr_recv (pipe));

    vtx_destroy (&vtx);
}

static void test_udp_dealer (void *args, zctx_t *ctx, void *pipe)
{
    vtx_t *vtx = vtx_new (ctx);
    int rc = vtx_udp_load (vtx, FALSE);
    assert (rc == 0);

    vtx_destroy (&vtx);
}

static void test_udp_router (void *args, zctx_t *ctx, void *pipe)
{
    vtx_t *vtx = vtx_new (ctx);
    int rc = vtx_udp_load (vtx, FALSE);
    assert (rc == 0);

    vtx_destroy (&vtx);
}

static void test_udp_pull (void *args, zctx_t *ctx, void *pipe)
{
    vtx_t *vtx = vtx_new (ctx);
    int rc = vtx_udp_load (vtx, FALSE);
    assert (rc == 0);

    free (zstr_recv (pipe));

    vtx_destroy (&vtx);
}

static void test_udp_push (void *args, zctx_t *ctx, void *pipe)
{
    vtx_t *vtx = vtx_new (ctx);
    int rc = vtx_udp_load (vtx, FALSE);
    assert (rc == 0);

    //  Create ventilator socket and bind to all network interfaces
    void *ventilator = vtx_socket (vtx, ZMQ_PUSH);
    assert (ventilator);
    rc = vtx_bind (vtx, ventilator, "udp://*:32000");
    assert (rc == 0);

    while (!zctx_interrupted) {
        zstr_sendf (ventilator, "DATA %04x", randof (0x10000));
        char *end = zstr_recv_nowait (pipe);
        if (end) {
            free (end);
            break;
        }
    }
    vtx_destroy (&vtx);
}

static void test_udp_pub (void *args, zctx_t *ctx, void *pipe)
{
    vtx_t *vtx = vtx_new (ctx);
    int rc = vtx_udp_load (vtx, FALSE);
    assert (rc == 0);

    vtx_destroy (&vtx);
}

static void test_udp_sub (void *args, zctx_t *ctx, void *pipe)
{
    vtx_t *vtx = vtx_new (ctx);
    int rc = vtx_udp_load (vtx, FALSE);
    assert (rc == 0);

    vtx_destroy (&vtx);
}

static void test_udp_pair (void *args, zctx_t *ctx, void *pipe)
{
    vtx_t *vtx = vtx_new (ctx);
    int rc = vtx_udp_load (vtx, FALSE);
    assert (rc == 0);

    vtx_destroy (&vtx);
}
