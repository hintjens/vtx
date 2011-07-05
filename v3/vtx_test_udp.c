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
        void *reply = zthread_fork (ctx, test_udp_rep, NULL);
        void *request = zthread_fork (ctx, test_udp_req, NULL);
        sleep (1);
        zstr_send (request, "END");
        free (zstr_recv (request));
        zstr_send (reply, "END");
        free (zstr_recv (reply));
    }
    //  Run push-pull tests
    {
        zclock_log ("I: testing push-pull over UDP...");
        void *pull1 = zthread_fork (ctx, test_udp_pull, NULL);
        void *pull2 = zthread_fork (ctx, test_udp_pull, NULL);
        void *push = zthread_fork (ctx, test_udp_push, NULL);
        sleep (1);
        zstr_send (push, "END");
        free (zstr_recv (push));
        zstr_send (pull1, "END");
        free (zstr_recv (pull1));
        zstr_send (pull2, "END");
        free (zstr_recv (pull2));
    }
    zctx_destroy (&ctx);
    return 0;
}


static void test_udp_req (void *args, zctx_t *ctx, void *pipe)
{
    vtx_t *vtx = vtx_new (ctx);
    int rc = vtx_udp_load (vtx, FALSE);
    assert (rc == 0);

    void *client = vtx_socket (vtx, ZMQ_REQ);
    assert (client);
    rc = vtx_connect (vtx, client, "udp://*:%d", 32000);
    assert (rc == 0);
    int sent = 0;
    int recd = 0;

    while (!zctx_interrupted) {
        zstr_send (client, "ICANHAZ?");
        sent++;
        zmq_pollitem_t items [] = {
            { pipe, 0, ZMQ_POLLIN, 0 },
            { client, 0, ZMQ_POLLIN, 0 }
        };
        int rc = zmq_poll (items, 2, 500 * ZMQ_POLL_MSEC);
        if (rc == -1)
            break;              //  Context has been shut down
        if (items [1].revents & ZMQ_POLLIN) {
            free (zstr_recv (client));
            recd++;
        }
        else {
            vtx_close (vtx, client);
            client = vtx_socket (vtx, ZMQ_REQ);
            rc = vtx_connect (vtx, client, "udp://*:%d", 32000);
        }
        if (items [0].revents & ZMQ_POLLIN) {
            free (zstr_recv (pipe));
            zstr_send (pipe, "OK");
            break;
        }
    }
    zclock_log ("I: REQ: sent=%d recd=%d", sent, recd);
    vtx_destroy (&vtx);
}

static void test_udp_rep (void *args, zctx_t *ctx, void *pipe)
{
    vtx_t *vtx = vtx_new (ctx);
    int rc = vtx_udp_load (vtx, FALSE);
    assert (rc == 0);

    void *server = vtx_socket (vtx, ZMQ_REP);
    assert (server);
    rc = vtx_bind (vtx, server, "udp://*:%d", 32000);
    assert (rc == 0);
    int sent = 0;

    while (!zctx_interrupted) {
        zmq_pollitem_t items [] = {
            { pipe, 0, ZMQ_POLLIN, 0 },
            { server, 0, ZMQ_POLLIN, 0 }
        };
        int rc = zmq_poll (items, 2, 500 * ZMQ_POLL_MSEC);
        if (rc == -1)
            break;              //  Context has been shut down
        if (items [1].revents & ZMQ_POLLIN) {
            free (zstr_recv (server));
            zstr_send (server, "CHEEZBURGER");
            sent++;
        }
        if (items [0].revents & ZMQ_POLLIN) {
            free (zstr_recv (pipe));
            zstr_send (pipe, "OK");
            break;
        }
    }
    zclock_log ("I: REP: sent=%d", sent);
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
    int rc = vtx_udp_load (vtx, TRUE);
    assert (rc == 0);

    void *collector = vtx_socket (vtx, ZMQ_PULL);
    assert (collector);
    rc = vtx_connect (vtx, collector, "udp://*:%d", 32002);
    assert (rc == 0);
    int recd = 0;

    while (!zctx_interrupted) {
        zmq_pollitem_t items [] = {
            { pipe, 0, ZMQ_POLLIN, 0 },
            { collector, 0, ZMQ_POLLIN, 0 }
        };
        int rc = zmq_poll (items, 2, 500 * ZMQ_POLL_MSEC);
        if (rc == -1)
            break;              //  Context has been shut down
        if (items [0].revents & ZMQ_POLLIN) {
            free (zstr_recv (pipe));
            zstr_send (pipe, "OK");
            break;
        }
        if (items [1].revents & ZMQ_POLLIN) {
            free (zstr_recv (collector));
            recd++;
        }
    }
    zclock_log ("I: PULL: recd=%d", recd);
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
    rc = vtx_bind (vtx, ventilator, "udp://*:%d", 32002);
    assert (rc == 0);
    int sent = 0;

    while (!zctx_interrupted) {
        if (sent == 100000)
            sleep (1);
        else {
        zstr_sendf (ventilator, "NOM %04x", randof (0x10000));
        sent++;
        }
        char *end = zstr_recv_nowait (pipe);
        if (end) {
            free (end);
            zstr_send (pipe, "OK");
            break;
        }
    }
    zclock_log ("I: PUSH: sent=%d", sent);
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
