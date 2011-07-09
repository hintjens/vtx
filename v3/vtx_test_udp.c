//
//  VTX UDP test bench
//
//  This file is part of VTX, the 0MQ virtual transport interface:
//  http://vtx.zeromq.org.

#include "vtx.c"
#include "vtx_udp.c"

//  These are the various test tasks

static void test_udp_req        (void *args, zctx_t *ctx, void *pipe);
static void test_udp_rep        (void *args, zctx_t *ctx, void *pipe);
static void test_udp_dealer_srv (void *args, zctx_t *ctx, void *pipe);
static void test_udp_dealer_cli (void *args, zctx_t *ctx, void *pipe);
static void test_udp_router     (void *args, zctx_t *ctx, void *pipe);
static void test_udp_pull       (void *args, zctx_t *ctx, void *pipe);
static void test_udp_push       (void *args, zctx_t *ctx, void *pipe);
static void test_udp_pub        (void *args, zctx_t *ctx, void *pipe);
static void test_udp_sub        (void *args, zctx_t *ctx, void *pipe);
static void test_udp_pair       (void *args, zctx_t *ctx, void *pipe);

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
        //  Send port number to use to each thread
        zstr_send (request, "32000");
        zstr_send (reply, "32000");
        sleep (1);
        zstr_send (request, "END");
        free (zstr_recv (request));
        zstr_send (reply, "END");
        free (zstr_recv (reply));
    }
    //  Run request-router tests
    {
        zclock_log ("I: testing request-router over UDP...");
        void *request = zthread_fork (ctx, test_udp_req, NULL);
        void *router = zthread_fork (ctx, test_udp_router, NULL);
        //  Send port number to use to each thread
        zstr_send (request, "32001");
        zstr_send (router, "32001");
        sleep (1);
        zstr_send (request, "END");
        free (zstr_recv (request));
        zstr_send (router, "END");
        free (zstr_recv (router));
    }
    //  Run request-dealer tests
    {
        zclock_log ("I: testing request-dealer over UDP...");
        void *request = zthread_fork (ctx, test_udp_req, NULL);
        void *dealer = zthread_fork (ctx, test_udp_dealer_srv, NULL);
        //  Send port number to use to each thread
        zstr_send (request, "32002");
        zstr_send (dealer, "32002");
        sleep (1);
        zstr_send (request, "END");
        free (zstr_recv (request));
        zstr_send (dealer, "END");
        free (zstr_recv (dealer));
    }
    //  Run dealer-router tests
    {
        zclock_log ("I: testing dealer-router over UDP...");
        void *dealer = zthread_fork (ctx, test_udp_dealer_cli, NULL);
        void *router = zthread_fork (ctx, test_udp_router, NULL);
        //  Send port number to use to each thread
        zstr_send (dealer, "32003");
        zstr_send (router, "32003");
        sleep (1);
        zstr_send (dealer, "END");
        free (zstr_recv (dealer));
        zstr_send (router, "END");
        free (zstr_recv (router));
    }
    //  Run push-pull tests
    {
        zclock_log ("I: testing push-pull over UDP...");
        void *pull1 = zthread_fork (ctx, test_udp_pull, NULL);
        void *pull2 = zthread_fork (ctx, test_udp_pull, NULL);
        void *push = zthread_fork (ctx, test_udp_push, NULL);
        //  Send port number to use to each thread
        zstr_send (pull1, "32004");
        zstr_send (pull2, "32004");
        zstr_send (push, "32004");
        sleep (1);
        zstr_send (push, "END");
        free (zstr_recv (push));
        zstr_send (pull1, "END");
        free (zstr_recv (pull1));
        zstr_send (pull2, "END");
        free (zstr_recv (pull2));
    }
    //  Run pub-sub tests
    {
        zclock_log ("I: testing pub-sub over UDP...");
        void *sub1 = zthread_fork (ctx, test_udp_sub, NULL);
        void *sub2 = zthread_fork (ctx, test_udp_sub, NULL);
        void *pub = zthread_fork (ctx, test_udp_pub, NULL);
        //  Send port number to use to each thread
        zstr_send (sub1, "32005");
        zstr_send (sub2, "32005");
        zstr_send (pub, "32005");
        sleep (1);
        zstr_send (pub, "END");
        free (zstr_recv (pub));
        zstr_send (sub1, "END");
        free (zstr_recv (sub1));
        zstr_send (sub2, "END");
        free (zstr_recv (sub2));
    }
    zctx_destroy (&ctx);
    return 0;
}


static void
test_udp_req (void *args, zctx_t *ctx, void *pipe)
{
    vtx_t *vtx = vtx_new (ctx);
    int rc = vtx_udp_load (vtx, FALSE);
    assert (rc == 0);
    char *port = zstr_recv (pipe);

    void *client = vtx_socket (vtx, ZMQ_REQ);
    assert (client);
    rc = vtx_connect (vtx, client, "udp://*:%s", port);
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
        if (items [0].revents & ZMQ_POLLIN) {
            free (zstr_recv (pipe));
            zstr_send (pipe, "OK");
            break;
        }
        if (items [1].revents & ZMQ_POLLIN) {
            free (zstr_recv (client));
            recd++;
        }
        else {
            //  No response, close socket and start a new one
            vtx_close (vtx, client);
            client = vtx_socket (vtx, ZMQ_REQ);
            rc = vtx_connect (vtx, client, "udp://*:%s", port);
        }
    }
    zclock_log ("I: REQ: sent=%d recd=%d", sent, recd);
    free (port);
    vtx_close (vtx, client);
    vtx_destroy (&vtx);
}

static void
test_udp_rep (void *args, zctx_t *ctx, void *pipe)
{
    vtx_t *vtx = vtx_new (ctx);
    int rc = vtx_udp_load (vtx, FALSE);
    assert (rc == 0);
    char *port = zstr_recv (pipe);

    void *server = vtx_socket (vtx, ZMQ_REP);
    assert (server);
    rc = vtx_bind (vtx, server, "udp://*:%s", port);
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
    free (port);
    vtx_close (vtx, server);
    vtx_destroy (&vtx);
}

static void
test_udp_router (void *args, zctx_t *ctx, void *pipe)
{
    vtx_t *vtx = vtx_new (ctx);
    int rc = vtx_udp_load (vtx, FALSE);
    assert (rc == 0);
    char *port = zstr_recv (pipe);

    void *router = vtx_socket (vtx, ZMQ_ROUTER);
    assert (router);
    rc = vtx_bind (vtx, router, "udp://*:%s", port);
    assert (rc == 0);
    int sent = 0;

    while (!zctx_interrupted) {
        zmq_pollitem_t items [] = {
            { pipe, 0, ZMQ_POLLIN, 0 },
            { router, 0, ZMQ_POLLIN, 0 }
        };
        int rc = zmq_poll (items, 2, 500 * ZMQ_POLL_MSEC);
        if (rc == -1)
            break;              //  Context has been shut down
        if (items [1].revents & ZMQ_POLLIN) {
            char *address = zstr_recv (router);
            free (zstr_recv (router));
            zstr_sendm (router, address);
            zstr_send (router, "CHEEZBURGER");
            free (address);
            sent++;
        }
        if (items [0].revents & ZMQ_POLLIN) {
            free (zstr_recv (pipe));
            zstr_send (pipe, "OK");
            break;
        }
    }
    free (port);
    zclock_log ("I: ROUTER: sent=%d", sent);
    vtx_close (vtx, router);
    vtx_destroy (&vtx);
}

static void
test_udp_dealer_srv (void *args, zctx_t *ctx, void *pipe)
{
    vtx_t *vtx = vtx_new (ctx);
    int rc = vtx_udp_load (vtx, FALSE);
    assert (rc == 0);
    char *port = zstr_recv (pipe);

    void *dealer = vtx_socket (vtx, ZMQ_DEALER);
    assert (dealer);
    rc = vtx_bind (vtx, dealer, "udp://*:%s", port);
    assert (rc == 0);
    int sent = 0;

    while (!zctx_interrupted) {
        zmq_pollitem_t items [] = {
            { pipe, 0, ZMQ_POLLIN, 0 },
            { dealer, 0, ZMQ_POLLIN, 0 }
        };
        int rc = zmq_poll (items, 2, 500 * ZMQ_POLL_MSEC);
        if (rc == -1)
            break;              //  Context has been shut down
        if (items [1].revents & ZMQ_POLLIN) {
            free (zstr_recv (dealer));
            zstr_send (dealer, "CHEEZBURGER");
            sent++;
        }
        if (items [0].revents & ZMQ_POLLIN) {
            free (zstr_recv (pipe));
            zstr_send (pipe, "OK");
            break;
        }
    }
    zclock_log ("I: DEALER: sent=%d", sent);
    free (port);
    vtx_close (vtx, dealer);
    vtx_destroy (&vtx);
}

static void
test_udp_dealer_cli (void *args, zctx_t *ctx, void *pipe)
{
    vtx_t *vtx = vtx_new (ctx);
    int rc = vtx_udp_load (vtx, FALSE);
    assert (rc == 0);
    char *port = zstr_recv (pipe);

    void *dealer = vtx_socket (vtx, ZMQ_DEALER);
    assert (dealer);
    rc = vtx_connect (vtx, dealer, "udp://*:%s", port);
    assert (rc == 0);
    int sent = 0;
    int recd = 0;

    while (!zctx_interrupted) {
        zstr_send (dealer, "ICANHAZ?");
        sent++;
        char *reply = zstr_recv_nowait (dealer);
        if (reply) {
            recd++;
            free (reply);
        }
        char *end = zstr_recv_nowait (pipe);
        if (end) {
            free (end);
            zstr_send (pipe, "OK");
            break;
        }
    }
    zclock_log ("I: DEALER: sent=%d recd=%d", sent, recd);
    free (port);
    vtx_close (vtx, dealer);
    vtx_destroy (&vtx);
}


static void
test_udp_pull (void *args, zctx_t *ctx, void *pipe)
{
    vtx_t *vtx = vtx_new (ctx);
    int rc = vtx_udp_load (vtx, FALSE);
    assert (rc == 0);
    char *port = zstr_recv (pipe);

    void *collector = vtx_socket (vtx, ZMQ_PULL);
    assert (collector);
    rc = vtx_connect (vtx, collector, "udp://*:%s", port);
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
    vtx_close (vtx, collector);
    vtx_destroy (&vtx);
}

static void
test_udp_push (void *args, zctx_t *ctx, void *pipe)
{
    vtx_t *vtx = vtx_new (ctx);
    int rc = vtx_udp_load (vtx, FALSE);
    assert (rc == 0);
    char *port = zstr_recv (pipe);

    //  Create ventilator socket and bind to all network interfaces
    void *ventilator = vtx_socket (vtx, ZMQ_PUSH);
    assert (ventilator);
    rc = vtx_bind (vtx, ventilator, "udp://*:%s", port);
    assert (rc == 0);
    int sent = 0;

    while (!zctx_interrupted) {
        zstr_sendf (ventilator, "NOM %04x", randof (0x10000));
        sent++;
        char *end = zstr_recv_nowait (pipe);
        if (end) {
            free (end);
            zstr_send (pipe, "OK");
            break;
        }
    }
    zclock_log ("I: PUSH: sent=%d", sent);
    free (port);
    vtx_close (vtx, ventilator);
    vtx_destroy (&vtx);
}

static void
test_udp_pub (void *args, zctx_t *ctx, void *pipe)
{
    vtx_t *vtx = vtx_new (ctx);
    int rc = vtx_udp_load (vtx, FALSE);
    assert (rc == 0);
    char *port = zstr_recv (pipe);

    //  Create publisher socket and bind to all network interfaces
    void *publisher = vtx_socket (vtx, ZMQ_PUB);
    assert (publisher);
    rc = vtx_bind (vtx, publisher, "udp://*:%s", port);
    assert (rc == 0);
    int sent = 0;

    while (!zctx_interrupted) {
        zstr_sendf (publisher, "NOM %04x", randof (0x10000));
        sent++;
        char *end = zstr_recv_nowait (pipe);
        if (end) {
            free (end);
            zstr_send (pipe, "OK");
            break;
        }
    }
    zclock_log ("I: PUB: sent=%d", sent);
    free (port);
    vtx_close (vtx, publisher);
    vtx_destroy (&vtx);
}

static void
test_udp_sub (void *args, zctx_t *ctx, void *pipe)
{
    vtx_t *vtx = vtx_new (ctx);
    int rc = vtx_udp_load (vtx, FALSE);
    assert (rc == 0);
    char *port = zstr_recv (pipe);

    void *subscriber = vtx_socket (vtx, ZMQ_SUB);
    assert (subscriber);
    rc = vtx_connect (vtx, subscriber, "udp://*:%s", port);
    assert (rc == 0);
    int recd = 0;

    while (!zctx_interrupted) {
        zmq_pollitem_t items [] = {
            { pipe, 0, ZMQ_POLLIN, 0 },
            { subscriber, 0, ZMQ_POLLIN, 0 }
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
            free (zstr_recv (subscriber));
            recd++;
        }
    }
    zclock_log ("I: SUB: recd=%d", recd);
    free (port);
    vtx_close (vtx, subscriber);
    vtx_destroy (&vtx);
}

static void
test_udp_pair (void *args, zctx_t *ctx, void *pipe)
{
    vtx_t *vtx = vtx_new (ctx);
    int rc = vtx_udp_load (vtx, FALSE);
    assert (rc == 0);
    char *port = zstr_recv (pipe);

    free (port);
    vtx_destroy (&vtx);
}
