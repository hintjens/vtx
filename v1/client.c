//
//  Example name client
//  Uses UDP virtual transport
//
#include "czmq.h"

static void
derp (char *s)
{
    perror (s);
    exit (1);
}

static void
s_udp (void *args, zctx_t *ctx, void *pipe)
{
    //  Create UDP socket
    int fd;
    if ((fd = socket (AF_INET, SOCK_DGRAM, IPPROTO_UDP)) == -1)
        derp ("socket");

    //  Enable broadcast mode
    int broadcast_on = 1;
    if (setsockopt (fd, SOL_SOCKET, SO_BROADCAST, &broadcast_on, sizeof (int)) == -1)
        derp ("setsockopt (SO_BROADCAST)");

    //  Set address to LAN broadcast
    struct sockaddr_in si_that = { 0 };
    si_that.sin_family = AF_INET;
    si_that.sin_port = htons (31000);
    si_that.sin_addr.s_addr = htonl (INADDR_BROADCAST);
    socklen_t si_len = sizeof (struct sockaddr_in);

    zmq_pollitem_t items [] = {
        { pipe, 0, ZMQ_POLLIN, 0 },
        { NULL, fd, ZMQ_POLLIN, 0 }
    };
    while (TRUE) {
        int rc = zmq_poll (items, 2, -1);
        if (rc == -1)
            break;              //  Context has been shut down

        if (items [0].revents & ZMQ_POLLIN) {
            //  Handle only single-part messages for now
            zframe_t *frame = zframe_recv (pipe);
            assert (!zframe_more (frame));
            assert (inet_ntoa (si_that.sin_addr));

            if (sendto (fd, zframe_data (frame), zframe_size (frame), 0, &si_that, si_len) == -1)
                derp ("sendto");
            zframe_destroy (&frame);
        }
        if (items [1].revents & ZMQ_POLLIN) {
            //  Activity on UDP socket
            char buffer [250];
            ssize_t size = recvfrom (fd, buffer, 250, 0, &si_that, &si_len);
            if (size == -1)
                derp ("recvfrom");

            printf ("Received from %s:%d\n", inet_ntoa (si_that.sin_addr), ntohs (si_that.sin_port));
            zframe_t *frame = zframe_new (buffer, size);
            zframe_send (&frame, pipe, 0);
        }
    }
    close (fd);
}

int main (void)
{
    zctx_t *ctx = zctx_new ();

    void *pipe = zthread_fork (ctx, s_udp, NULL);
    while (!zctx_interrupted) {
        //  Look for name server anywhere on LAN
        zstr_send (pipe, "hello?");
        puts ("hello?");

        //  Wait for at most 1000msec for reply before retrying
        zmq_pollitem_t items [] = { { pipe, 0, ZMQ_POLLIN, 0 } };
        int rc = zmq_poll (items, 1, 1000 * ZMQ_POLL_MSEC);
        if (rc == -1)
            break;              //  Context has been shut down

        if (items [0].revents & ZMQ_POLLIN) {
            char *input = zstr_recv (pipe);
            puts (input);
            free (input);
            sleep (1);
        }
    }
    zctx_destroy (&ctx);
    return 0;
}
