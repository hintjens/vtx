//
//  Example name client
//  Uses UDP virtual transport
//
#include "zvudp.c"

int main (void)
{
    zvudp_t *zvudp = zvudp_new ();
    void *client = zvudp_socket (zvudp);
    zvudp_bind (zvudp, "*", 31000);

    while (!zctx_interrupted) {
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
    zvudp_destroy (&zvudp);
    return 0;
}
