//
//  Example name server
//  Uses UDP virtual transport
//
#include "zvudp.c"

int main (void)
{
    zvudp_t *zvudp = zvudp_new ();
    void *server = zvudp_socket (zvudp);
    zvudp_bind (zvudp, "*", 31000);

    while (!zctx_interrupted) {
        char *input = zstr_recv (server);
        if (!input)
            break;              //  Interrupted
        puts (input);
        free (input);
        zstr_send (server, "ack");
    }
    zvudp_destroy (&zvudp);
    return 0;
}
