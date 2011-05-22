//
//  Performance client for UDP virtual transport
//
#include "zvudp.c"

int main (void)
{
    zvudp_t *zvudp = zvudp_new ();
    void *client = zvudp_socket (zvudp);
    zvudp_connect (zvudp, "127.0.0.1", 31000);

    //  Send test set to server
    puts ("Sending test set...");
    zstr_send (client, "START");
    int count;
    for (count = 0; count < 1000000; count++) {
        if (zctx_interrupted)
            break;
        zstr_send (client, "This is a test");
    }
    zstr_send (client, "END");

    //  Wait for server to confirm
    puts ("Waiting for server...");
    char *input = zstr_recv (client);
    if (input) {
        puts (input);
        free (input);
    }
    zvudp_destroy (&zvudp);
    return 0;
}
