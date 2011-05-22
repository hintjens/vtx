//
//  Performance server for UDP virtual transport
//
#include "zvudp.c"

int main (void)
{
    zvudp_t *zvudp = zvudp_new ();
    void *server = zvudp_socket (zvudp);
    zvudp_bind (zvudp, "127.0.0.1", 31000);

    puts ("Waiting for client...");
    char *start = zstr_recv (server);
    if (start) {
        assert (streq (start, "START"));
        int count = 0;
        puts ("Receiving test set...");
        while (!zctx_interrupted) {
            char *input = zstr_recv (server);
            if (!input)
                break;              //  Interrupted
            if (streq (input, "END")) {
                zstr_sendf (server, "%d messages received", count);
                free (input);
                break;
            }
            count++;
            free (input);
        }
        puts ("Finished");
    }
    zvudp_destroy (&zvudp);
    return 0;
}
