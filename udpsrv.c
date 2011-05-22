#include "czmq.h"

#define BUFLEN 512
#define NPACK 10
#define PORT 9930

void derp (char *s)
{
    perror (s);
    exit (1);
}

int main (void)
{
    struct sockaddr_in si_me, si_other;
    socklen_t slen = sizeof (si_other);
    char buffer [BUFLEN];
    int s, i;

    if ((s = socket (AF_INET, SOCK_DGRAM, IPPROTO_UDP)) == -1)
        derp ("socket");

    memset ((char *) &si_me, 0, sizeof (si_me));
    si_me.sin_family = AF_INET;
    si_me.sin_port = htons (PORT);
    si_me.sin_addr.s_addr = htonl (INADDR_ANY);
    if (bind (s, &si_me, sizeof (si_me)) == -1)
        derp ("bind");

    while (TRUE) {
        for (i = 0; i < NPACK; i++) {
            if (recvfrom (s, buffer, BUFLEN, 0, &si_other, &slen) == -1)
                derp ("recvfrom");

            printf ("Received from %s:%d\nData: %s\n",
                inet_ntoa (si_other.sin_addr),
                ntohs (si_other.sin_port), buffer);

            sprintf (buffer, "This is response %d", i);
            if (sendto (s, buffer, BUFLEN, 0, &si_other, slen) == -1)
                derp ("sendto");
        }
    }
    close (s);
    return 0;
}
