#include "czmq.h"

#define SRV_IP "255.255.255.255"
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
    struct sockaddr_in si_other;
    socklen_t slen = sizeof (si_other);
    char buffer [BUFLEN];
    int s, i;

    if ((s = socket (AF_INET, SOCK_DGRAM, IPPROTO_UDP)) == -1)
        derp ("socket");

    int broadcast_on = 1;
    if (setsockopt(s, SOL_SOCKET, SO_BROADCAST, &broadcast_on, sizeof (int)) == -1)
        derp ("setsockopt (SO_BROADCAST)");

    memset ((char *) &si_other, 0, sizeof (si_other));
    si_other.sin_family = AF_INET;
    si_other.sin_port = htons (PORT);

    if (inet_aton ("255.255.255.255", &si_other.sin_addr) == 0)
        derp ("inet_aton");

    for (i = 0; i < NPACK; i++) {
        printf ("Sending packet %d\n", i);
        sprintf (buffer, "This is request %d", i);
        if (sendto (s, buffer, BUFLEN, 0, &si_other, slen) == -1)
            derp ("sendto");

        if (recvfrom (s, buffer, BUFLEN, 0, &si_other, &slen) == -1)
            derp ("recvfrom");

        printf ("Received from %s:%d\nData: %s\n",
            inet_ntoa (si_other.sin_addr),
            ntohs (si_other.sin_port), buffer);
    }
    close (s);
    return 0;
}