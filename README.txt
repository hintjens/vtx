.set GIT=https://github.com/imatix/vtx
.sub 0MQ=ØMQ

# VTX - Virtual Transport Extension Layer for 0MQ

## Contents

.toc

## Overview

### Problem Statement

Currently, 0MQ (the core libzmq library) supports TCP, PGM, IPC, and inproc transports. While it is in theory possible to add new transports, in practice this has proven too difficult for contributors. There is no abstraction for transports in libzmq. Socket semantics are built on top of transports, but in such a way that adding new transports requires large-scale changes. Thus the set of transports has not changed since version 1.x of the product.

The difficulty of adding new transports has limited 0MQ's development in a major way. First, it has been impossible to experiment with new protocols over the TCP transport. Second, it has been impossible to create secure transports using SSL/TLS or SASL. Third, it has been hard to accurately bridge 0MQ over transports like HTTP.

VTX is meant to make it possible to extend 0MQ with user-space transports, called *drivers*. Our goal is to make it feasible to write drivers for any native transport protocol that is supported by the target operating system, can be accessed in C, and can be integrated into 0MQ's event polling (or provides its own event loop that can work on 0MQ file handles).

Transports we would like to explore include: SSL/TLS, SASL-secured TCP, UDP, DCCP, SCTP, and TCP over IPv6.

### Architecture

A VTX driver is a C task that integrates into a 0MQ application as an *in-process protocol bridge*:

[diagram]
                        +-------------+
                        |             |
                        | VTX Manager |
                        |             |
                        +-------------+
                           ^      ^
                  API      |      |       DPI
           /---------------/      \--------------\
           |                                     |
           v                                     v
    +-------------+--------+      +--------+------------+
    |             |        |      |        |            |       Native
    | Application | inproc |<---->| inproc | VTX driver |<----> transport
    |             |        |      |        |            |       protocol
    +-------------+--------+      +--------+------------+
           ^                                     ^
           |                                     |
           \---------------\      /--------------/
                 Call      |      |     Call
                           v      v
                        +-------------+
                        |             |
                        |   libzmq    |
                        |             |
                        +-------------+

[/diagram]

VTX is currently accessible only to C/C++ applications. To make VTX accessible to other languages would require wrapping the VTX API (explained below) in the same way as the libzmq API is wrapped today.

### Ownership and License

VTX is built by iMatix Corporation and maintained by Pieter Hintjens. Its authors are listed in the AUTHORS file. The authors of VTX grant you use of this software under the terms of the GNU Lesser General Public License (LGPL). For details see the files `COPYING` and `COPYING.LESSER` in this directory.

### Contributing

To submit an issue use the [issue tracker](http://github.com/pieter/vtx/issues). All discussion happens on the [zeromq-dev](zeromq-dev@lists.zeromq.org) list or #zeromq IRC channel at irc.freenode.net. The proper way to contribute is to fork this repository, make your changes, and submit a pull request.

## Using VTX

### Status

VTX is currently a *concept* project, not ready for use. Any aspect of it may change before the product is usable and there is no guarantee that VTX will actually make it to production.

The current VTX concept source code is in the v3 subdirectory.

### Dependencies

VTX depends on the [czmq C language binding](http://czmq.zeromq.org). Please build and install the latest czmq master from github before building and installing VTX.

### Building and Installing

VTX does not yet use autotools. To build, manually compile & link the C main programs. You can use the 'c' script from czmq:

    c -l -lzmq -lczmq server client

### The VTX API

The VTX API has these methods:

    vtx_t *
        vtx_new (zctx_t *ctx);
    void
        vtx_destroy (vtx_t **self_p);
    void *
        vtx_socket (vtx_t *self, int type);
    int
        vtx_close (vtx_t *self, void *socket);
    int
        vtx_bind (vtx_t *self, void *socket, char *endpoint);
    int
        vtx_connect (vtx_t *self, void *socket, char *endpoint);
    int
        vtx_register (vtx_t *self, char *protocol,
                      zthread_attached_fn *driver_fn);

Here is a sample client and server that use VTX and the vtx_udp driver:

    static void
    client_task (void *args, zctx_t *ctx, void *pipe)
    {
        //  Initialize virtual transport interface
        vtx_t *vtx = vtx_new (ctx);
        int rc = vtx_register (vtx, "udp", vtx_udp_driver);
        assert (rc == 0);

        //  Create client socket and connect to broadcast address
        void *client = vtx_socket (vtx, ZMQ_REQ);
        rc = vtx_connect (vtx, client, "udp://127.0.0.255:32000");
        assert (rc == 0);

        while (TRUE) {
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
        vtx_destroy (&vtx);
    }

    static void
    server_task (void *args, zctx_t *ctx, void *pipe)
    {
        //  Initialize virtual transport interface
        vtx_t *vtx = vtx_new (ctx);
        int rc = vtx_register (vtx, "udp", vtx_udp_driver);
        assert (rc == 0);

        //  Create server socket and bind to all network interfaces
        void *server = vtx_socket (vtx, ZMQ_REP);
        rc = vtx_bind (vtx, server, "udp://*:32000");
        assert (rc == 0);

        while (TRUE) {
            char *input = zstr_recv (server);
            if (!input)
                break;              //  Interrupted
            puts (input);
            free (input);
            zstr_send (server, "ack");
        }
        vtx_destroy (&vtx);
    }

## Driver Design

A driver exists as a background *attached* thread. VTX communicates with the driver over an inproc *control* socket pair, and the application sends and receives messages over an inproc *data* socket pair.

The driver reimplements the 0MQ socket semantics, in effect emulating the behaviour of the built-in socket types, over whatever transport protocol the driver supports. This makes driver development a fairly major task, but still less difficult than adding transports to libzmq. We hope that the driver socket emulation can be partially lifted into the VTX manager (reused by all drivers).

Each driver implements its own wire-level protocol, which can be an extension of the standard libzmq wire-level protocols. For example the planned UDP wire-level protocol includes heartbeating, request-reply retries, socket validation, and other experimental semantics.

## This Document

This document is originally at README.txt and is built using [gitdown](http://github.com/imatix/gitdown).


