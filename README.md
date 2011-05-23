
<A name="toc1-3" title="zvtran - Virtual Userspace Transport layer for ØMQ" />
# zvtran - Virtual Userspace Transport layer for ØMQ

<A name="toc2-6" title="Contents" />
## Contents


**<a href="#toc2-11">Overview</a>**
&emsp;<a href="#toc3-14">Scope and Goals</a>
&emsp;<a href="#toc3-19">Ownership and License</a>
&emsp;<a href="#toc3-26">Contributing</a>

**<a href="#toc2-33">Using zvtran</a>**
&emsp;<a href="#toc3-36">Dependencies</a>
&emsp;<a href="#toc3-41">Building and Installing</a>

**<a href="#toc2-48">Design Notes</a>**
&emsp;<a href="#toc3-72">This Document</a>

<A name="toc2-11" title="Overview" />
## Overview

<A name="toc3-14" title="Scope and Goals" />
### Scope and Goals

zvtran is a conceptual project aimed to explore the possibility of userspace virtual transports for ØMQ. It's written in C, using Pieter Hintjens' [czmq](http://czmq.zeromq.org) class style. At this stage zvtran doesn't aim to become a general purpose library or layer. It's POSIX only.

<A name="toc3-19" title="Ownership and License" />
### Ownership and License

zvtran is written by Pieter Hintjens. Its other authors and contributors are listed in the AUTHORS file.

The authors of zvtran grant you use of this software under the terms of the GNU Lesser General Public License (LGPL). For details see the files `COPYING` and `COPYING.LESSER` in this directory.

<A name="toc3-26" title="Contributing" />
### Contributing

To submit an issue use the [issue tracker](http://github.com/pieter/zvtran/issues). All discussion happens on the [zeromq-dev](zeromq-dev@lists.zeromq.org) list or #zeromq IRC channel at irc.freenode.net.

The proper way to contribute is to fork this repository, make your changes, and submit a pull request. All contributors are listed in AUTHORS.

<A name="toc2-33" title="Using zvtran" />
## Using zvtran

<A name="toc3-36" title="Dependencies" />
### Dependencies

zvtran depends on the [czmq C language binding](http://czmq.zeromq.org). Please build and install czmq before building and installing zvtran.

<A name="toc3-41" title="Building and Installing" />
### Building and Installing

zvtran does not use autotools. To build, manually compile & link the C main programs. You can use the 'c' script from czmq:

    c -l -lzmq -lczmq server client

<A name="toc2-48" title="Design Notes" />
## Design Notes

This is a project to create a virtual transport layer for ØMQ. I want to make it possible to write protocol drivers as plugins in user space. Right now the only way to add a transport layer is to extend the core codebase. It's difficult enough that we've had zero contributed transports in two years.

Some of the example transports I'd like to be able to explore are:

* UDP
* TLS/SSL or similar
* HTTP(s), i.e. as web proxy
* Persistence over random database products

Making a full virtual transport layer means reimplementing non-trivial parts of ØMQ, including routing algorithms, reconnection, framing, etc. etc. This is kind of okay. I'd like however that to be reusable by drivers, rather than force each driver to reimplement it.

The technique I'm using is inprocess bridging, i.e. the driver is a separate thread that handles a weird transport at one edge, and a ØMQ socket at the other. It bridges messages across the two.

The current implementation (v2) does UDP. It uses two inproc PAIR sockets between driver and application, one for data and one for control commands. This is just to keep things simple, and allow the data socket to look "native" to the caller. The main use case for this driver is the ZeroMQ Name Service, which I'm slowly building. That requires a zero-configuration discovery layer, which now works.

I'm not sure yet how socket patterns fit on top of this, but what I'd like to explore is dynamic patterns. That is, you start with a raw asynchronous bi-directional socket and you then select specific semantics: load-balancing vs. cc distribution; routing or request-reply, maximum peers, filtering, etc. This would let us emulate all existing socket types. Obviously there's going to have to be some clever code reuse here. But a load-balancer is basically the same no matter what the transport.

Performance is currently crap. The UDP driver can do about 100K messages per second. I've made no attempt to batch messages, nor tune the code in any way.

Comments welcome, please discuss on the zeromq-dev list.

<A name="toc3-72" title="This Document" />
### This Document

This document is originally at README.txt and is built using [gitdown](http://github.com/imatix/gitdown).


