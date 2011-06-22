/*  =====================================================================
    vtx_queue - 0MQ virtual transport interface - message ring queue

    This implements a simple FIFO ring-queue that holds message frames.
    The current strategy on full queue is to drop the oldest message.

    ---------------------------------------------------------------------
    Copyright (c) 1991-2011 iMatix Corporation <www.imatix.com>
    Copyright other contributors as noted in the AUTHORS file.

    This file is part of VTX, the 0MQ virtual transport interface:
    http://vtx.zeromq.org.

    This is free software; you can redistribute it and/or modify it under
    the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation; either version 3 of the License, or (at
    your option) any later version.

    This software is distributed in the hope that it will be useful, but
    WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
    Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public
    License along with this program. If not, see
    <http://www.gnu.org/licenses/>.
    =====================================================================
*/

#ifndef __VTX_QUEUE_INCLUDED__
#define __VTX_QUEUE_INCLUDED__

#include "czmq.h"

typedef struct _queue_t queue_t;

struct _queue_t {
    zframe_t **queue;           //  Message queue ring buffer
    uint limit;                 //  Limit of queue in elements
    uint head;                  //  Oldest message is here
    uint tail;                  //  New messages go here
};

#ifdef __cplusplus
extern "C" {
#endif

//  Create new queue
static queue_t *
    queue_new (size_t limit);

//  Destroy queue and all messages it holds
static void
    queue_destroy (queue_t **self_p);

//  Store message in queue
static void
    queue_store (queue_t *self, zframe_t *frame, Bool grab);

//  Return pointer to oldest message in queue
static zframe_t *
    queue_oldest (queue_t *self);

//  Return pointer to newest message in queue
static zframe_t *
    queue_newest (queue_t *self);

//  Drop oldest message in queue
static void
    queue_drop_oldest (queue_t *self);

//  Drop newest message in queue
static void
    queue_drop_newest (queue_t *self);

//  Return number of messages in queue
static size_t
    queue_size (queue_t *self);

#ifdef __cplusplus
}
#endif


//  Create new queue
static queue_t *
queue_new (size_t limit)
{
    queue_t *self = (queue_t *) zmalloc (sizeof (queue_t));
    self->queue = malloc (limit * sizeof (zframe_t *));
    self->limit = limit;
    self->head = 0;
    self->tail = 0;
    return self;
}

//  Destroy queue and all messages it holds
static void
queue_destroy (queue_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        queue_t *self = *self_p;
        while (queue_size (self))
            queue_drop_oldest (self);
        free (self->queue);
        free (self);
        *self_p = NULL;
    }
}

//  Store message in queue
static void
queue_store (queue_t *self, zframe_t *frame, Bool grab)
{
    self->queue [self->tail] = grab? frame: zframe_dup (frame);
    self->tail = ++self->tail % self->limit;
    if (self->tail == self->head) {
        //  Queue is full, so bump head
        //  Strategy for now is to drop message
        zframe_t *frame = self->queue [self->head];
        self->head = ++self->head % self->limit;
        zframe_destroy (&frame);
    }
}

//  Return pointer to oldest message in queue
static zframe_t *
queue_oldest (queue_t *self)
{
    zframe_t *frame = NULL;
    if (self->head != self->tail)
        frame = self->queue [self->head];
    return frame;
}

//  Return pointer to newest message in queue
static zframe_t *
queue_newest (queue_t *self)
{
    zframe_t *frame = NULL;
    if (self->head != self->tail)
        frame = self->queue [(self->tail + self->limit - 1) % self->limit];
    return frame;
}

//  Drop oldest message in queue
static void
queue_drop_oldest (queue_t *self)
{
    if (self->head != self->tail) {
        zframe_t *frame = self->queue [self->head];
        self->head = ++self->head % self->limit;
        zframe_destroy (&frame);
    }
}

//  Drop newest message in queue
static void
queue_drop_newest (queue_t *self)
{
    if (self->head != self->tail) {
        self->tail = (self->tail + self->limit - 1) % self->limit;
        zframe_t *frame = self->queue [self->tail];
        zframe_destroy (&frame);
    }
}

//  Return number of messages in queue
static size_t
queue_size (queue_t *self)
{
    return (self->tail - self->head + self->limit) % self->limit;
}

static void
queue_selftest (void)
{
    queue_t *queue = queue_new (3);
    assert (queue_size (queue) == 0);

    zframe_t *frame = zframe_new ("ABC", 3);
    queue_store (queue, frame, FALSE);
    assert (queue_size (queue) == 1);
    queue_store (queue, frame, FALSE);
    assert (queue_size (queue) == 2);
    queue_store (queue, frame, FALSE);
    assert (queue_size (queue) == 2);
    queue_store (queue, frame, TRUE);
    assert (queue_size (queue) == 2);

    frame = queue_oldest (queue);
    assert (frame);
    frame = queue_newest (queue);
    assert (frame);
    queue_drop_oldest (queue);
    assert (queue_size (queue) == 1);
    queue_drop_newest (queue);
    assert (queue_size (queue) == 0);
    queue_drop_newest (queue);
    assert (queue_size (queue) == 0);

    frame = queue_newest (queue);
    assert (frame == NULL);
    queue_destroy (&queue);
}

#endif
