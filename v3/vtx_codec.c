/*  =====================================================================
    vtx_codec - message encoding/decoding buffer

    The codec encodes and decodes 0MQ messages for sending/receiving to
    or from the network. It batches small messages together and stores
    references to larger messages to avoid copying.

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

#ifndef __VTX_CODEC_INCLUDED__
#define __VTX_CODEC_INCLUDED__

#include "czmq.h"

#define VTX_INDEX_SIZE      sizeof (uint32_t)
#define VTX_MSG_OBJECT      0xFFFFFFFF

typedef struct _vtx_codec_t vtx_codec_t;

//  We store batches of data, each either a run of collected very small
//  messages (VSM), or a single large message. Batches are held in a
//  ring buffer:
//
//    +------------------------+             +------------------+
//    | Batch entries          |             |                  |
//    |------------------------|             |                  |
//    | VSM batch              +------------------>             |
//    +------------------------+             |                  |
//    | VSM batch              +---------------------->         |
//    +------------------------+             |                  |
//    | Large message          |             |    Data buffer   |
//    +------------------------+             |                  |
//    | VSM batch              +------------------------>       |
//    +------------------------+             |                  |
//                                           |                  |
//                                           +------------------+
//
//  The data buffer is also a ring buffer. In both cases, when head = tail,
//  the buffer is empty, and we add to tail, and remove from head. Batched
//  VSM data is always contiguous in the data buffer.

//  This is the structure of a single batch
typedef struct {
    byte *data;                 //  Pointer into data buffer
    size_t size;                //  Size of this batch in data buffer
    zmq_msg_t *msg;             //  Or, zmq_msg reference
} batch_t;

//  This is the structure of our object
struct _vtx_codec_t {
    batch_t *batch;             //  Ring buffer of batch entries
    byte *buffer;               //  Ring buffer for message data
    size_t batch_limit;         //  Allocated size of batch table
    size_t buffer_limit;        //  Allocated size of buffer
    uint batch_tail;            //  Where we add a new batch entry
    uint batch_head;            //  Where we take off batch entries
    uint buffer_tail;           //  Where we store new data
    uint buffer_head;           //  Where we remove old data
    batch_t *writer;            //  Current batch (for writing)
    batch_t *reader;            //  Current batch (for reading)

    size_t msg_total;           //  Total size of msg data held

    //  When this is null, we'll start on the next batch
    byte *deliver_data;         //  Data still to deliver
    size_t deliver_size;        //  Amount of data still to deliver
};


#ifdef __cplusplus
extern "C" {
#endif

//  Create new codec
static vtx_codec_t *
    vtx_codec_new (size_t limit);

//  Destroy codec and all messages it holds
static void
    vtx_codec_destroy (vtx_codec_t **self_p);

//  Store message in codec
static int
    vtx_codec_store (vtx_codec_t *self, zmq_msg_t *msg, Bool more);

//  Set pointer to serialized data for writing, return size
static size_t
    vtx_codec_deliver (vtx_codec_t *self, byte **data);

//  Update codec with actual amount of data written
static void
    vtx_codec_confirm (vtx_codec_t *self, size_t size);

//  Dump contents of codec buffer for debugging
static void
    vtx_codec_dump (vtx_codec_t *self);

#ifdef __cplusplus
}
#endif


//  Helper functions
static inline int
    s_writer_start (vtx_codec_t *self);
static inline void
    s_write_batch (vtx_codec_t *self, byte *data, size_t size);
static inline size_t
    s_put_zmq_header (zmq_msg_t *msg, Bool more, byte *header);
static inline size_t
    s_get_zmq_header (zmq_msg_t *msg, Bool *more, byte *header);
static int
    s_random (int limit);


//  -------------------------------------------------------------------------
//  Create new codec instance, where limit is number of batch entries

static vtx_codec_t *
vtx_codec_new (size_t limit)
{
    vtx_codec_t *self = (vtx_codec_t *) zmalloc (sizeof (vtx_codec_t));
    assert (limit);

    //  Ring buffer always has one empty slot
    self->batch_limit = limit + 1;
    //  Heuristic to get a decent size for the data buffer
    self->buffer_limit = (limit + 1) * ZMQ_MAX_VSM_SIZE / 2;
    self->batch = malloc (sizeof (batch_t) * self->batch_limit);
    self->buffer = malloc (self->buffer_max);

    //  Start new batch at beginning of buffer
    s_writer_start (self);
    return self;
}

//  Start a new writer
static inline int
s_writer_start (vtx_codec_t *self)
{
    if ((self->batch_tail + 1) % self->batch_limit == self->batch_head)
        return -1;          //  Batch table full
    self->writer = self->batch [self->batch_tail];
    self->writer->size = 0;
    self->writer->data = self->buffer_tail;
    self->writer->msg = NULL;
    self->batch_tail = ++self->batch_tail % self->batch_limit;
}


//  -------------------------------------------------------------------------
//  Destroy codec and all messages it holds

static void
vtx_codec_destroy (vtx_codec_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        vtx_codec_t *self = *self_p;
        while (self->batch_head != self->batch_tail) {
            batch_t *batch = self->batch [self->batch_head];
            if (batch->msg) {
                zmq_msg_close (msg);
                free (msg);
            }
            self->batch_head = ++self->batch_head % self->batch_limit;
        }
        free (self->batch);
        free (self->buffer);
        free (self);
        *self_p = NULL;
    }
}


//  -------------------------------------------------------------------------
//  Store message in codec, returns 0 if OK, -1 if the store is full.

static int
vtx_codec_store (vtx_codec_t *self, zmq_msg_t *msg, Bool more)
{
    //  Encode message header
    byte header [16];
    size_t header_size = s_put_zmq_header (msg, more, header);
    size_t msg_size = zmq_msg_size (msg);


    if (msg_size < ZMQ_MAX_VSM_SIZE) {
        size_t required = header_size + msg_size;
        if (self->buffer_


        s_write_batch (self, header, header_size);
        s_write_batch (self, zmq_msg_data (msg), msg_size);
    }
    else {
        //  We need an open writer to store our message header
        if (self->writer->msg && s_writer_start (self))
            return -1;
        //  We will need one more batch entry for the message itself
        if ((self->batch_tail + 1) % self->batch_limit == self->batch_head)
            return -1;

        //  Now store the message header
        s_writer_store (self, header, header_size);


        self->writer->msg = malloc (sizeof (zmq_msg_t));
        zmq_msg_init (self->writer->msg);
        zmq_msg_copy (self->writer->msg, msg);
    }
    self->msg_total += msg_size;
    return 0;
}


//  Encode 0MQ message frame header, return bytes written
static inline size_t
s_put_zmq_header (zmq_msg_t *msg, Bool more, byte *header)
{
    //  Frame size includes 'more' octet
    size_t frame_size = zmq_msg_size (msg) + 1;
    if (frame_size < 0xFF) {
        header [0] = (byte) frame_size;
        header [1] = more? 1: 0;
        return 2;
    }
    else {
        header [0] = 0xFF;
        header [1] = (byte) ((frame_size >> 56) & 255);
        header [2] = (byte) ((frame_size >> 48) & 255);
        header [3] = (byte) ((frame_size >> 40) & 255);
        header [4] = (byte) ((frame_size >> 32) & 255);
        header [5] = (byte) ((frame_size >> 24) & 255);
        header [6] = (byte) ((frame_size >> 16) & 255);
        header [7] = (byte) ((frame_size >>  8) & 255);
        header [8] = (byte) ((frame_size)       & 255);
        header [9] = more? 1: 0;
        return 10;
    }
}

//  Store batch data, update batch length
static inline void
s_write_batch (vtx_codec_t *self, byte *data, size_t size)
{
    memcpy (self->ins_data_ptr, data, size);
    self->ins_data_ptr += size;
    *((uint32_t *) self->ins_head_ptr) += size;
}


//  -------------------------------------------------------------------------
//  Set pointer to serialized data for writing, return size, or 0 if there
//  is no data available. Call confirm to update buffer with successful
//  write.

static size_t
vtx_codec_deliver (vtx_codec_t *self, byte **data)
{
    //  Look for new batch to deliver, if necessary
    if (self->deliver_size == 0) {
        size_t batch_size = *(uint32_t *) self->del_head_ptr;
        if (batch_size == VTX_MSG_OBJECT) {
            zmq_msg_t *msg = *((zmq_msg_t **) self->del_data_ptr);
            self->deliver_data = zmq_msg_data (msg);
            self->deliver_size = zmq_msg_size (msg);
        }
        else
        if (batch_size > 0) {
            //  Close batch, if it's open, and start new one
            if (self->del_head_ptr == self->ins_head_ptr)
                s_start_batch (self, self->ins_data_ptr, 0, 0);
            self->deliver_data = self->del_data_ptr;
            self->deliver_size = batch_size;
        }
    }
    *data = self->deliver_data;
    return self->deliver_size;
}


//  -------------------------------------------------------------------------
//  Update codec with actual amount of data written

static void
vtx_codec_confirm (vtx_codec_t *self, size_t size)
{
    assert (size <= self->deliver_size);
    self->deliver_size -= size;
    self->deliver_data += size;
}



//  Decode 0MQ message frame header, return bytes scanned
static inline size_t
s_get_zmq_header (zmq_msg_t *msg, Bool *more, byte *header)
{
    size_t frame_size = header [0];
    if (frame_size < 0xFF) {
        assert (frame_size > 0);
        zmq_msg_init_size (msg, frame_size - 1);
        *more = (header [1] == 1);
        return 2;
    }
    else {
        frame_size = ((size_t) (header [1]) << 56)
                   + ((size_t) (header [2]) << 48)
                   + ((size_t) (header [3]) << 40)
                   + ((size_t) (header [4]) << 32)
                   + ((size_t) (header [5]) << 24)
                   + ((size_t) (header [6]) << 16)
                   + ((size_t) (header [7]) << 8)
                   + ((size_t) (header [8]));
        Bool more = (header [9] == 1);
        zmq_msg_init_size (msg, frame_size - 1);
        return 10;
    }
}


//  -------------------------------------------------------------------------
//  Selftest this class

static void
vtx_codec_selftest (void)
{
    size_t msg_nbr = 0;
    int test_run;
    for (test_run = 0; test_run < 1; test_run++) {
        //  Store a series of random-sized messages
        vtx_codec_t *codec = vtx_codec_new (1000);
        while (TRUE) {
            //  80% smaller, 20% larger messages
            size_t size = s_random (s_random (10) < 8? ZMQ_MAX_VSM_SIZE: 5000);
            //  If you want to test the raw speed of encoding, don't use
            //  the random number generator
            //  size_t size = 4;
            zmq_msg_t msg;
            zmq_msg_init_size (&msg, size);
            int rc = vtx_codec_store (codec, &msg, FALSE);
            msg_nbr++;
            zmq_msg_close (&msg);
            if (rc)
                break;
        }

        //  Now extract all messages from store
        byte  *data;
        size_t size;
        while ((size = vtx_codec_deliver (codec, &data))) {
            printf ("DELIVER: batch size: %zd\n", size);
            vtx_codec_confirm (codec, size);
            break;
        }
        vtx_codec_destroy (&codec);
    }
    printf ("%zd messages stored\n", msg_nbr);
}

//  Fast pseudo-random number generator

static int
s_random (int limit)
{
    static uint32_t value = 0;
    if (value == 0)
        value = (uint32_t) (time (NULL));
    value = (value ^ 61) ^ (value >> 16);
    value = value + (value << 3);
    value = value ^ (value >> 4);
    value = value * 0x27d4eb2d;
    value = value ^ (value >> 15);
    return value % limit;
}

#endif
