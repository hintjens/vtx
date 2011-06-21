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

//  Messages smaller than this are batched together
#define VTX_CODEC_CUTOFF    64
#define VTX_INDEX_SIZE      sizeof (qbyte)

typedef struct _vtx_codec_t vtx_codec_t;

struct _vtx_codec_t {
    byte *buffer;               //  Buffer data
    size_t limit;               //  Buffer size allocated
    size_t available;           //  Buffer size available
    size_t msg_total;           //  Total size of msg data held

    //  Batches are stored as [4*length][data], where length > 0.
    //  Batches are never wrapped around the end of the buffer.
    //  End-of-buffer filler is binary zeroes.
    //  Message references are stored as [0xFFFFFFFF][pointer]

    //  Buffer is empty when read_ptr == tail
    byte *tail;                 //  Oldest byte in buffer
    byte *write_ptr;            //  Where we write next data
    byte *length_at;            //  Where we update batch length
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

//  Check consistency of codec buffer
static void
    vtx_codec_check (vtx_codec_t *self);

//  Dump contents of codec buffer for debugging
static void
    vtx_codec_dump (vtx_codec_t *self);

#ifdef __cplusplus
}
#endif


//  Helper functions
static inline void
    s_start_batch (vtx_codec_t *self, qbyte initial);
static inline void
    s_write_batch (vtx_codec_t *self, byte *data, size_t size);
static inline size_t
    s_put_zmq_header (zmq_msg_t *msg, Bool more, byte *header);
static inline size_t
    s_get_zmq_header (zmq_msg_t *msg, Bool *more, byte *header);


//  -------------------------------------------------------------------------
//  Create new codec instance

static vtx_codec_t *
vtx_codec_new (size_t limit)
{
    vtx_codec_t *self = (vtx_codec_t *) zmalloc (sizeof (vtx_codec_t));
    self->buffer = malloc (limit);
    self->limit = limit;
    self->available = limit;
    self->write_ptr = self->buffer;
    self->tail = self->buffer;

    //  Start writing at beginning of buffer
    s_start_batch (self, 0);
    return self;
}


//  -------------------------------------------------------------------------
//  Destroy codec and all messages it holds

static void
vtx_codec_destroy (vtx_codec_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        vtx_codec_t *self = *self_p;

        //  Close zmq messages we still hold
        byte *length_at = self->buffer;
        byte *read_ptr = length_at + VTX_INDEX_SIZE;

        while (read_ptr < self->write_ptr) {
            int this_size = *(qbyte *) length_at;
            if (this_size == 0xFFFFFFFF) {
                zmq_msg_t *msg = *((zmq_msg_t **) read_ptr);
                zmq_msg_close (msg);
                free (msg);
                read_ptr += sizeof (zmq_msg_t *);
            }
            else
                read_ptr += this_size;

            length_at = read_ptr;
            read_ptr = length_at + VTX_INDEX_SIZE;
        }
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
    //  Store message header at current batch
    byte header [16];
    int header_size = s_put_zmq_header (msg, more, header);

    //  Check that buffer has room for this message
    size_t msg_size = zmq_msg_size (msg);
    if (msg_size < VTX_CODEC_CUTOFF) {
        if (header_size + msg_size > self->available)
            return -1;
        s_write_batch (self, header, header_size);
        s_write_batch (self, zmq_msg_data (msg), msg_size);
    }
    else {
        if (header_size + 2 * VTX_INDEX_SIZE + sizeof (zmq_msg_t *) > self->available)
            return -1;
        zmq_msg_t *copy = zmalloc (sizeof (zmq_msg_t));
        zmq_msg_init (copy);
        zmq_msg_copy (copy, msg);
        assert (zmq_msg_size (copy) == zmq_msg_size (msg));

        s_write_batch (self, header, header_size);
        s_start_batch (self, 0xFFFFFFFF);

        *((zmq_msg_t **) self->write_ptr) = copy;
        self->write_ptr += sizeof (zmq_msg_t *);
        self->available -= sizeof (zmq_msg_t *);
        s_start_batch (self, 0);
    }
    self->msg_total += msg_size;
    return 0;
}


//  -------------------------------------------------------------------------
//  Check consistency of codec buffer

static void
vtx_codec_check (vtx_codec_t *self)
{
    byte *length_at = self->buffer;
    byte *read_ptr = length_at + VTX_INDEX_SIZE;
    size_t msg_total = 0;

    while (read_ptr < self->write_ptr) {
        int remaining = *(qbyte *) length_at;
        while (remaining > 0) {
            zmq_msg_t msg;
            Bool more;
            size_t header_size = s_get_zmq_header (&msg, &more, read_ptr);
            size_t msg_size = zmq_msg_size (&msg);
            msg_total += msg_size;

            //  If amount left in batch > header size, message is inline
            if (remaining > header_size || msg_size == 0) {
                //  Skip over small message body
                read_ptr += header_size + msg_size;
                remaining -= header_size + msg_size;
                assert (remaining >= 0);
            }
            else {
                //  Skip over large message reference
                read_ptr += header_size;
                remaining -= header_size;
                assert (remaining == 0);

                length_at = read_ptr;
                read_ptr = length_at + VTX_INDEX_SIZE;
                assert (*((qbyte *) length_at) == 0xFFFFFFFF);

                zmq_msg_t *copy = *((zmq_msg_t **) read_ptr);
                assert (zmq_msg_size (copy) == msg_size);
                read_ptr += sizeof (zmq_msg_t *);
            }
            zmq_msg_close (&msg);
        }
        //  Next size field is at batch read_ptr
        length_at = read_ptr;
        read_ptr = length_at + VTX_INDEX_SIZE;
    }
    assert (msg_total == self->msg_total);
}


//  Start new batch at self->write_ptr, set initial batch length
static inline void
s_start_batch (vtx_codec_t *self, qbyte initial)
{
    assert (self->available >= VTX_INDEX_SIZE);

    self->length_at = self->write_ptr;
    self->write_ptr += VTX_INDEX_SIZE;
    self->available -= VTX_INDEX_SIZE;
    *((qbyte *) self->length_at) = initial;
}

//  Store batch data, update batch length
static inline void
s_write_batch (vtx_codec_t *self, byte *data, size_t size)
{
    assert (self->available >= size);

    memcpy (self->write_ptr, data, size);
    self->write_ptr += size;
    self->available -= size;
    *((qbyte *) self->length_at) += size;
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
//    srandom ((unsigned) time (NULL));

    int i;
    for (i = 10; i<1000000; i++) {
        srandom (i); printf ("SEED=%d\n", i);

//    srandom (110);

    //  Store a series of random-sized messages
    vtx_codec_t *codec = vtx_codec_new (100000);
    int msg_nbr = 0;
    while (TRUE) {
        //  80% smaller, 20% larger messages
        size_t size = randof (randof (10) < 5? 50: 5000);
        Bool more = randof (10) < 7;
        zmq_msg_t msg;
        zmq_msg_init_size (&msg, size);
        int rc = vtx_codec_store (codec, &msg, more);
        msg_nbr++;
        zmq_msg_close (&msg);
        if (rc)
            break;
    }
    printf ("%d messages stored\n", msg_nbr);
    vtx_codec_check (codec);
    vtx_codec_destroy (&codec);
    }
}

#endif
