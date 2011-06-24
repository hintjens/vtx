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
//    | Large message          |             |   Data buffer    |
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
    uint size;                  //  Size of this batch in data buffer
    zmq_msg_t *msg;             //  Or, zmq_msg reference
    Bool busy;                  //  Batch is being extracted
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
    size_t active;              //  Total serialized data size
    Bool debug;                 //  Debug switch

    //  When this is null, we'll start on the next batch
    byte *extract_data;         //  Data still to extract
    size_t extract_size;        //  Amount of data still to extract
};

//  The batch table is full when we can't bump the batch_tail
#define BATCH_TABLE_FULL \
    ((self->batch_tail + 1) % self->batch_limit == self->batch_head)

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
     vtx_codec_playback (vtx_codec_t *self, byte **data);

//  Update codec with actual amount of data written
static void
    vtx_codec_confirm (vtx_codec_t *self, size_t size);

//  Return capacity for new input data, 0 means full
static size_t
    vtx_codec_capacity (vtx_codec_t *self);

//  Store read data into codec, if possible
//  where do we store message flags
static int
    vtx_codec_record (vtx_codec_t *self, byte *data, size_t size);


//  Return active size of codec (message data plus headers)
static size_t
    vtx_codec_active (vtx_codec_t *self);

//  Consistency check of codec, asserts if there's a fault
static void
    vtx_codec_check (vtx_codec_t *self, char *text);

//  Selftest of codec class
static void
    vtx_codec_selftest (void);

#ifdef __cplusplus
}
#endif

//  Helper functions
static inline int
    s_batch_start (vtx_codec_t *self);
static inline int
    s_batch_ready (vtx_codec_t *self, size_t required);
static inline void
    s_batch_store (vtx_codec_t *self, byte *data, size_t size);
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
    self->buffer_limit = limit * ZMQ_MAX_VSM_SIZE * 10 / 8;
    self->batch = malloc (sizeof (batch_t) * self->batch_limit);
    self->buffer = malloc (self->buffer_limit);
    //  Start new batch at beginning of buffer
    self->batch_tail = 0;
    self->batch_head = self->batch_tail;
    self->buffer_tail = 0;
    //  TEST: start at a random position in buffer to test end conditions
    srandom (time (NULL));
    self->buffer_tail = s_random (self->buffer_limit);
    self->buffer_head = self->buffer_tail;
    s_batch_start (self);

    return self;
}

//  Start a new writer
static inline int
s_batch_start (vtx_codec_t *self)
{
    if (BATCH_TABLE_FULL)
        return -1;          //  Batch table full
    self->writer = &self->batch [self->batch_tail];
    self->writer->size = 0;
    self->writer->data = self->buffer + self->buffer_tail;
    self->writer->msg = NULL;
    self->writer->busy = FALSE;
    self->batch_tail = ++self->batch_tail % self->batch_limit;
    return 0;
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
            batch_t *batch = &self->batch [self->batch_head];
            if (batch->msg) {
                zmq_msg_close (batch->msg);
                free (batch->msg);
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
    uint header_size = s_put_zmq_header (msg, more, header);
    uint msg_size = zmq_msg_size (msg);

    //  Open a writer for the message header, if needed
    if ((self->writer->msg || self->writer->busy)
    &&  s_batch_start (self))
        return -1;

    if (msg_size < ZMQ_MAX_VSM_SIZE) {
        //  Check sufficient space and prepare to write
        if (s_batch_ready (self, header_size + msg_size))
            return -1;
        //  Store message header
        s_batch_store (self, header, header_size);
        //  Store message data
        s_batch_store (self, zmq_msg_data (msg), msg_size);
    }
    else {
        //  We will need a batch entry for the message reference
        if (BATCH_TABLE_FULL)
            return -1;
        //  Check sufficient space and prepare to write
        if (s_batch_ready (self, header_size))
            return -1;

        //  Store message header
        s_batch_store (self, header, header_size);
        //  Store message reference
        s_batch_start (self);
        self->writer->msg = malloc (sizeof (zmq_msg_t));
        zmq_msg_init (self->writer->msg);
        zmq_msg_copy (self->writer->msg, msg);
    }
    self->active += header_size + msg_size;
    return 0;
}


//  Encode 0MQ message frame header, return bytes written
static inline size_t
s_put_zmq_header (zmq_msg_t *msg, Bool more, byte *header)
{
    //  Frame size includes 'more' octet
    int64_t frame_size = zmq_msg_size (msg) + 1;
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

//  Check sufficient space and prepare to write
//      H=T     HT...............................   empty
//      H=T     ..........HT.....................   empty
//      H>T     ******T.............H************   one run
//      H<T     H********************T...........   one run
//      H<T     .........H**************T........   two runs
//      H<T     H*******************************T   full

static inline int
s_batch_ready (vtx_codec_t *self, size_t required)
{
    if (self->buffer_head <= self->buffer_tail) {
        //  Look for free space at current buffer tail
        size_t free_space = self->buffer_limit - self->buffer_tail;
        if (free_space < required + 1) {
            //  If not sufficient, look at start of buffer
            free_space = self->buffer_head;
            if (free_space < required + 1)
                return -1;
            else {
                //  Wrap around means starting new writer
                if (self->writer->size && s_batch_start (self))
                    return -1;
                //  All ready, prepare to write at start of buffer
                self->buffer_tail = 0;
                self->writer->data = self->buffer;
            }
        }
        else
        if (free_space == required + 1
        &&  BATCH_TABLE_FULL)
            //  We'll have to start a new batch after this
            return -1;
    }
    else
    if (self->buffer_head > self->buffer_tail) {
        size_t free_space = self->buffer_head - self->buffer_tail - 1;
        if (free_space < required + 1)
            return -1;
    }
    return 0;
}


//  Store batch data, update batch length. Does not wrap around, caller
//  must have called s_batch_ready before, to set things up.
static inline void
s_batch_store (vtx_codec_t *self, byte *data, size_t size)
{
    assert (!self->writer->msg);
    self->writer->size += size;
    memcpy (self->buffer + self->buffer_tail, data, size);
    self->buffer_tail += size;
    if (self->buffer_tail == self->buffer_limit)
        s_batch_start (self);
}


//  -------------------------------------------------------------------------
//  Set pointer to serialized data for writing, return size, or 0 if there
//  is no data available. Call confirm to update buffer with successful
//  write.

static size_t
 vtx_codec_playback (vtx_codec_t *self, byte **data)
{
    //  Look for new batch to extract, if necessary
    if (self->extract_size == 0
    &&  self->batch_head != self->batch_tail) {
        self->reader = &self->batch [self->batch_head];
        if (self->reader->msg) {
            self->reader->busy = TRUE;
            self->extract_data = zmq_msg_data (self->reader->msg);
            self->extract_size = zmq_msg_size (self->reader->msg);
        }
        else
        if (self->reader->size) {
            self->reader->busy = TRUE;
            self->extract_data = self->reader->data;
            self->extract_size = self->reader->size;
        }
    }
    *data = self->extract_data;
    return self->extract_size;
}


//  -------------------------------------------------------------------------
//  Update codec with actual amount of data written

static void
vtx_codec_confirm (vtx_codec_t *self, size_t size)
{
    assert (size <= self->extract_size);
    if (size) {
        self->extract_size -= size;
        self->extract_data += size;
        if (self->extract_size == 0) {
            self->batch_head = ++self->batch_head % self->batch_limit;
            if (!self->reader->msg)
                self->buffer_head = (self->buffer_head + self->reader->size)
                                   % self->buffer_limit;
        }
        self->active -= size;
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
        frame_size = ((int64_t) (header [1]) << 56)
                   + ((int64_t) (header [2]) << 48)
                   + ((int64_t) (header [3]) << 40)
                   + ((int64_t) (header [4]) << 32)
                   + ((int64_t) (header [5]) << 24)
                   + ((int64_t) (header [6]) << 16)
                   + ((int64_t) (header [7]) << 8)
                   + ((int64_t) (header [8]));
        Bool more = (header [9] == 1);
        zmq_msg_init_size (msg, frame_size - 1);
        return 10;
    }
}


//  -------------------------------------------------------------------------
//  Return active size of codec (message data plus headers)
static size_t
vtx_codec_active (vtx_codec_t *self)
{
    return self->active;
}


//  -------------------------------------------------------------------------
//  Consistency check of codec, asserts if there's a fault

static void
vtx_codec_check (vtx_codec_t *self, char *text)
{
    uint head = self->batch_head;
    while (head != self->batch_tail) {
        batch_t *batch = &self->batch [head];
        if (batch->size > 0 && batch->msg == NULL && batch->data [0] == 0) {
            printf ("(%s) %p - ", text, batch->data);
            int i;
            for (i = 0; i < batch->size; i++)
                printf ("%02x ", batch->data [i]);
            puts ("");
            printf ("Invalid zero data, at=%d size=%d this=%d head=%d tail=%d\n",
                    batch->data - self->buffer,
                    batch->size, head, self->batch_head, self->batch_tail);
            assert (0);
        }
        head = (head + 1) % self->batch_limit;
    }
}


//  -------------------------------------------------------------------------
//  Selftest of codec class

static void
vtx_codec_selftest (void)
{
    int msg_nbr = 0;
    vtx_codec_t *codec = vtx_codec_new (10000);
    //  Run randomized inserts/extracts for 1 second

    //  This is NOT fast, if you want to run a performance test then
    //  remove the codec_store calls, and the extraction to a memory
    //  buffer and subsequent decoding.

    int64_t start = zclock_time ();
    byte write_seq = 0;
    byte read_seq = 0;

    while (TRUE) {
        //  Insert a bunch of messages
        int insert = s_random (1000);
        while (insert) {
            //  80% smaller, 20% larger messages
            size_t size = s_random (s_random (10) < 8? ZMQ_MAX_VSM_SIZE: 5000);
            zmq_msg_t msg;
            zmq_msg_init_size (&msg, size);
            if (size > 0) {
                byte *data = zmq_msg_data (&msg);
                data [0] = write_seq;
            }
            int rc = vtx_codec_store (codec, &msg, FALSE);
            vtx_codec_check (codec, "store");
            msg_nbr++;
            zmq_msg_close (&msg);
            if (rc)
                break;          //  If store full, stop inserting
            write_seq++;
        }
        size_t active = vtx_codec_active (codec);
        byte *buffer = malloc (active);
        uint bufidx = 0;
        //  Extract everything we can
        byte  *data;
        size_t size;
        while ((size =  vtx_codec_playback (codec, &data))) {
            //  We should shove this into a receiver codec now
            memcpy (buffer + bufidx, data, size);
            bufidx += size;
            vtx_codec_confirm (codec, size);
            vtx_codec_check (codec, "extract");
        }
        assert (vtx_codec_active (codec) == 0);

        bufidx = 0;
        while (bufidx < active) {
            zmq_msg_t msg;
            Bool more;
            bufidx += s_get_zmq_header (&msg, &more, buffer + bufidx);
            size_t size = zmq_msg_size (&msg);
            if (size > 0) {
                assert (buffer [bufidx] == read_seq);
                bufidx += size;
            }
            read_seq++;
        }
        free (buffer);

        //  Stop after a second of work
        if (zclock_time () - start > 999)
            break;
    }
    vtx_codec_destroy (&codec);
    printf ("%d messages stored & extracted\n", msg_nbr);
}

//  Fast pseudo-random number generator

static int
s_random (int limit)
{
    static uint32_t value = 0;
    if (value == 0) {
        value = (uint32_t) (time (NULL));
        printf ("Seeding at %d\n", value);
    }
    value = (value ^ 61) ^ (value >> 16);
    value = value + (value << 3);
    value = value ^ (value >> 4);
    value = value * 0x27d4eb2d;
    value = value ^ (value >> 15);
    return value % limit;
}

#endif
