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
    uint batch_limit;           //  Allocated size of batch table
    uint buffer_limit;          //  Allocated size of buffer
    uint batch_tail;            //  Where we add a new batch entry
    uint batch_head;            //  Where we take off batch entries
    uint buffer_tail;           //  Where we store new data
    uint buffer_head;           //  Where we remove old data
    batch_t *writer;            //  Current batch (for writing)
    batch_t *reader;            //  Current batch (for reading)
    size_t free_space;          //  Size of next available run
    size_t active;              //  Total serialized data size
    Bool debug;                 //  Debug mode on codec?

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

//  Store 0MQ message into codec, returns 0 if OK, -1 if the store is full
static int
    vtx_codec_msg_put (vtx_codec_t *self, zmq_msg_t *msg, Bool more);

//  Fetch 0MQ message from codec. When you have finished processing the
//  message, call zmq_msg_close() on it. Returns 0 if OK, -1 if there are
//  no more messages in codec.
static int
    vtx_codec_msg_get (vtx_codec_t *self, zmq_msg_t *msg, Bool *more_p);

//  Store serialized data into codec
static int
    vtx_codec_bin_put (vtx_codec_t *self, byte *data, size_t size);

//  Fetch serialized data from codec. You can process the serialized data
//  in chunks, each time calling bin_tick() with the actual amount processed,
//  and then bin_get() again to get a new pointer.
static size_t
    vtx_codec_bin_get (vtx_codec_t *self, byte **data_p);

//  Update codec with actual amount of data extracted
static void
    vtx_codec_bin_tick (vtx_codec_t *self, size_t size);

//  Return capacity for new input data, 0 means full
static size_t
    vtx_codec_bin_space (vtx_codec_t *self);

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
    s_get_zmq_header (vtx_codec_t *self, zmq_msg_t *msg, Bool *more, byte *header);
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
    if (self->debug)
        printf ("start batch at=%d\n", self->batch_tail);
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
//  Store 0MQ message into codec, returns 0 if OK, -1 if the store is full

static int
vtx_codec_msg_put (vtx_codec_t *self, zmq_msg_t *msg, Bool more)
{
    assert (self);
    assert (msg);

    //  Encode message header
    byte header [16];
    uint header_size = s_put_zmq_header (msg, more, header);
    uint msg_size = zmq_msg_size (msg);
    if (self->debug)
        printf ("msg_put size=%d\n", msg_size);

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
        //  Check sufficient space and prepare to write
        if (s_batch_ready (self, header_size))
            return -1;
        //  We will need a batch entry for the message reference
        if (BATCH_TABLE_FULL)
            return -1;

        //  Store message header
        s_batch_store (self, header, header_size);
        //  Store message reference
        s_batch_start (self);
        self->writer->msg = malloc (sizeof (zmq_msg_t));
        zmq_msg_init (self->writer->msg);
        zmq_msg_copy (self->writer->msg, msg);
        if (self->debug)
            printf ("store message=%p\n", self->writer->msg);
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
    //  Open a writer if necessary
    if ((self->writer->msg || self->writer->busy)
    &&  s_batch_start (self))
        return -1;

    if (self->buffer_head <= self->buffer_tail) {
        //  Look for free space at current buffer tail
        self->free_space = self->buffer_limit - self->buffer_tail;
        if (self->free_space < required + 1) {
            //  If not sufficient, look at start of buffer
            self->free_space = self->buffer_head;
            if (self->free_space < required + 1)
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
        if (self->free_space == required + 1
        &&  BATCH_TABLE_FULL)
            //  We'll have to start a new batch after this
            return -1;
    }
    else
    if (self->buffer_head > self->buffer_tail) {
        self->free_space = self->buffer_head - self->buffer_tail - 1;
        if (self->free_space < required + 1)
            return -1;
    }
    return 0;
}


//  Store batch data, update batch length. Does not wrap around, caller
//  must have called s_batch_ready before, to set things up.
static inline void
s_batch_store (vtx_codec_t *self, byte *data, size_t size)
{
    if (self->debug)
        printf ("store size=%zd at=%d/%d\n",
            size, self->buffer_tail, self->buffer_limit);
    assert (!self->writer->msg);
    self->writer->size += size;
    memcpy (self->buffer + self->buffer_tail, data, size);
    self->buffer_tail += size;
    if (self->buffer_tail == self->buffer_limit)
        s_batch_start (self);
}


//  -------------------------------------------------------------------------
//  Fetch 0MQ message from codec. When you have finished processing the
//  message, call zmq_msg_close() on it. Returns 0 if OK, -1 if there are
//  no more messages in codec.

//  TODO: handle invalid message data (bad header) by signalling an error.

static int
vtx_codec_msg_get (vtx_codec_t *self, zmq_msg_t *msg, Bool *more_p)
{
    assert (self);
    assert (msg);
    assert (more_p);

    //  Author's note:
    //  This code is absolutely horrid, because there are so many different
    //  edge cases. I'll think about a cleaner data structure that makes it
    //  possible to extract messages in a single step.

    //  Look for new batch to extract, if necessary
    if (self->extract_size == 0) {
        if (self->batch_head == self->batch_tail) {
            if (self->debug)
                printf ("** extract empty, head=%d tail=%d\n",
                        self->batch_head, self->batch_tail);
            return -1;          //  Buffer is empty
        }
        self->reader = &self->batch [self->batch_head];
        if (self->debug)
            printf ("extract batch at=%d tail=%d reader=%d bufhead=%d\n",
                    self->batch_head, self->batch_tail,
                    (int) (self->reader->data - self->buffer),
                    self->buffer_head);
        self->reader->busy = TRUE;
        self->extract_data = self->reader->data;
        self->extract_size = self->reader->size;
        assert (self->extract_size);
    }
    //  Extract 0MQ message header from data
    size_t header_size = s_get_zmq_header (self, msg, more_p, self->extract_data);
    if (header_size == 0)
        return -1;
    size_t msg_size = zmq_msg_size (msg);
    if (self->debug)
        printf (" -- extract remaining=%zd msgsize=%zd\n", self->extract_size, msg_size);
    self->extract_data += header_size;
    self->extract_size -= header_size;
    self->active -= header_size + msg_size;
    self->buffer_head = self->extract_data - self->buffer;
    if (self->debug)
        printf (" -- bump buffer-head=%d (msg header)\n", self->buffer_head);

    if (self->extract_size || msg_size == 0) {
        //  Message data is in buffer
        if (msg_size) {
            memcpy (zmq_msg_data (msg), self->extract_data, msg_size);
            self->extract_data += msg_size;
            self->extract_size -= msg_size;
            self->buffer_head = self->extract_data - self->buffer;
            if (self->debug)
                printf (" -- bump buffer-head=%d (msg body)\n", self->buffer_head);
        }
        if (self->extract_size == 0) {
            self->batch_head = ++self->batch_head % self->batch_limit;
            if (self->debug)
                printf (" -- bump batch head=%d (1)\n", self->batch_head);
        }
    }
    else {
        if (self->debug)
            printf (" -- message split, at=%d msgsize=%zd\n",
                (int) (self->extract_data - self->buffer), msg_size);

        //  Batch is done, drop it
        self->batch_head = ++self->batch_head % self->batch_limit;
        if (self->debug)
            printf (" -- bump batch head=%d (2)\n", self->batch_head);
        if (msg_size) {
            //  Message may be stored by reference in next batch
            self->reader = &self->batch [self->batch_head];
            if (self->debug)
                printf ("extract batch at=%d tail=%d reader=%d bufhead=%d (split)\n",
                        self->batch_head, self->batch_tail,
                        (int) (self->reader->data - self->buffer),
                        self->buffer_head);
            if (self->reader->size) {
                //  Or may be in buffer directly
                self->reader->busy = TRUE;
                self->extract_data = self->reader->data;
                self->extract_size = self->reader->size;
                if (self->debug)
                    printf (" -- extract remaining=%zd msgsize=%zd (split)\n",
                            self->extract_size, msg_size);

                memcpy (zmq_msg_data (msg), self->extract_data, msg_size);
                self->extract_data += msg_size;
                self->extract_size -= msg_size;
                self->buffer_head = self->extract_data - self->buffer;
                if (self->debug)
                    printf (" -- bump buffer-head=%d (msg body, spilt)\n",
                            self->buffer_head);
                if (self->extract_size == 0) {
                    self->batch_head = ++self->batch_head % self->batch_limit;
                    if (self->debug)
                        printf (" -- bump batch head=%d (3)\n", self->batch_head);
                }
            }
            else {
                assert (self->reader->msg);
                zmq_msg_copy (msg, self->reader->msg);
                zmq_msg_close (self->reader->msg);
                free (self->reader->msg);
            }
        }
    }
    return 0;
}

static void
s_dump (vtx_codec_t *self)
{
    printf ("DUMP\n");
    printf ("  batch  head=%d tail=%d\n", self->batch_head, self->batch_tail);
    printf ("  buffer head=%d tail=%d size=%d\n",
            self->buffer_head, self->buffer_tail, self->buffer_limit);
    printf ("  extract data=%d size=%zd\n",
            (int) (self->extract_data - self->buffer), self->extract_size);
    puts ("");
}

//  Decode 0MQ message frame header, return bytes scanned.
//  If there is sufficient active data, initializes message to
//  appropriate size. If there is insufficient active data, returns
//  zero,
static inline size_t
s_get_zmq_header (vtx_codec_t *self, zmq_msg_t *msg, Bool *more, byte *header)
{
    size_t frame_size = header [0];
    if (frame_size < 0xFF) {
        if (frame_size == 0) {
            s_dump (self);
            int i;
            for (i = 0; i < 10; i++)
                printf ("%02x ", header [i]);
            puts ("");
        }
        assert (frame_size > 0);
        *more = (header [1] == 1);
        if (self->active < frame_size)
            return 0;
        zmq_msg_init_size (msg, frame_size - 1);
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
        assert (frame_size > 0);
        *more = (header [9] == 1);
        if (self->active < frame_size)
            return 0;
        zmq_msg_init_size (msg, frame_size - 1);
        return 10;
    }
}


//  -------------------------------------------------------------------------
//  Store serialized data into codec

static int
vtx_codec_bin_put (vtx_codec_t *self, byte *data, size_t size)
{
    if (s_batch_ready (self, size) == 0) {
        if (self->debug)
            printf ("bin put size=%zd at=%d/%d\n",
                size, self->buffer_tail, self->buffer_limit);
        s_batch_store (self, data, size);
        self->active += size;
        return 0;
    }
    else
        return -1;
}


//  -------------------------------------------------------------------------
//  Fetch serialized data from codec. You can process the serialized data
//  in chunks, each time calling bin_tick() with the actual amount processed,
//  and then bin_get() again to get a new pointer.

static size_t
vtx_codec_bin_get (vtx_codec_t *self, byte **data_p)
{
    assert (self);
    assert (data_p);

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
    *data_p = self->extract_data;
    if (self->debug)
        printf ("get bin size=%zd\n", self->extract_size);
    return self->extract_size;
}


//  -------------------------------------------------------------------------
//  Update codec with actual amount of data extracted

static void
vtx_codec_bin_tick (vtx_codec_t *self, size_t size)
{
    assert (self);
    assert (size <= self->extract_size);

    if (size) {
        self->extract_size -= size;
        self->extract_data += size;
        if (self->extract_size == 0) {
            if (self->debug)
                printf (" -- bump batch head=%d (4)\n", self->batch_head);
            self->batch_head = ++self->batch_head % self->batch_limit;
            if (self->reader->msg) {
                zmq_msg_close (self->reader->msg);
                free (self->reader->msg);
            }
            else
                self->buffer_head = (self->buffer_head + self->reader->size)
                                   % self->buffer_limit;
        }
        self->active -= size;
    }
}


//  -------------------------------------------------------------------------
//  Return capacity for new input data, 0 means full

static size_t
vtx_codec_bin_space (vtx_codec_t *self)
{
    assert (self);

    if (s_batch_ready (self, 1) == 0)
        return self->free_space;
    else
        return 0;
}


//  -------------------------------------------------------------------------
//  Return active size of codec (message data plus headers)
static size_t
vtx_codec_active (vtx_codec_t *self)
{
    assert (self);

    return self->active;
}


//  -------------------------------------------------------------------------
//  Consistency check of codec, asserts if there's a fault

static void
vtx_codec_check (vtx_codec_t *self, char *text)
{
    assert (self);

    uint head = self->batch_head;
    while (head != self->batch_tail) {
        batch_t *batch = &self->batch [head];
        if (batch->size > 0 && batch->msg == NULL && batch->data [0] == 0
            && batch->data > self->buffer) {
            printf ("(%s) %p - ", text, batch->data);
            int i;
            for (i = 0; i < batch->size && i < 40; i++)
                printf ("%02x ", batch->data [i]);
            puts ("");
            printf ("Invalid zero data, at=%d size=%d this=%d head=%d tail=%d\n",
                    (int) (batch->data - self->buffer),
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
    //  Run randomized inserts/extracts for 1 second.
    //  This is NOT fast, if you want to run a performance test then
    //  remove the codec_store calls

    vtx_codec_t *codec1 = vtx_codec_new (100);
    vtx_codec_t *codec2 = vtx_codec_new (10000);
    codec1->debug = FALSE;
    codec2->debug = FALSE;
    int msg_count = 0;
    int64_t start = zclock_time ();

    while (TRUE) {
        //  Insert a bunch of messages
        int insert = s_random (1000);
        while (insert--) {
            //  80% smaller, 20% larger messages
            size_t size = s_random (s_random (10) < 8? ZMQ_MAX_VSM_SIZE: 5000);
            zmq_msg_t msg;
            zmq_msg_init_size (&msg, size);
            int rc = vtx_codec_msg_put (codec1, &msg, FALSE);
            vtx_codec_check (codec1, "msg put");
            msg_count++;
            zmq_msg_close (&msg);
            if (rc)
                break;          //  If store full, stop inserting
        }
        //  Recycle a bunch of messages as binary data
        while (TRUE) {
            byte *data;
            size_t size = vtx_codec_bin_get (codec1, &data);
            if (size == 0)
                break;          //  If store empty, stop recycling
            int rc = vtx_codec_bin_put (codec2, data, size);
            assert (rc == 0);
            vtx_codec_bin_tick (codec1, size);
            vtx_codec_check (codec1, "recycle1");
            vtx_codec_check (codec2, "recycle2");
        }
        assert (vtx_codec_active (codec1) == 0);

        //  Now extract a bunch of messages
        while (TRUE) {
            zmq_msg_t msg;
            Bool more;
            int rc = vtx_codec_msg_get (codec2, &msg, &more);
            vtx_codec_check (codec2, "msg get");
            zmq_msg_close (&msg);
            if (rc)
                break;          //  If store empty, stop extracting
        }
        assert (vtx_codec_active (codec2) == 0);

        //  Stop after 1 second of work
        if (zclock_time () - start > 999)
            break;
    }
    vtx_codec_destroy (&codec1);
    vtx_codec_destroy (&codec2);
    printf ("%d messages stored & extracted\n", msg_count);
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
