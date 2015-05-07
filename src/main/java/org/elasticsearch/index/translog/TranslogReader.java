/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.translog;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.InputStreamDataInput;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A base class for all classes that allows reading ops from translog files
 */
public abstract class TranslogReader implements Closeable, Comparable<TranslogReader> {

    public static final int UNKNOWN_OP_COUNT = -1;

    protected final long id;
    protected final ChannelReference channelReference;
    protected final FileChannel channel;
    protected final AtomicBoolean closed = new AtomicBoolean(false);

    public TranslogReader(long id, ChannelReference channelReference) {
        this.id = id;
        this.channelReference = channelReference;
        this.channel = channelReference.channel();
    }

    public long translogId() {
        return this.id;
    }

    abstract public long sizeInBytes();

    /** the position the first operation is written at */
    public long firstPosition() {
        return CodecUtil.headerLength(TRANSLOG_CODEC);
    }

    abstract public int totalOperations();

    public Translog.Operation read(Translog.Location location) throws IOException {
        assert location.translogId == id : "read location's translog id [" + location.translogId + "] is not [" + id + "]";
        ByteBuffer buffer = ByteBuffer.allocate(location.size);
        return read(buffer, location.translogLocation, location.size);
    }

    /** read the size of the op (i.e., number of bytes, including the op size) written at the given position */
    public final int readSize(ByteBuffer reusableBuffer, long position) {
        // read op size from disk
        assert reusableBuffer.capacity() >= 4 : "reusable buffer must have capacity >=4 when reading opSize. got [" + reusableBuffer.capacity() + "]";
        try {
            reusableBuffer.clear();
            reusableBuffer.limit(4);
            readBytes(reusableBuffer, position);
            reusableBuffer.flip();
            // Add an extra 4 to account for the operation size integer itself
            final int size = reusableBuffer.getInt() + 4;
            final long maxSize = sizeInBytes() - position;
            if (size < 0 || size > maxSize) {
                throw new TranslogCorruptedException("operation size is corrupted must be [0.." + maxSize + "] but was: " + size);
            }

            return size;
        } catch (IOException e) {
            throw new ElasticsearchException("unexpected exception reading from translog snapshot of " + this.channelReference.file(), e);
        }
    }

    /**
     * reads an operation at the given position and returns it. The buffer length is equal to the number
     * of bytes reads.
     */
    public final Translog.Operation read(ByteBuffer reusableBuffer, long position, int opSize) throws IOException {
        final ByteBuffer buffer;
        if (reusableBuffer.capacity() >= opSize) {
            buffer = reusableBuffer;
        } else {
            buffer = ByteBuffer.allocate(opSize);
        }
        buffer.clear();
        buffer.limit(opSize);
        readBytes(buffer, position);
        BytesArray bytesArray = new BytesArray(buffer.array(), 0, buffer.limit());
        return read(bytesArray.streamInput());
    }



    public Translog.Operation read(StreamInput inStream) throws IOException {
        return Translog.readOperation(inStream);
    }

    /**
     * reads bytes at position into the given buffer, filling it.
     */
    abstract protected void readBytes(ByteBuffer buffer, long position) throws IOException;

    /** create snapshot for this channel */
    abstract ChannelSnapshot newChannelSnapshot();

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            doClose();
        }
    }

    protected void doClose() throws IOException {
        channelReference.decRef();
    }

    @Override
    public String toString() {
        return "translog [" + id + "][" + channelReference.file() + "]";
    }

    @Override
    public int compareTo(TranslogReader o) {
        return Long.compare(translogId(), o.translogId());
    }


    public static final String TRANSLOG_CODEC = "translog";
    private static final byte LUCENE_CODEC_HEADER_BYTE = 0x3f;
    private static final byte UNVERSIONED_TRANSLOG_HEADER_BYTE = 0x00;

    /**
     * Given a file, return a VersionedTranslogStream based on an
     * optionally-existing header in the file. If the file does not exist, or
     * has zero length, returns the latest version. If the header does not
     * exist, assumes Version 0 of the translog file format.
     * <p/>
     *
     * @throws IOException
     */
    public static ImmutableTranslogReader open(ChannelReference channelReference) throws IOException {
        try {
            if (channelReference.file().endsWith(Translog.TRANSLOG_FILE_SUFFIX) == false && channelReference.channel().size() == 0) {
                return new LegacyTranslogReader(channelReference.getId(), channelReference);
            }

            InputStreamStreamInput headerStream = new InputStreamStreamInput(Channels.newInputStream(channelReference.channel())); // don't close
            // Lucene's CodecUtil writes a magic number of 0x3FD76C17 with the
            // header, in binary this looks like:
            //
            // binary: 0011 1111 1101 0111 0110 1100 0001 0111
            // hex   :    3    f    d    7    6    c    1    7
            //
            // With version 0 of the translog, the first byte is the
            // Operation.Type, which will always be between 0-4, so we know if
            // we grab the first byte, it can be:
            // 0x3f => Lucene's magic number, so we can assume it's version 1 or later
            // 0x00 => version 0 of the translog
            //
            // otherwise the first byte of the translog is corrupted and we
            // should bail
            byte b1 = headerStream.readByte();
            if (b1 == LUCENE_CODEC_HEADER_BYTE) {
                // Read 3 more bytes, meaning a whole integer has been read
                byte b2 = headerStream.readByte();
                byte b3 = headerStream.readByte();
                byte b4 = headerStream.readByte();
                // Convert the 4 bytes that were read into an integer
                int header = ((b1 & 0xFF) << 24) + ((b2 & 0xFF) << 16) + ((b3 & 0xFF) << 8) + ((b4 & 0xFF) << 0);
                // We confirm CodecUtil's CODEC_MAGIC number (0x3FD76C17)
                // ourselves here, because it allows us to read the first
                // byte separately
                if (header != CodecUtil.CODEC_MAGIC) {
                    throw new TranslogCorruptedException("translog looks like version 1 or later, but has corrupted header");
                }
                // Confirm the rest of the header using CodecUtil, extracting
                // the translog version
                int version = CodecUtil.checkHeaderNoMagic(new InputStreamDataInput(headerStream), TRANSLOG_CODEC, 1, Integer.MAX_VALUE);
                final Checkpoint checkPoint;
                switch (version) {
                    case TranslogWriter.VERSION_CHECKSUMS:
                        // legacy - we still have to support it somehow
                        checkPoint = new Checkpoint(channelReference.channel().size(), TranslogReader.UNKNOWN_OP_COUNT);
                        break;
                    case TranslogWriter.VERSION_CHECKPOINTS:
                        checkPoint = openCheckpoint(channelReference.file());
                        assert checkPoint.syncedPosition <= channelReference.channel().size() : "checkpoint is inconsistent with channel length:" + channelReference.channel().size() + " " + checkPoint;
                        break;
                    default:
                        throw new TranslogCorruptedException("No known translog stream version: " + version);
                }
                return new ImmutableTranslogReader(channelReference.getId(), channelReference, checkPoint.syncedPosition, checkPoint.numWrittenOperations);

            } else if (b1 == UNVERSIONED_TRANSLOG_HEADER_BYTE) {
                return new LegacyTranslogReader(channelReference.getId(), channelReference);
            } else {
                throw new TranslogCorruptedException("Invalid first byte in translog file, got: " + Long.toHexString(b1) + ", expected 0x00 or 0x3f");
            }
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException e) {
            throw new TranslogCorruptedException("Translog header corrupted", e);
        }
    }

    public static Checkpoint openCheckpoint(Path translogFile) throws IOException {
        try (InputStream in = Files.newInputStream(checkpointFile(translogFile))) {
            return new Checkpoint(new InputStreamDataInput(in));
        }
    }

    protected static Path checkpointFile(Path file) {
        return file.resolveSibling("checkpoint_" + file.getFileName().toString());
    }

    public Path path() {
        return channelReference.file();
    }
}
