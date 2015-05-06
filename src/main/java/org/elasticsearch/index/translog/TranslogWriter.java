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
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.io.stream.NoopStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.Callback;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TranslogWriter extends TranslogReader {

    public static final String TRANSLOG_CODEC = "translog";
    public static final int VERSION_CHECKSUMS = 1;
    public static final int VERSION_CHECKPOINTS = 2; // since 2.0 we have checkpoints?
    public static final int VERSION = VERSION_CHECKPOINTS;

    protected final ShardId shardId;
    protected final ReleasableLock readLock;
    protected final ReleasableLock writeLock;
    /* the offset in bytes that was written when the file was last synced*/
    protected volatile long lastSyncedOffset;
    /* the number of translog operations written to this file */
    protected volatile int operationCounter;
    /* the offset in bytes written to the file */
    protected volatile long writtenOffset;

    public TranslogWriter(ShardId shardId, long id, ChannelReference channelReference) throws IOException {
        super(id, channelReference);
        this.shardId = shardId;
        ReadWriteLock rwl = new ReentrantReadWriteLock();
        readLock = new ReleasableLock(rwl.readLock());
        writeLock = new ReleasableLock(rwl.writeLock());
        final int headerLength = CodecUtil.headerLength(TRANSLOG_CODEC);
        this.writtenOffset = headerLength;
        this.lastSyncedOffset = headerLength;
        checkpoint(lastSyncedOffset, operationCounter);
    }

    public static TranslogWriter create(Type type, ShardId shardId, long id, Path file, Callback<ChannelReference> onClose, int bufferSize) throws IOException {
        Path pendingFile = file.resolveSibling("pending_" + file.getFileName());
        final int headerLength = CodecUtil.headerLength(TRANSLOG_CODEC);
        try (FileChannel channel = FileChannel.open(pendingFile, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
            // This OutputStreamDataOutput is intentionally not closed because
            // closing it will close the FileChannel
            OutputStreamDataOutput out = new OutputStreamDataOutput(java.nio.channels.Channels.newOutputStream(channel));
            CodecUtil.writeHeader(out, TRANSLOG_CODEC, VERSION);
            channel.force(false);
            writeCheckpoint(headerLength, 0, file);
        }
        Files.move(pendingFile, file, StandardCopyOption.ATOMIC_MOVE);
        FileChannel channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE);
        boolean success = false;
        try {
            channel.position(headerLength);
            final TranslogWriter writer = type.create(shardId, id, new ChannelReference(file, id, channel, onClose), bufferSize);
            success = true;
            return writer;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(channel);
            }
        }
    }

    public enum Type {

        SIMPLE() {
            @Override
            public TranslogWriter create(ShardId shardId, long id, ChannelReference channelReference, int bufferSize) throws IOException {
                return new TranslogWriter(shardId, id, channelReference);
            }
        },
        BUFFERED() {
            @Override
            public TranslogWriter create(ShardId shardId, long id, ChannelReference channelReference, int bufferSize) throws IOException {
                return new BufferingTranslogWriter(shardId, id, channelReference, bufferSize);
            }
        };

        public abstract TranslogWriter create(ShardId shardId, long id, ChannelReference raf, int bufferSize) throws IOException;

        public static Type fromString(String type) {
            if (SIMPLE.name().equalsIgnoreCase(type)) {
                return SIMPLE;
            } else if (BUFFERED.name().equalsIgnoreCase(type)) {
                return BUFFERED;
            }
            throw new IllegalArgumentException("No translog fs type [" + type + "]");
        }
    }


    /**
     * add the given bytes to the translog and return the location they were written at
     */
    public Translog.Location add(BytesReference data) throws IOException {
        try (ReleasableLock lock = writeLock.acquire()) {
            long position = writtenOffset;
            data.writeTo(channel);
            writtenOffset = writtenOffset + data.length();
            operationCounter = operationCounter + 1;
            return new Translog.Location(id, position, data.length());
        }
    }

    /**
     * change the size of the internal buffer if relevant
     */
    public void updateBufferSize(int bufferSize) throws TranslogException {
    }

    /**
     * write all buffered ops to disk and fsync file
     */
    public void sync() throws IOException {
        // check if we really need to sync here...
        if (syncNeeded()) {
            try (ReleasableLock lock = writeLock.acquire()) {
                lastSyncedOffset = writtenOffset;
                checkpoint(lastSyncedOffset, operationCounter);
            }
        }
    }

    /**
     * returns true if there are buffered ops
     */
    public boolean syncNeeded() {
        return writtenOffset != lastSyncedOffset; // by default nothing is buffered
    }

    @Override
    public int totalOperations() {
        return operationCounter;
    }

    @Override
    public long sizeInBytes() {
        return writtenOffset;
    }

    @Override
    public ChannelSnapshot newChannelSnapshot() {
        return new ChannelSnapshot(immutableReader());
    }

    /**
     * Flushes the buffer if the translog is buffered.
     */
    protected void flush() throws IOException {
    }

    /**
     * returns a new reader that follows the current writes (most importantly allows making
     * repeated snapshots that includes new content)
     */
    public TranslogReader reader() {
        channelReference.incRef();
        boolean success = false;
        try {
            TranslogReader reader = new InnerReader(this.id, channelReference);
            success = true;
            return reader;
        } finally {
            if (!success) {
                channelReference.decRef();
            }
        }
    }


    /**
     * returns a new immutable reader which only exposes the current written operation *
     */
    public ImmutableTranslogReader immutableReader() throws TranslogException {
        if (channelReference.tryIncRef()) {
            try (ReleasableLock lock = writeLock.acquire()) {
                flush();
                ImmutableTranslogReader reader = new ImmutableTranslogReader(this.id, channelReference, writtenOffset, operationCounter);
                channelReference.incRef(); // for new reader
                return reader;
            } catch (Exception e) {
                throw new TranslogException(shardId, "exception while creating an immutable reader", e);
            } finally {
                channelReference.decRef();
            }
        } else {
            throw new TranslogException(shardId, "can't increment channel [" + channelReference + "] ref count");
        }
    }

    boolean assertBytesAtLocation(Translog.Location location, BytesReference expectedBytes) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(location.size);
        readBytes(buffer, location.translogLocation);
        return new BytesArray(buffer.array()).equals(expectedBytes);
    }

    /**
     * this class is used when one wants a reference to this file which exposes all recently written operation.
     * as such it needs access to the internals of the current reader
     */
    final class InnerReader extends TranslogReader {

        public InnerReader(long id, ChannelReference channelReference) {
            super(id, channelReference);
        }

        @Override
        public long sizeInBytes() {
            return TranslogWriter.this.sizeInBytes();
        }

        @Override
        public int totalOperations() {
            return TranslogWriter.this.totalOperations();
        }

        @Override
        protected void readBytes(ByteBuffer buffer, long position) throws IOException {
            TranslogWriter.this.readBytes(buffer, position);
        }

        @Override
        public ChannelSnapshot newChannelSnapshot() {
            return TranslogWriter.this.newChannelSnapshot();
        }
    }

    /**
     * Syncs the translog up to at least the given offset unless already synced
     *
     * @return <code>true</code> if this call caused an actual sync operation
     */
    public boolean syncUpTo(long offset) throws IOException {
        if (lastSyncedOffset < offset) {
            sync();
            return true;
        }
        return false;
    }

    @Override
    protected final void doClose() throws IOException {
        try {
            sync();
        } finally {
            super.doClose();
        }
    }

    @Override
    protected void readBytes(ByteBuffer buffer, long position) throws IOException {
        try (ReleasableLock lock = readLock.acquire()) {
            Channels.readFromFileChannelWithEofException(channel, position, buffer);
        }
    }

    protected synchronized void checkpoint(long lastSyncPosition, int operationCounter) throws IOException {
        channel.force(false);
        writeCheckpoint(lastSyncPosition, operationCounter, channelReference.getPath());
    }

    //    @SuppressForbidden(reason = "We need control over if the channel write succeeded")
    private static void writeCheckpoint(long syncPosition, int numOperations, Path translogFile) throws IOException {
        final Path checkpointFile = checkpointFile(translogFile);
        try (FileChannel channel = FileChannel.open(checkpointFile, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            Checkpoint checkpoint = new Checkpoint(syncPosition, numOperations);
            byte[] buffer = new byte[RamUsageEstimator.NUM_BYTES_INT + RamUsageEstimator.NUM_BYTES_LONG];

            checkpoint.write(new ByteArrayDataOutput(buffer));
            Channels.writeToChannel(buffer, channel);
            /* //nocommit should we rather do our own writing here?
            ByteBuffer bb = ByteBuffer.wrap(buffer, 0, buffer.length);
            final int write = channel.write(bb);
            if (write != buffer.length) { // hmm should we retry here?
                throw new IllegalStateException("write checkpoint failed only wrote: " + write + " bytes");
            }
            */
            channel.force(false);
        }
    }

    protected static Path checkpointFile(Path file) {
        return file.resolveSibling("checkpoint_" + file.getFileName().toString());
    }
}
