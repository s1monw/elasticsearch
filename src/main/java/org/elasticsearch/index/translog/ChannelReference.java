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

import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;

class ChannelReference extends AbstractRefCounted {

    private final Path file;

    private final FileChannel channel;

    private final TranslogStream stream;

    public ChannelReference(Path file, boolean readOnly) throws IOException {
        super(file.toString());
        this.file = file;
        if (readOnly) {
            this.channel = FileChannel.open(file, StandardOpenOption.READ);
            try {
                this.stream = TranslogStreams.translogStreamFor(file);
            } catch (Throwable t) {
                IOUtils.closeWhileHandlingException(channel);
                throw t;
            }
        } else {
            this.stream = TranslogStreams.LATEST;
            Path pendingFile = file.resolveSibling("pending_" + file.getFileName());
            final int headerLength;
            try (FileChannel channel = FileChannel.open(pendingFile, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
                headerLength = this.stream.writeHeader(channel);
                channel.force(false);
                writeCheckpoint(headerLength, 0);
            }
            Files.move(pendingFile, file, StandardCopyOption.ATOMIC_MOVE);

            this.channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE);
            this.channel.position(headerLength);
        }


    }

    public Path file() {
        return this.file;
    }

    public FileChannel channel() {
        return this.channel;
    }

    public TranslogStream stream() {
        return this.stream;
    }

    @Override
    public String toString() {
        return "channel: file [" + file + "], ref count [" + refCount() + "]";
    }

    @Override
    protected void closeInternal() {
        IOUtils.closeWhileHandlingException(channel);
    }

    public void checkpoint(long lastSyncPosition, int operationCounter) throws IOException {
        channel.force(false);
        writeCheckpoint(lastSyncPosition, operationCounter);
    }

//    @SuppressForbidden(reason = "We need control over if the channel write succeeded")
    private void writeCheckpoint(long syncPosition, int numOperations) throws IOException {
        final Path checkpointFile = checkpointFile(file);
        try (FileChannel channel = FileChannel.open(checkpointFile, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            Checkpoint pos = new Checkpoint(syncPosition, numOperations);
            byte[] buffer = new byte[RamUsageEstimator.NUM_BYTES_INT + RamUsageEstimator.NUM_BYTES_LONG];

            pos.write(new ByteArrayDataOutput(buffer));
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

    public static Checkpoint openCheckpoint(Path translogFile) throws IOException {
        try (InputStream in = Files.newInputStream(checkpointFile(translogFile))) {
            return new Checkpoint(new InputStreamDataInput(in));
        }
    }

    private static Path checkpointFile(Path file) {
        return file.resolveSibling("checkpoint_" + file.getFileName().toString());
    }

    public Checkpoint checkpointFromStream() throws IOException {
        return stream.getLatestCheckpoint(this);
    }
}
