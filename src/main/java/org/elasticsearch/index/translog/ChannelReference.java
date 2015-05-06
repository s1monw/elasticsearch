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
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.*;

class ChannelReference extends AbstractRefCounted {

    private static final Checkpoint EMPTY_CHECKPOINT = new Checkpoint(0,0,0);

    private final Path file;

    private final FileChannel channel;

    private final TranslogStream stream;
    private int generation = -1;


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

    private long writeCheckpoint(long syncPosition, int numOperations) throws IOException {
        generation++;
        try(final OutputStream outputStream = Files.newOutputStream(file.resolveSibling(generationFile(generation)), StandardOpenOption.DSYNC, StandardOpenOption.CREATE_NEW)) {
            final byte[] buffer = new byte[RamUsageEstimator.NUM_BYTES_INT + RamUsageEstimator.NUM_BYTES_INT + RamUsageEstimator.NUM_BYTES_LONG];
            Checkpoint pos = new Checkpoint(syncPosition, numOperations, generation);
            ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
            pos.write(out);
            outputStream.write(buffer);

        }
        Files.deleteIfExists(file.resolveSibling(generationFile(generation - 1)));
        IOUtils.fsync(file.getParent(), true); //nocommit do we have to do that?
        return generation;
    }

    public static Checkpoint findCheckPoint(Path translogFile) throws IOException {
        Checkpoint currentPosition = EMPTY_CHECKPOINT;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(translogFile.getParent(), "checkpoint_" + translogFile.getFileName().toString() + "_*.gen")) {
            for (Path path : stream) {
                try (InputStream in = Files.newInputStream(path)) {
                    final Checkpoint checkpoint = new Checkpoint(new InputStreamDataInput(in));
                    if (checkpoint.compareTo(currentPosition) > 0) {
                        currentPosition = checkpoint;
                    }
                }
            }
        }
        return currentPosition;
    }

    private String generationFile(long generation) {
        return "checkpoint_" + file.getFileName().toString() + "_" + generation + ".gen";
    }

    public Checkpoint checkpointFromStream() throws IOException {
        return stream.getLatestCheckpoint(this);
    }
}
