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
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.NoopStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Version 1 of the translog file format. Writes a header to identify the
 * format, also writes checksums for each operation
 */
public class ChecksummedTranslogStream implements TranslogStream {

    public static final int VERSION_CHECKSUMS = 1;
    public static final int VERSION_CHECKPOINTS = 2; // since 2.0 we have checkpoints?
    public static final int VERSION = VERSION_CHECKPOINTS;

    ChecksummedTranslogStream() {
    }

    private void verifyChecksum(BufferedChecksumStreamInput in) throws IOException {
        // This absolutely must come first, or else reading the checksum becomes part of the checksum
        long expectedChecksum = in.getChecksum();
        long readChecksum = in.readInt() & 0xFFFF_FFFFL;
        if (readChecksum != expectedChecksum) {
            throw new TranslogCorruptedException("translog stream is corrupted, expected: 0x" +
                    Long.toHexString(expectedChecksum) + ", got: 0x" + Long.toHexString(readChecksum));
        }
    }

    @Override
    public Translog.Operation read(StreamInput inStream) throws IOException {
        // TODO: validate size to prevent OOME
        int opSize = inStream.readInt();
        // This BufferedChecksumStreamInput remains unclosed on purpose,
        // because closing it closes the underlying stream, which we don't
        // want to do here.
        BufferedChecksumStreamInput in = new BufferedChecksumStreamInput(inStream);
        Translog.Operation operation;
        try {
            Translog.Operation.Type type = Translog.Operation.Type.fromId(in.readByte());
            operation = TranslogStreams.newOperationFromType(type);
            operation.readFrom(in);
            verifyChecksum(in);
        } catch (EOFException e) {
            throw new TruncatedTranslogException("reached premature end of file, translog is truncated", e);
        } catch (AssertionError|Exception e) {
            throw new TranslogCorruptedException("translog corruption while reading from stream", e);
        }
        return operation;
    }

    @Override
    public void write(StreamOutput outStream, Translog.Operation op) throws IOException {
        // We first write to a NoopStreamOutput to get the size of the
        // operation. We could write to a byte array and then send that as an
        // alternative, but here we choose to use CPU over allocating new
        // byte arrays.
        NoopStreamOutput noopOut = new NoopStreamOutput();
        noopOut.writeByte(op.opType().id());
        op.writeTo(noopOut);
        noopOut.writeInt(0); // checksum holder
        int size = noopOut.getCount();

        // This BufferedChecksumStreamOutput remains unclosed on purpose,
        // because closing it closes the underlying stream, which we don't
        // want to do here.
        BufferedChecksumStreamOutput out = new BufferedChecksumStreamOutput(outStream);
        outStream.writeInt(size); // opSize is not checksummed
        out.writeByte(op.opType().id());
        op.writeTo(out);
        long checksum = out.getChecksum();
        out.writeInt((int)checksum);
    }

    @Override
    public int writeHeader(FileChannel channel) throws IOException {
        // This OutputStreamDataOutput is intentionally not closed because
        // closing it will close the FileChannel
        OutputStreamDataOutput out = new OutputStreamDataOutput(Channels.newOutputStream(channel));
        CodecUtil.writeHeader(out, TranslogStreams.TRANSLOG_CODEC, VERSION);
        return headerLength();
    }

    @Override
    public int headerLength() {
        return CodecUtil.headerLength(TranslogStreams.TRANSLOG_CODEC);
    }

    @Override
    public StreamInput openInput(Path translogFile) throws IOException {
        final InputStream fileInputStream = Files.newInputStream(translogFile);
        boolean success = false;
        try {
            final InputStreamStreamInput in = new InputStreamStreamInput(fileInputStream);
            CodecUtil.checkHeader(new InputStreamDataInput(in), TranslogStreams.TRANSLOG_CODEC, VERSION_CHECKSUMS, VERSION);
            success = true;
            return in;
        } catch (IOException e) {
            throw new TranslogCorruptedException("translog header corrupted", e);
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(fileInputStream);
            }
        }
    }

    @Override
    public Checkpoint getLatestCheckpoint(ChannelReference reference) throws IOException {
        try (final InputStream fileInputStream = Files.newInputStream(reference.file())) {
            final InputStreamStreamInput in = new InputStreamStreamInput(fileInputStream);
            final int version = CodecUtil.checkHeader(new InputStreamDataInput(in), TranslogStreams.TRANSLOG_CODEC, VERSION_CHECKSUMS, VERSION);
            switch (version) {
                case VERSION_CHECKPOINTS:
                    return ChannelReference.findCheckPoint(reference.file());
                case VERSION_CHECKSUMS:
                    return new Checkpoint(reference.channel().size(), -1, 0);
                default:
                    throw new IllegalStateException("Unknown version: " + version);
            }
        }
    }

}
