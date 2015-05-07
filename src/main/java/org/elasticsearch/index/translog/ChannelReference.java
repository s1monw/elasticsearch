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

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.util.Callback;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;

final class ChannelReference extends AbstractRefCounted {

    private final Path file;

    private final FileChannel channel;

    protected final long translogId;
    private final Callback<ChannelReference> onClose;

    public ChannelReference(Path file, long translogId, FileChannel channel, Callback<ChannelReference> onClose) throws IOException {
        super(file.toString());
        this.translogId = translogId;
        this.file = file;
        this.channel = channel;
        this.onClose = onClose;
//        if (readOnly) {
//            this.channel = FileChannel.open(file, StandardOpenOption.READ);
//        } else {

//        }


    }

    public long getId() {
        return translogId;
    }

    public Path file() {
        return this.file;
    }

    public FileChannel channel() {
        return this.channel;
    }


    @Override
    public String toString() {
        return "channel: file [" + file + "], ref count [" + refCount() + "]";
    }

    @Override
    protected void closeInternal() {
        try {
            IOUtils.closeWhileHandlingException(channel);
        } finally {
            if (onClose != null) {
                onClose.handle(this);
            }
        }

    }


}
