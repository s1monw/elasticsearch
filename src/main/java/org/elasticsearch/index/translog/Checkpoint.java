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

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

import java.io.IOException;

/**
 */
class Checkpoint implements Comparable<Checkpoint> {
    final long syncedPosition;
    final int numWrittenOperations;
    final int generation;

    Checkpoint(long syncedPosition, int numWrittenOperations, int generation) {
        this.syncedPosition = syncedPosition;
        this.numWrittenOperations = numWrittenOperations;
        this.generation = generation;
    }

    Checkpoint(DataInput in) throws IOException {
        syncedPosition = in.readLong();
        numWrittenOperations = in.readInt();
        generation = in.readInt();
    }

    @Override
    public int compareTo(Checkpoint o) {
        return Long.compare(syncedPosition, o.syncedPosition);
    }

    void write(DataOutput out) throws IOException {
        out.writeLong(syncedPosition);
        out.writeInt(numWrittenOperations);
        out.writeInt(generation);
    }

    @Override
    public String toString() {
        return "Checkpoint{" +
                "syncedPosition=" + syncedPosition +
                ", numWrittenOperations=" + numWrittenOperations +
                ", generation=" + generation +
                '}';
    }
}
