/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.cluster.metadata;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * Meta data about snapshots that are currently executing
 */
public class BenchmarkMetaData implements MetaData.Custom {
    public static final String TYPE = "benchmark";

    public static final Factory FACTORY = new Factory();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BenchmarkMetaData that = (BenchmarkMetaData) o;

        if (!entries.equals(that.entries)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return entries.hashCode();
    }

    public static class Entry {
        private final State state;
        private final String benchmarkId;
        private final String[] nodeids;

        public Entry(Entry e, State state) {
            this(e.benchmarkId(), state, e.nodes());
        }

        public Entry(String benchmarkId, State state, String[] nodeIds) {
            this.state = state;
            this.benchmarkId = benchmarkId;
            this.nodeids =  nodeIds;
        }

        public String benchmarkId() {
            return this.benchmarkId;
        }

        public State state() {
            return state;
        }

        public String[] nodes() {
            return nodeids;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Entry entry = (Entry) o;

            if (!benchmarkId.equals(entry.benchmarkId)) return false;
            if (state != entry.state) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = state.hashCode();
            result = 31 * result + benchmarkId.hashCode();
            return result;
        }
    }

    public static enum State {
        STARTED((byte) 0),
        SUCCESS((byte) 1),
        FAILED((byte) 2),
        ABORTED((byte) 3);

        private byte value;

        State(byte value) {
            this.value = value;
        }

        public byte value() {
            return value;
        }

        public boolean completed() {
            return this == SUCCESS || this == FAILED;
        }

        public static State fromValue(byte value) {
            switch (value) {
                case 0:
                    return STARTED;
                case 1:
                    return SUCCESS;
                case 2:
                    return FAILED;
                case 3:
                    return ABORTED;
                default:
                    throw new ElasticSearchIllegalArgumentException("No snapshot state for value [" + value + "]");
            }
        }
    }

    private final ImmutableList<Entry> entries;


    public BenchmarkMetaData(ImmutableList<Entry> entries) {
        this.entries = entries;
    }

    public BenchmarkMetaData(Entry... entries) {
        this.entries = ImmutableList.copyOf(entries);
    }

    public ImmutableList<Entry> entries() {
        return this.entries;
    }

    public Entry snapshot(SnapshotId snapshotId) {
        for (Entry entry : entries) {
            if (snapshotId.equals(entry.benchmarkId())) {
                return entry;
            }
        }
        return null;
    }


    public static class Factory implements MetaData.Custom.Factory<BenchmarkMetaData> {

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public BenchmarkMetaData readFrom(StreamInput in) throws IOException {
            Entry[] entries = new Entry[in.readVInt()];
            for (int i = 0; i < entries.length; i++) {
                String benchmarkId = in.readString();
                State state = State.fromValue(in.readByte());
                String[] nodes = in.readStringArray();
                entries[i] = new Entry(benchmarkId, state, nodes);
            }
            return new BenchmarkMetaData(entries);
        }

        @Override
        public void writeTo(BenchmarkMetaData repositories, StreamOutput out) throws IOException {
            out.writeVInt(repositories.entries().size());
            for (Entry entry : repositories.entries()) {
                out.writeString(entry.benchmarkId());
                out.writeByte(entry.state().value());
                out.writeStringArray(entry.nodes());
            }
        }

        @Override
        public BenchmarkMetaData fromXContent(XContentParser parser) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void toXContent(BenchmarkMetaData customIndexMetaData, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startArray("benchmarks");
            for (Entry entry : customIndexMetaData.entries()) {
                toXContent(entry, builder, params);
            }
            builder.endArray();
        }

        public void toXContent(Entry entry, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.field("id", entry.benchmarkId());
            builder.field("state", entry.state());
            builder.startArray("on_nodes");
            for (String nodeid : entry.nodes()) {
                builder.value(nodeid);
            }
            builder.endArray();
            builder.endObject();
        }

        public boolean isPersistent() {
            return false;
        }

    }

    public boolean contains(String benchmarkId) {
        for (Entry e : entries) {
           if (e.benchmarkId.equals(benchmarkId)) {
               return true;
           }
        }
        return false;
    }


}
