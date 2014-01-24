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
package org.elasticsearch.search.suggest.completion;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.suggest.Suggest;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class CompletionSuggestion extends Suggest.Suggestion<CompletionSuggestion.Entry> {

    public static final int TYPE = 2;

    public CompletionSuggestion() {
    }

    public CompletionSuggestion(String name, int size) {
        super(name, size);
    }

    @Override
    public int getType() {
        return TYPE;
    }

    @Override
    protected Entry newEntry() {
        return new Entry();
    }

    public static class Entry extends org.elasticsearch.search.suggest.Suggest.Suggestion.Entry<CompletionSuggestion.Entry.Option> {

        public Entry(Text text, int offset, int length) {
            super(text, offset, length);
        }

        protected Entry() {
            super();
        }

        @Override
        protected Option newOption() {
            return new Option();
        }

        public static class Option extends org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option {
            private BytesReference payload;
            private boolean deduplicate = true;

            public Option(Text text, float score, BytesReference payload, boolean deduplicate) {
                super(text, score);
                this.payload = payload;
                this.deduplicate = deduplicate;
            }


            protected Option() {
                super();
            }

            public void setPayload(BytesReference payload) {
                this.payload = payload;
            }

            public BytesReference getPayload() {
                return payload;
            }

            public String getPayloadAsString() {
                return payload.toUtf8();
            }

            public long getPayloadAsLong() {
                return Long.parseLong(payload.toUtf8());
            }

            public double getPayloadAsDouble() {
                return Double.parseDouble(payload.toUtf8());
            }

            public Map<String, Object> getPayloadAsMap() {
                return XContentHelper.convertToMap(payload, false).v2();
            }

            public void setScore(float score) {
                super.setScore(score);
            }

            @Override
            protected XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
                super.innerToXContent(builder, params);
                if (payload != null && payload.length() > 0) {
                    builder.rawField("payload", payload);
                }
                return builder;
            }

            @Override
            public void readFrom(StreamInput in) throws IOException {
                super.readFrom(in);
                payload = in.readBytesReference();
                if (in.getVersion().onOrAfter(Version.V_1_0_0_RC2)) {
                    deduplicate = in.readBoolean();
                }
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
                out.writeBytesReference(payload);
                if (out.getVersion().onOrAfter(Version.V_1_0_0_RC2)) {
                    out.writeBoolean(deduplicate);
                }
            }

            @Override
            public boolean equals(Object o) {
                if (deduplicate) {
                    return super.equals(o);
                }
                if (this == o) return true;
                if (o != null && o instanceof Option && super.equals(o)) {
                    Option that = (Option) o;
                    if (payload == null) {
                        return payload.equals(that.payload);
                    }
                    return that.payload == null;
                }
                return false;
            }

            @Override
            public int hashCode() {
                if (deduplicate) {
                    return super.hashCode();
                }
                int result = super.hashCode();
                if (payload != null) {
                    result = 31 * result + payload.hashCode();
                }
                return result;
            }
        }
    }

}
