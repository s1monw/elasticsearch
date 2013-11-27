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

package org.elasticsearch.action.bench;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The response of a benchmark action.
 */
public class BenchResponse extends ActionResponse implements Streamable, ToXContent {
    private final List<BenchSpecResult> specResults;
    private String[] failures = Strings.EMPTY_ARRAY;
    private boolean aborted = false;

    // TODOs:
    // round summaries
    // add support to send per query result to another index

    BenchResponse() {
        specResults = new ArrayList<BenchSpecResult>();
    }

    BenchResponse(List<BenchSpecResult> specResults) {
        this.specResults = specResults;
    }

    public List<BenchSpecResult> competitorResults() {
        return this.specResults;
    }


    public String[] failures() {
        return this.failures;
    }

    public void failures(String[] failures) {
        if (failures == null) {
            this.failures = Strings.EMPTY_ARRAY;
        } else {
            this.failures = failures;
        }
    }

    public void aborted(boolean aborted) {
        this.aborted = aborted;
    }

    public boolean isAborted() {
        return aborted;
    }

    public boolean hasFailures() {
        return failures.length > 0;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("aborted", aborted);
        builder.startArray("specs");
        for (BenchSpecResult comp : specResults) {
            builder.startObject();
            comp.toXContent(builder, params);
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        aborted = in.readBoolean();
        failures = in.readStringArray();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            BenchSpecResult r = new BenchSpecResult();
            r.readFrom(in);
            this.specResults.add(r);
        }

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(aborted);
        out.writeStringArray(failures);
        out.writeVInt(specResults.size());
        for (BenchSpecResult r : specResults) {
            r.writeTo(out);
        }

    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return builder.string();
        } catch (IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }
}
