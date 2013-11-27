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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The response of a benchmark action.
 */
public class BenchSpecResult extends ActionResponse implements Streamable, ToXContent {
    private final List<Iteration> iters;
    private String name;
    private int iterations;
    private int multiplier;
    private int totalQueries;
    private int concurrency;
    private long warmupTook = -1;
    // NOCOMMIT: add from which node this comes?
    // TODOs:
    // round summaries
    // add support to send per query result to another index


    BenchSpecResult() {
        iters = new ArrayList<Iteration>();
    }

    BenchSpecResult(String name, List<Iteration> iters, int iterations, int multiplier, int totalQueries, int concurrency) {
        this.name = name;
        this.iters = iters;
        this.iterations = iterations;
        this.multiplier = multiplier;
        this.totalQueries = totalQueries;
        this.concurrency = concurrency;
    }

    public List<Iteration> iters() {
        return this.iters;
    }

    public void warmupTook(long warmupTook) {
        this.warmupTook = warmupTook;
    }

    public long warmupTook() {
        return this.warmupTook;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("name", name);
        builder.field("num_iterations", iterations);
        builder.field("multiplier", multiplier);
        builder.field("total_num_queries", totalQueries);
        builder.field("concurrency", concurrency);
        if (warmupTook != -1) {
            builder.field("warmupTook", warmupTook);
        }

        builder.startArray("iterations");
        for (Iteration iter : iters) {
            iter.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        name = in.readString();
        iterations = in.readVInt();
        multiplier = in.readVInt();
        totalQueries = in.readVInt();
        warmupTook = in.readLong(); // no VLong it can be negative.
        concurrency = in.readVInt();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            Iteration r = new Iteration();
            r.readFrom(in);
            this.iters.add(r);
        }

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
        out.writeVInt(iterations);
        out.writeVInt(multiplier);
        out.writeVInt(totalQueries);
        out.writeLong(warmupTook);
        out.writeVInt(concurrency);
        out.writeVInt(iters.size());
        for (Iteration r : iters) {
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


    public static class Iteration implements ToXContent, Streamable {
        private long numQueries;
        private long percentile90th;
        private SlowestRequests slowestRequests;
        private long totalTime;
        private long sum;
        private double stdDev;
        private long median;
        private long mean;
        private double qps;
        private double millisPerHit;

        public Iteration() {}

        public Iteration(SlowestRequests slowestRequests, long totalTime, long numQueries, long sumTotalHits, BenchStatistics statistics) {
            this.totalTime = totalTime;
            this.slowestRequests = slowestRequests;
            sum = statistics.sum();
            stdDev = statistics.stdDev();
            median = statistics.median();
            percentile90th = statistics.percentile(0.9);
            mean = statistics.mean();
            this.numQueries = numQueries;
            millisPerHit = totalTime / (double)sumTotalHits;
            qps = numQueries * (1000.d/(double)sum);
        }

        public long percentile90th() {
            return percentile90th;
        }

        public SlowestRequests slowest() {
            return slowestRequests;
        }

        public long totalTime() {
            return totalTime;
        }

        public double stdDev() {
            return stdDev;
        }

        public long sum() {
            return sum;
        }

        public long median() {
            return median;
        }

        public long mean() {
            return mean;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            totalTime = in.readVLong();
            numQueries = in.readVLong();
            qps = in.readDouble();
            millisPerHit = in.readDouble();
            sum = in.readVLong();
            stdDev = in.readDouble();
            median = in.readVLong();
            mean = in.readVLong();
            percentile90th = in.readVLong();
            slowestRequests = in.readOptionalStreamable(new SlowestRequests());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(totalTime);
            out.writeVLong(numQueries);
            out.writeDouble(qps);
            out.writeDouble(millisPerHit);
            out.writeVLong(sum);
            out.writeDouble(stdDev);
            out.writeVLong(median);
            out.writeVLong(mean);
            out.writeVLong(percentile90th);
            out.writeOptionalStreamable(slowestRequests);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("total_time", totalTime);
            builder.field("num_queries", numQueries);
            builder.field("qps", qps);
            builder.field("millis_per_hit", millisPerHit);
            builder.field("sum", sum);
            builder.field("std_dev", stdDev);
            builder.field("median", median);
            builder.field("mean", mean);
            builder.field("90th_percentile", percentile90th);
            if (slowestRequests != null) {
                builder.field("slowest");
                slowestRequests.toXContent(builder, params);
            }
            builder.endObject();
            return builder;
        }
    }

    public static class SlowestRequests implements ToXContent, Streamable {
        private long[] maxTimeTaken;
        private long[] avgTimeTaken;
        private SearchRequest[] requests;

        public SlowestRequests() {
        }

        public SlowestRequests(SearchRequest[] req, long[] maxTimeTaken, long[] avgTimeTaken) {
            this.requests(req);
            this.maxTimeTaken(maxTimeTaken);
            this.avgTimeTaken(avgTimeTaken);
            assert requests().length == maxTimeTaken.length;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            final int size = in.readVInt();
            maxTimeTaken(new long[size]);
            avgTimeTaken(new long[size]);
            requests(new SearchRequest[size]);
            for (int i = 0; i < maxTimeTaken.length; i++) {
                maxTimeTaken[i] = in.readVLong();
                avgTimeTaken[i] = in.readVLong();
                requests()[i] = new SearchRequest();
                requests()[i].readFrom(in);
            }

        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(maxTimeTaken.length);
            for (int i = 0; i < maxTimeTaken.length; i++) {
                out.writeVLong(maxTimeTaken[i]);
                out.writeVLong(avgTimeTaken[i]);
                requests()[i].writeTo(out);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startArray();
            for (int i = 0; i < maxTimeTaken.length; i++) {
                builder.startObject();
                builder.field("max_time", maxTimeTaken[i]);
                builder.field("avg_time", avgTimeTaken[i]);
                XContentHelper.writeRawField("request", requests()[i].source(), builder, params);
                builder.endObject();
            }
            builder.endArray();
            return builder;
        }

        public long[] maxTimeTaken() {
            return maxTimeTaken;
        }

        public long[] avgTimeTaken() {
            return avgTimeTaken;
        }

        public void maxTimeTaken(long[] timeTaken) {
            this.maxTimeTaken = timeTaken;
        }
        public void avgTimeTaken(long[] timeTaken) {
            this.avgTimeTaken = timeTaken;
        }

        public SearchRequest[] requests() {
            return requests;
        }

        public void requests(SearchRequest[] requests) {
            this.requests = requests;
        }
    }

}
