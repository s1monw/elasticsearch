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

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A Benchmark Request
 */
public class BenchRequest extends MasterNodeOperationRequest<BenchRequest> {

    private String[] indices = Strings.EMPTY_ARRAY;
    private String benchmarkId;

    private ClearIndicesCacheRequest clearCaches;
    private int numExecutors = 1;
    private List<BenchSpec> competitors = new ArrayList<BenchSpec>();
    private List<SearchRequest> requests = new ArrayList<SearchRequest>();

    BenchRequest(String[] indices) {
        this.indices = indices == null ? Strings.EMPTY_ARRAY : indices;
    }

    BenchRequest() {
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (benchmarkId == null) {
            validationException = ValidateActions.addValidationError("benchmarkId must not be null", validationException);
        }
        if (competitors.isEmpty()) {
            validationException = ValidateActions.addValidationError("competitors must not be empty", validationException);
        }
        if (numExecutors <= 0) {
            validationException = ValidateActions.addValidationError("num_executors must not be less than 1", validationException);
        }
        for (BenchSpec competitor : competitors) {
            validationException = competitor.validate(validationException);
            if (validationException != null) {
                break;
            }
        }
        return validationException;
    }

    public int numExecutors() {
        return numExecutors;
    }

    public void numExecutors(int numExecutors) {
        this.numExecutors = numExecutors;
    }

    public String benchmarkId() {
        return benchmarkId;
    }

    public void benchmarkId(String benchmarkId) {
        this.benchmarkId = benchmarkId;
    }


    public String[] indices() {
        return indices;
    }

    public void clearCaches(ClearIndicesCacheRequest clearCaches) {
        this.clearCaches = clearCaches;
    }

    public ClearIndicesCacheRequest clearCaches() {
        return clearCaches;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        this.benchmarkId = in.readString();
        this.indices = in.readStringArray();
        this.numExecutors = in.readVInt();
        clearCaches = in.readOptionalStreamable(new ClearIndicesCacheRequest());
        {
            final int size = in.readVInt();
            requests.clear();
            for (int i = 0; i < size; i++) {
                SearchRequest searchRequest = new SearchRequest();
                searchRequest.readFrom(in);
                requests.add(searchRequest);
            }
        }
        {
            final int size = in.readVInt();
            competitors.clear();
            for (int i = 0; i < size; i++) {
                BenchSpec competitor = new BenchSpec();
                competitor.readFrom(in);
                competitors.add(competitor);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(benchmarkId);
        out.writeStringArray(indices);
        out.writeVInt(numExecutors);
        out.writeOptionalStreamable(clearCaches);
        out.writeVInt(requests.size());
        for (SearchRequest request : requests) {
            if (request.indices() == null) {   // unless it's set we override it
                request.indices(this.indices);
            }
            request.writeTo(out);
        }

        out.writeVInt(competitors.size());
        for (BenchSpec competitor : competitors) {
            competitor.writeTo(out);
        }
    }

    public List<BenchSpec> competiors() {
        return competitors;
    }

    public void addCompetitor(BenchSpec competitor) {
        this.competitors.add(competitor);
    }

    public void addSearchRequest(SearchRequest... searchRequest) {
        requests.addAll(Arrays.asList(searchRequest));
    }

    public List<SearchRequest> searchRequests() {
        return requests;
    }
}
