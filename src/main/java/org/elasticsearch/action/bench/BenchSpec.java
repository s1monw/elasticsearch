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

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 */
public class BenchSpec implements Streamable {
    private SearchType searchType = SearchType.DEFAULT;
    private boolean warmup = true;
    private int concurrency = 1;
    private int iterations = 1;
    private int multiplier = 1;
    private int numSlowest = 0;
    private String[] indices;
    private ClearIndicesCacheRequest clearCaches;
    private List<SearchRequest> requests = new ArrayList<SearchRequest>();
    private SearchRequest defaultSearchRequest;
    private String name;

    BenchSpec(String... indices) {
        indices(indices);
    }

    BenchSpec() {
    }

    ActionRequestValidationException validate(ActionRequestValidationException validationException) {
        if (name == null) {
            validationException = ValidateActions.addValidationError("name must not be null", validationException);
        }
        if (concurrency < 1) {
            validationException = ValidateActions.addValidationError("concurrent requests must be >= 1 but was [" + concurrency + "]", validationException);
        }
        if (iterations < 1) {
            validationException = ValidateActions.addValidationError("iterations must be >= 1 but was [" + iterations + "]", validationException);
        }
        if (numSlowest < 0) {
            validationException = ValidateActions.addValidationError("numSlowest must be >= 0 but was [" + numSlowest + "]", validationException);
        }
        if (searchType == null) {
            validationException = ValidateActions.addValidationError("searchType must not be null", validationException);
        }
        return validationException;
    }

    public String name() {
        return name;
    }

    public void name(String name) {
        this.name = name;
    }


    public boolean warmup() {
        return warmup;
    }

    public void warmup(boolean warmup) {
        this.warmup = warmup;
    }

    public void concurrency(int concurrency) {
        this.concurrency = concurrency;
    }

    public int concurrency() {
        return concurrency;
    }

    public void iterations(int iterations) {
        this.iterations = iterations;
    }

    public int iterations() {
        return iterations;
    }

    public void multiplier(int multiplier) {
        this.multiplier = multiplier;
    }

    public int multiplier() {
        return multiplier;
    }

    public void numSlowest(int numSlowest) {
        this.numSlowest = numSlowest;
    }

    public int numSlowest() {
        return numSlowest;
    }

    public String[] indices() {
        return this.indices;
    }

    public void indices(String... indices) {
        this.indices = indices == null ? Strings.EMPTY_ARRAY : indices;
    }

    public void searchType(SearchType searchType) {
        this.searchType = searchType;
    }

    public SearchType searchType() {
        return searchType;
    }

    public void clearCaches(ClearIndicesCacheRequest clearCaches) {
        this.clearCaches = clearCaches;
    }

    public ClearIndicesCacheRequest clearCaches() {
        return clearCaches;
    }

    public SearchRequest defaultSearchRequest() {
        return defaultSearchRequest;
    }

    public void defaultSearchRequest(SearchRequest defaultSearchRequest) {
        this.defaultSearchRequest = defaultSearchRequest;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        this.indices = in.readStringArray();
        this.name = in.readString();
        this.concurrency = in.readVInt();
        this.iterations = in.readVInt();
        this.multiplier = in.readVInt();
        this.numSlowest = in.readVInt();
        this.warmup = in.readBoolean();
        this.searchType = SearchType.fromId(in.readByte());
        clearCaches = in.readOptionalStreamable(new ClearIndicesCacheRequest());
        defaultSearchRequest = in.readOptionalStreamable(new SearchRequest());
        final int size = in.readVInt();
        requests.clear();
        for (int i = 0; i < size; i++) {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.readFrom(in);
            requests.add(searchRequest);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(indices);
        out.writeString(name);
        out.writeVInt(concurrency);
        out.writeVInt(iterations);
        out.writeVInt(multiplier);
        out.writeVInt(numSlowest);
        out.writeBoolean(warmup);
        out.writeByte(searchType.id());
        out.writeOptionalStreamable(clearCaches);
        out.writeOptionalStreamable(defaultSearchRequest);
        out.writeVInt(requests.size());
        for (SearchRequest request : requests) {
            if (request.indices() == null) {   // unless it's set we override it
                request.indices(this.indices);
            }
            request.writeTo(out);
        }
    }

    public void addSearchRequest(SearchRequest... searchRequest) {
        requests.addAll(Arrays.asList(searchRequest));
    }

    public List<SearchRequest> searchRequests() {
       return requests;
    }
}
