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

import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;

/**
 */
public class BenchSpecBuilder {
    private final BenchSpec competitor;

    public BenchSpecBuilder(String... indices) {
        competitor = new BenchSpec(indices);
    }

    public BenchSpecBuilder setWarmup(boolean warmup) {
        competitor.warmup(warmup);
        return this;
    }

    public BenchSpecBuilder setIndices(String... indices) {
        competitor.indices(indices);
        return this;
    }

    public BenchSpecBuilder setClearCaches(ClearIndicesCacheRequest clearCaches) {
        competitor.clearCaches(clearCaches);
        return this;
    }

    /**
     */
    public BenchSpecBuilder setConcurrency(int concurrentRequests) {
        competitor.concurrency(concurrentRequests);
        return this;
    }

    public BenchSpecBuilder addSearchRequest(SearchRequest... searchRequest) {
        competitor.addSearchRequest(searchRequest);
        return this;
    }

    public BenchSpecBuilder defaultSearchReqeust(SearchRequest searchRequest) {
        competitor.defaultSearchRequest(searchRequest);
        return this;
    }

    /**
     */
    public BenchSpecBuilder setIterations(int iters) {
        competitor.iterations(iters);
        return this;
    }

    public BenchSpecBuilder setMultiplier(int multiplier) {
        competitor.multiplier(multiplier);
        return this;
    }

    public BenchSpecBuilder setNumSlowest(int numSlowest) {
        competitor.numSlowest(numSlowest);
        return this;
    }

    public BenchSpec build() {
        return competitor;
    }

    public static BenchSpecBuilder competitorBuilder(String... indices) {
        return new BenchSpecBuilder(indices);
    }

    public BenchSpecBuilder setSearchType(SearchType searchType) {
        competitor.searchType(searchType);
        return this;
    }

    public BenchSpecBuilder setName(String name) {
        competitor.name(name);
        return this;
    }
}
