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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.internal.InternalClient;

/**
 */
public class BenchRequestBuilder extends ActionRequestBuilder<BenchRequest, BenchResponse, BenchRequestBuilder> {

    public BenchRequestBuilder(Client client, String[] indices) {
        super((InternalClient) client, new BenchRequest(indices));
    }

    public BenchRequestBuilder setClearCaches(ClearIndicesCacheRequest clearCaches) {
        request.clearCaches(clearCaches);
        return this;
    }

    public BenchRequestBuilder addSearchRequest(SearchRequest... searchRequest) {
        request.addSearchRequest(searchRequest);
        return this;
    }

    public BenchRequestBuilder addCompetitor(BenchSpec competitor) {
        request.addCompetitor(competitor);
        return this;
    }

    public BenchRequestBuilder addCompetitor(BenchSpecBuilder competitorBuilder) {
        return addCompetitor(competitorBuilder.build());
    }

    public BenchSpecBuilder competitorBuilder(String[] indices) {
        return BenchSpecBuilder.competitorBuilder(indices);
    }

    public BenchRequestBuilder setNumExecutors(int numExecutors) {
        request.numExecutors(numExecutors);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<BenchResponse> listener) {
        ((Client) client).bench(request, listener);
    }

    public BenchRequestBuilder setBenchmarkId(String benchmarkId) {
        request.benchmarkId(benchmarkId);
        return this;
    }
}
