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

package org.elasticsearch.action.search;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.transport.Transport;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Function;

final class SearchQueryThenFetchAsyncAction
        extends AbstractSearchAsyncAction<SearchPhaseResult> {

    private final SearchPhaseController searchPhaseController;

    SearchQueryThenFetchAsyncAction(
            final Logger logger,
            final SearchTransportService searchTransportService,
            final Function<String, Transport.Connection> nodeIdToConnection,
            final Map<String, AliasFilter> aliasFilter,
            final Map<String, Float> concreteIndexBoosts,
            final SearchPhaseController searchPhaseController,
            final Executor executor,
            final SearchRequest request,
            final ActionListener<SearchResponse> listener,
            final GroupShardsIterator shardsIts,
            final SearchTimeProvider timeProvider,
            long clusterStateVersion,
            SearchTask task) {
        super(
                "query",
                logger,
                searchTransportService,
                nodeIdToConnection,
                aliasFilter,
                concreteIndexBoosts,
                executor,
                request,
                listener,
                shardsIts,
                timeProvider,
                clusterStateVersion,
                task,
                searchPhaseController.newSearchPhaseResults(request, shardsIts.size()));
        this.searchPhaseController = searchPhaseController;
    }

    protected void executePhaseOnShard(
            final ShardIterator shardIt,
            final ShardRouting shard,
            final SearchActionListener<SearchPhaseResult> listener) {
        getSearchTransport().sendExecuteQuery(
                getConnection(shard.currentNodeId()),
                buildShardSearchRequest(shardIt, shard),
                getTask(),
                listener);
    }

    @Override
    protected SearchPhase getNextPhase(
            final SearchPhaseResults<SearchPhaseResult> results,
            final SearchPhaseContext context) {
        return new FetchSearchPhase(results, searchPhaseController, context);
    }
}
