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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static org.elasticsearch.action.search.SearchType.QUERY_THEN_FETCH;

public final class TransportSearchAction extends HandledTransportAction<SearchRequest, SearchResponse> {

    private final ActionRunnable<SearchRequest, SearchResponse> runnable;

    @Inject
    public TransportSearchAction(Settings settings, ThreadPool threadPool, TransportService transportService, SearchService searchService,
                                 SearchTransportService searchTransportService, SearchPhaseController searchPhaseController,
                                 ClusterService clusterService, ActionFilters actionFilters,
                                 IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, SearchAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, SearchRequest::new);
        this.runnable = SearchAction.INSTANCE.getActionRunnable(indexNameExpressionResolver, clusterService, searchTransportService,
            searchPhaseController, searchService);
    }

    @Override
    protected void doExecute(Task task, SearchRequest request, ActionListener<SearchResponse> listener) {
        runnable.execute(task, request, listener);
    }

    @Override
    protected void doExecute(SearchRequest request, ActionListener<SearchResponse> listener) {
        runnable.execute(request, listener);
    }
}
