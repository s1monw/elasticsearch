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

package org.elasticsearch.cluster.routing.allocation.decider;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodeFilters;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

import static org.elasticsearch.cluster.node.DiscoveryNodeFilters.OpType.*;

/**
 * This {@link AllocationDecider} control shard allocation by include and
 * exclude filters via dynamic cluster and index routing settings.
 * <p>
 * This filter is used to make explicit decision on which nodes certain shard
 * can / should be allocated. The decision if a shard can be allocated, must not
 * be allocated or should be allocated is based on either cluster wide dynamic
 * settings (<tt>cluster.routing.allocation.*</tt>) or index specific dynamic
 * settings (<tt>index.routing.allocation.*</tt>). All of those settings can be
 * changed at runtime via the cluster or the index update settings API.
 * </p>
 * Note: Cluster settings are applied first and will override index specific
 * settings such that if a shard can be allocated according to the index routing
 * settings it wont be allocated on a node if the cluster specific settings
 * would disallow the allocation. Filters are applied in the following order:
 * <ol>
 * <li><tt>required</tt> - filters required allocations. 
 * If any <tt>required</tt> filters are set the allocation is denied if the index is <b>not</b> in the set of <tt>required</tt> to allocate on the filtered node</li>
 * 
 * <li><tt>include</tt> - filters "allowed" allocations. 
 * If any <tt>include</tt> filters are set the allocation is denied if the index is <b>not</b> in the set of <tt>include</tt> filters for the filtered node</li>
 * 
 * <li><tt>exclude</tt> - filters "prohibited" allocations. 
 * If any <tt>exclude</tt> filters are set the allocation is denied if the index is in the set of <tt>exclude</tt> filters for the filtered node</li>
 * </ol>
 * 
 * 
 */
public class FilterAllocationDecider extends AllocationDecider {

    static {
        MetaData.addDynamicSettings(
                "cluster.routing.allocation.require.*",
                "cluster.routing.allocation.include.*",
                "cluster.routing.allocation.exclude.*"
        );
        IndexMetaData.addDynamicSettings(
                "index.routing.allocation.require.*",
                "index.routing.allocation.include.*",
                "index.routing.allocation.exclude.*"
        );
    }

    private volatile DiscoveryNodeFilters clusterRequireFilters;
    private volatile DiscoveryNodeFilters clusterIncludeFilters;
    private volatile DiscoveryNodeFilters clusterExcludeFilters;

    @Inject
    public FilterAllocationDecider(Settings settings, NodeSettingsService nodeSettingsService) {
        super(settings);
        ImmutableMap<String, String> requireMap = settings.getByPrefix("cluster.routing.allocation.require.").getAsMap();
        if (requireMap.isEmpty()) {
            clusterRequireFilters = null;
        } else {
            clusterRequireFilters = DiscoveryNodeFilters.buildFromKeyValue(AND, requireMap);
        }
        ImmutableMap<String, String> includeMap = settings.getByPrefix("cluster.routing.allocation.include.").getAsMap();
        if (includeMap.isEmpty()) {
            clusterIncludeFilters = null;
        } else {
            clusterIncludeFilters = DiscoveryNodeFilters.buildFromKeyValue(OR, includeMap);
        }
        ImmutableMap<String, String> excludeMap = settings.getByPrefix("cluster.routing.allocation.exclude.").getAsMap();
        if (excludeMap.isEmpty()) {
            clusterExcludeFilters = null;
        } else {
            clusterExcludeFilters = DiscoveryNodeFilters.buildFromKeyValue(OR, excludeMap);
        }
        nodeSettingsService.addListener(new ApplySettings());
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation, boolean explain) {
        return shouldFilter(shardRouting, node, allocation, explain);
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation, boolean explain) {
        return shouldFilter(shardRouting, node, allocation, explain);
    }

    private Decision shouldFilter(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation, boolean explain) {
        if (clusterRequireFilters != null) {
            if (!clusterRequireFilters.match(node.node())) {
                return decision(Decision.Type.NO, "[FilterAllocation] Node is not required", explain);
            }
        }
        if (clusterIncludeFilters != null) {
            if (!clusterIncludeFilters.match(node.node())) {
                return decision(Decision.Type.NO, "[FilterAllocation] Node is not included", explain);
            }
        }
        if (clusterExcludeFilters != null) {
            if (clusterExcludeFilters.match(node.node())) {
                return decision(Decision.Type.NO, "[FilterAllocation] Node is excluded", explain);
            }
        }

        IndexMetaData indexMd = allocation.routingNodes().metaData().index(shardRouting.index());
        if (indexMd.requireFilters() != null) {
            if (!indexMd.requireFilters().match(node.node())) {
                return decision(Decision.Type.NO, "[FilterAllocation] Node is not required for the current index", explain);
            }
        }
        if (indexMd.includeFilters() != null) {
            if (!indexMd.includeFilters().match(node.node())) {
                return decision(Decision.Type.NO, "[FilterAllocation] Node is not included for the current index", explain);
            }
        }
        if (indexMd.excludeFilters() != null) {
            if (indexMd.excludeFilters().match(node.node())) {
                return decision(Decision.Type.NO, "[FilterAllocation] Node is excluded for the current index", explain);
            }
        }

        return decision(Decision.Type.YES, "[FilterAllocation] Allocation is not filtered", explain);
    }

    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            ImmutableMap<String, String> requireMap = settings.getByPrefix("cluster.routing.allocation.require.").getAsMap();
            if (!requireMap.isEmpty()) {
                clusterRequireFilters = DiscoveryNodeFilters.buildFromKeyValue(AND, requireMap);
            }
            ImmutableMap<String, String> includeMap = settings.getByPrefix("cluster.routing.allocation.include.").getAsMap();
            if (!includeMap.isEmpty()) {
                clusterIncludeFilters = DiscoveryNodeFilters.buildFromKeyValue(OR, includeMap);
            }
            ImmutableMap<String, String> excludeMap = settings.getByPrefix("cluster.routing.allocation.exclude.").getAsMap();
            if (!excludeMap.isEmpty()) {
                clusterExcludeFilters = DiscoveryNodeFilters.buildFromKeyValue(OR, excludeMap);
            }
        }
    }
}
