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

package org.elasticsearch.test.integration.search.stats;

import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 */
public class SearchStatsTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        Settings settings = settingsBuilder().put("index.number_of_shards", 3).put("index.number_of_replicas", 0).build();
        startNode("server1", settings);
        startNode("server2", settings);
        client = getClient();
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("server1");
    }

    @Test
    public void testSimpleStats() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();

        for (int i = 0; i < 500; i++) {
            client.prepareIndex("test1", "type", Integer.toString(i)).setSource("field", "value").execute().actionGet();
            if (i == 10) {
                client.admin().indices().prepareRefresh("test1").execute().actionGet();
            }
        }
        for (int i = 0; i < 500; i++) {
            client.prepareIndex("test2", "type", Integer.toString(i)).setSource("field", "value").execute().actionGet();
            if (i == 10) {
                client.admin().indices().prepareRefresh("test1").execute().actionGet();
            }
        }
        for (int i = 0; i < 200; i++) {
            client.prepareSearch().setQuery(QueryBuilders.termQuery("field", "value")).setStats("group1", "group2").execute().actionGet();
        }

        IndicesStatsResponse indicesStats = client.admin().indices().prepareStats().execute().actionGet();
        assertThat(indicesStats.getTotal().getSearch().getTotal().getQueryCount(), greaterThan(0l));
        assertThat(indicesStats.getTotal().getSearch().getTotal().getQueryTimeInMillis(), greaterThan(0l));
        assertThat(indicesStats.getTotal().getSearch().getTotal().getFetchCount(), greaterThan(0l));
        assertThat(indicesStats.getTotal().getSearch().getTotal().getFetchTimeInMillis(), greaterThan(0l));
        assertThat(indicesStats.getTotal().getSearch().getGroupStats(), nullValue());

        indicesStats = client.admin().indices().prepareStats().setGroups("group1").execute().actionGet();
        assertThat(indicesStats.getTotal().getSearch().getGroupStats(), notNullValue());
        assertThat(indicesStats.getTotal().getSearch().getGroupStats().get("group1").getQueryCount(), greaterThan(0l));
        assertThat(indicesStats.getTotal().getSearch().getGroupStats().get("group1").getQueryTimeInMillis(), greaterThan(0l));
        assertThat(indicesStats.getTotal().getSearch().getGroupStats().get("group1").getFetchCount(), greaterThan(0l));
        assertThat(indicesStats.getTotal().getSearch().getGroupStats().get("group1").getFetchTimeInMillis(), greaterThan(0l));

        NodesStatsResponse nodeStats = client.admin().cluster().prepareNodesStats().execute().actionGet();
        assertThat(nodeStats.getNodes()[0].getIndices().getSearch().getTotal().getQueryCount(), greaterThan(0l));
        assertThat(nodeStats.getNodes()[0].getIndices().getSearch().getTotal().getQueryTimeInMillis(), greaterThan(0l));
    }
}
