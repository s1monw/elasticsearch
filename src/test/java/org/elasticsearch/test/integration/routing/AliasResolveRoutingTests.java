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

package org.elasticsearch.test.integration.routing;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static org.elasticsearch.cluster.metadata.AliasAction.newAddAliasAction;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
public class AliasResolveRoutingTests extends AbstractNodesTests {
    private Client client;

    private ClusterService clusterService;

    @BeforeClass
    public void createNodes() throws Exception {
        startNode("node1");
        startNode("node2");
        client = getClient();
        clusterService = ((InternalNode) node("node1")).injector().getInstance(ClusterService.class);
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("node1");
    }

    @Test
    public void testResolveIndexRouting() throws Exception {
        try {
            client.admin().indices().prepareDelete("test1").execute().actionGet();
            client.admin().indices().prepareDelete("test2").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test1").execute().actionGet();
        client.admin().indices().prepareCreate("test2").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client.admin().indices().prepareAliases().addAliasAction(newAddAliasAction("test1", "alias")).execute().actionGet();
        client.admin().indices().prepareAliases().addAliasAction(newAddAliasAction("test1", "alias10").routing("0")).execute().actionGet();
        client.admin().indices().prepareAliases().addAliasAction(newAddAliasAction("test1", "alias110").searchRouting("1,0")).execute().actionGet();
        client.admin().indices().prepareAliases().addAliasAction(newAddAliasAction("test1", "alias12").routing("2")).execute().actionGet();
        client.admin().indices().prepareAliases().addAliasAction(newAddAliasAction("test2", "alias20").routing("0")).execute().actionGet();
        client.admin().indices().prepareAliases().addAliasAction(newAddAliasAction("test2", "alias21").routing("1")).execute().actionGet();
        client.admin().indices().prepareAliases().addAliasAction(newAddAliasAction("test1", "alias0").routing("0")).execute().actionGet();
        client.admin().indices().prepareAliases().addAliasAction(newAddAliasAction("test2", "alias0").routing("0")).execute().actionGet();

        assertThat(clusterService.state().metaData().resolveIndexRouting(null, "test1"), nullValue());
        assertThat(clusterService.state().metaData().resolveIndexRouting(null, "alias"), nullValue());

        assertThat(clusterService.state().metaData().resolveIndexRouting(null, "test1"), nullValue());
        assertThat(clusterService.state().metaData().resolveIndexRouting(null, "alias10"), equalTo("0"));
        assertThat(clusterService.state().metaData().resolveIndexRouting(null, "alias20"), equalTo("0"));
        assertThat(clusterService.state().metaData().resolveIndexRouting(null, "alias21"), equalTo("1"));
        assertThat(clusterService.state().metaData().resolveIndexRouting("3", "test1"), equalTo("3"));
        assertThat(clusterService.state().metaData().resolveIndexRouting("0", "alias10"), equalTo("0"));
        try {
            clusterService.state().metaData().resolveIndexRouting("1", "alias10");
            assert false : "should fail";
        } catch (ElasticSearchIllegalArgumentException e) {
            // all is well, we can't have two mappings, one provided, and one in the alias
        }

        try {
            clusterService.state().metaData().resolveIndexRouting(null, "alias0");
            assert false : "should fail";
        } catch (ElasticSearchIllegalArgumentException ex) {
            // Expected
        }
    }


    @Test
    public void testResolveSearchRouting() throws Exception {
        try {
            client.admin().indices().prepareDelete("test1").execute().actionGet();
            client.admin().indices().prepareDelete("test2").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test1").execute().actionGet();
        client.admin().indices().prepareCreate("test2").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client.admin().indices().prepareAliases().addAliasAction(newAddAliasAction("test1", "alias")).execute().actionGet();
        client.admin().indices().prepareAliases().addAliasAction(newAddAliasAction("test1", "alias10").routing("0")).execute().actionGet();
        client.admin().indices().prepareAliases().addAliasAction(newAddAliasAction("test2", "alias20").routing("0")).execute().actionGet();
        client.admin().indices().prepareAliases().addAliasAction(newAddAliasAction("test2", "alias21").routing("1")).execute().actionGet();
        client.admin().indices().prepareAliases().addAliasAction(newAddAliasAction("test1", "alias0").routing("0")).execute().actionGet();
        client.admin().indices().prepareAliases().addAliasAction(newAddAliasAction("test2", "alias0").routing("0")).execute().actionGet();

        assertThat(clusterService.state().metaData().resolveSearchRouting(null, "alias"), nullValue());
        assertThat(clusterService.state().metaData().resolveSearchRouting("0,1", "alias"), equalTo(newMap("test1", newSet("0", "1"))));
        assertThat(clusterService.state().metaData().resolveSearchRouting(null, "alias10"), equalTo(newMap("test1", newSet("0"))));
        assertThat(clusterService.state().metaData().resolveSearchRouting(null, "alias10"), equalTo(newMap("test1", newSet("0"))));
        assertThat(clusterService.state().metaData().resolveSearchRouting("0", "alias10"), equalTo(newMap("test1", newSet("0"))));
        assertThat(clusterService.state().metaData().resolveSearchRouting("1", "alias10"), nullValue());
        assertThat(clusterService.state().metaData().resolveSearchRouting(null, "alias0"), equalTo(newMap("test1", newSet("0"), "test2", newSet("0"))));

        assertThat(clusterService.state().metaData().resolveSearchRouting(null, new String[]{"alias10", "alias20"}),
                equalTo(newMap("test1", newSet("0"), "test2", newSet("0"))));
        assertThat(clusterService.state().metaData().resolveSearchRouting(null, new String[]{"alias10", "alias21"}),
                equalTo(newMap("test1", newSet("0"), "test2", newSet("1"))));
        assertThat(clusterService.state().metaData().resolveSearchRouting(null, new String[]{"alias20", "alias21"}),
                equalTo(newMap("test2", newSet("0", "1"))));
        assertThat(clusterService.state().metaData().resolveSearchRouting(null, new String[]{"test1", "alias10"}), nullValue());
        assertThat(clusterService.state().metaData().resolveSearchRouting(null, new String[]{"alias10", "test1"}), nullValue());


        assertThat(clusterService.state().metaData().resolveSearchRouting("0", new String[]{"alias10", "alias20"}),
                equalTo(newMap("test1", newSet("0"), "test2", newSet("0"))));
        assertThat(clusterService.state().metaData().resolveSearchRouting("0,1", new String[]{"alias10", "alias20"}),
                equalTo(newMap("test1", newSet("0"), "test2", newSet("0"))));
        assertThat(clusterService.state().metaData().resolveSearchRouting("1", new String[]{"alias10", "alias20"}), nullValue());
        assertThat(clusterService.state().metaData().resolveSearchRouting("0", new String[]{"alias10", "alias21"}),
                equalTo(newMap("test1", newSet("0"))));
        assertThat(clusterService.state().metaData().resolveSearchRouting("1", new String[]{"alias10", "alias21"}),
                equalTo(newMap("test2", newSet("1"))));
        assertThat(clusterService.state().metaData().resolveSearchRouting("0,1,2", new String[]{"alias10", "alias21"}),
                equalTo(newMap("test1", newSet("0"), "test2", newSet("1"))));
        assertThat(clusterService.state().metaData().resolveSearchRouting("0,1,2", new String[]{"test1", "alias10", "alias21"}),
                equalTo(newMap("test1", newSet("0", "1", "2"), "test2", newSet("1"))));
    }

    private <T> Set<T> newSet(T... elements) {
        return newHashSet(elements);
    }


    private <K, V> Map<K, V> newMap(K key, V value) {
        Map<K, V> r = newHashMap();
        r.put(key, value);
        return r;
    }

    private <K, V> Map<K, V> newMap(K key1, V value1, K key2, V value2) {
        Map<K, V> r = newHashMap();
        r.put(key1, value1);
        r.put(key2, value2);
        return r;
    }

}
