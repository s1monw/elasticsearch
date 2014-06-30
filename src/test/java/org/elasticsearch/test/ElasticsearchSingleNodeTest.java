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
package org.elasticsearch.test;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalNode;
import org.junit.After;

/**
 */
public class ElasticsearchSingleNodeTest extends ElasticsearchTestCase {

    private static Node node = node();

    @After
    public void after() {
        node.client().admin().indices().prepareDelete("*").get();
    }


    public static Node node() {
        return node(ImmutableSettings.EMPTY);
    }

    public static Node node(Settings settings) {
        return NodeBuilder.nodeBuilder().local(true).data(true).settings(ImmutableSettings.builder().put(settings).put("http.enabled", false)
                .put("index.store.type", "ram")
                .put("gateway.type", "none")).build().start();
    }

    public synchronized <T> T getInstanceFromNode(Class<T> clazz, Node node) {
        return ((InternalNode)node).injector().getInstance(clazz);
    }
    public IndexService createIndex(String index) {
        return createIndex(index, ImmutableSettings.EMPTY);
    }

    public IndexService createIndex(String index, Settings settings) {
        node.client().admin().indices().prepareCreate(index).setSettings(settings).get();
        IndicesService instanceFromNode = getInstanceFromNode(IndicesService.class, node);
        return instanceFromNode.indexService(index);
    }
}
