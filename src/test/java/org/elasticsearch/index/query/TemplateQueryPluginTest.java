/**
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
package org.elasticsearch.index.query;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Full integration test of the template query plugin.
 * */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE)
public class TemplateQueryPluginTest extends ElasticsearchIntegrationTest {

    @Test
    public void testTemplateInBody() {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "testtype").setId("1")
                .setSource("text", "value1").get();
        client().prepareIndex("test", "testtype").setId("2")
                .setSource("text", "value2").get();
        refresh();

        Map<String, Object> vars = new HashMap<String, Object>();
        vars.put("template", "all");

        TemplateQueryBuilder builder = new TemplateQueryBuilder(
                "{\"match_{{template}}\": {}}\"", vars);
        SearchResponse sr = client().prepareSearch().setQuery(builder)
                .execute().actionGet();
        assertEquals("Template query didn't return correct number of hits.", 2,
                sr.getHits().totalHits());
    }

    @Test
    public void testTemplateInFile() {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "testtype").setId("1")
                .setSource("text", "value1").get();
        client().prepareIndex("test", "testtype").setId("2")
                .setSource("text", "value2").get();
        refresh();

        Map<String, Object> vars = new HashMap<String, Object>();
        vars.put("template", "all");
        TemplateQueryBuilder builder = new TemplateQueryBuilder(
                "storedTemplate", vars);
        SearchResponse sr = client().prepareSearch().setQuery(builder)
                .execute().actionGet();
        assertEquals("Template query didn't return correct number of hits.", 2,
                sr.getHits().totalHits());

    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        String scriptPath = this.getClass()
                .getResource("config").getPath();

        Settings settings = ImmutableSettings
                .settingsBuilder()
                .put("path.conf", scriptPath).build();

        return settings;
    }
}
