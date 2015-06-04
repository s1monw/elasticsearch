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

package org.elasticsearch.index.analysis;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.hamcrest.CoreMatchers.*;

@ElasticsearchIntegrationTest.ClusterScope(numDataNodes = 1, scope = ElasticsearchIntegrationTest.Scope.SUITE)
public class SimpleSmartChineseIntegrationTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("path.home", createTempDir())
                .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true)
                .build();
    }

    @Test
    public void testSmartcnAnalyzer() throws ExecutionException, InterruptedException {
        AnalyzeResponse response = client().admin().indices()
                .prepareAnalyze("叻出色").setAnalyzer("smartcn")
                .execute().get();

        assertThat(response, notNullValue());
        assertThat(response.getTokens().size(), is(2));
    }

    @Test
    public void testSmartcnTokenizer() throws ExecutionException, InterruptedException {
        AnalyzeResponse response = client().admin().indices()
                .prepareAnalyze("叻出色").setTokenizer("smartcn_tokenizer")
                .execute().get();

        assertThat(response, notNullValue());
        assertThat(response.getTokens().size(), is(2));
    }
}
