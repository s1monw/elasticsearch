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
package org.elasticsearch.index.analysis;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.analysis.PreBuiltAnalyzers;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class PreBuiltAnalyzerIntegrationTests extends ElasticsearchIntegrationTest {

    @Test
    public void testThatPreBuiltAnalyzersAreNotClosedOnIndexClose() throws Exception {
        Map<PreBuiltAnalyzers, List<Version>> loadedAnalyzers = Maps.newHashMap();

        List<String> indexNames = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            String indexName = randomAsciiOfLength(10).toLowerCase(Locale.ROOT);
            indexNames.add(indexName);

            int randomInt = randomInt(PreBuiltAnalyzers.values().length-1);
            PreBuiltAnalyzers preBuiltAnalyzer = PreBuiltAnalyzers.values()[randomInt];
            String name = preBuiltAnalyzer.name().toLowerCase(Locale.ROOT);

            Version randomVersion = randomVersion();
            if (!loadedAnalyzers.containsKey(preBuiltAnalyzer)) {
                 loadedAnalyzers.put(preBuiltAnalyzer, Lists.<Version>newArrayList());
            }
            loadedAnalyzers.get(preBuiltAnalyzer).add(randomVersion);

            final XContentBuilder mapping = jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("foo")
                            .field("type", "string")
                            .field("analyzer", name)
                        .endObject()
                    .endObject()
                .endObject()
                .endObject();

            Settings versionSettings = ImmutableSettings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, randomVersion).build();
            client().admin().indices().prepareCreate(indexName).addMapping("type", mapping).setSettings(versionSettings).get();
        }

        ensureGreen();

        // index some amount of data
        for (int i = 0; i < 100; i++) {
            String randomIndex = indexNames.get(randomInt(indexNames.size()-1));
            String randomId = randomInt() + "";

            Map<String, Object> data = Maps.newHashMap();
            data.put("foo", randomAsciiOfLength(50));

            index(randomIndex, "type", randomId, data);
        }

        refresh();

        // close some of the indices
        int amountOfIndicesToClose = randomInt(10-1);
        for (int i = 0; i < amountOfIndicesToClose; i++) {
            String indexName = indexNames.get(i);
            client().admin().indices().prepareClose(indexName).execute().actionGet();
        }

        ensureGreen();

        // check that all above configured analyzers have been loaded
        assertThatAnalyzersHaveBeenLoaded(loadedAnalyzers);

        // check that all of the prebuiltanalyzers are still open
        for (PreBuiltAnalyzers preBuiltAnalyzer : PreBuiltAnalyzers.values()) {
            assertLuceneAnalyzerIsNotClosed(preBuiltAnalyzer);
        }
    }

    private void assertThatAnalyzersHaveBeenLoaded(Map<PreBuiltAnalyzers, List<Version>> expectedLoadedAnalyzers) {
        for (Map.Entry<PreBuiltAnalyzers, List<Version>> entry : expectedLoadedAnalyzers.entrySet()) {
            Map<Version, Analyzer> cachedAnalyzers = entry.getKey().getCachedAnalyzers();
            assertThat(cachedAnalyzers.keySet(), hasItems(entry.getValue().toArray(new Version[]{})));
            /*for (Version expectedVersion : entry.getValue()) {
                assertThat(cachedAnalyzers, contains(ex))
            }
            */
        }
    }

    // the close() method of a lucene analyzer sets the storedValue field to null
    // we simply check this via reflection - ugly but works
    private void assertLuceneAnalyzerIsNotClosed(PreBuiltAnalyzers preBuiltAnalyzer) throws IllegalAccessException, NoSuchFieldException {

        for (Map.Entry<Version, Analyzer> luceneAnalyzerEntry : preBuiltAnalyzer.getCachedAnalyzers().entrySet()) {
            Field field = getFieldFromClass("storedValue", luceneAnalyzerEntry.getValue());
            boolean currentAccessible = field.isAccessible();
            field.setAccessible(true);
            Object storedValue = field.get(preBuiltAnalyzer.getAnalyzer(luceneAnalyzerEntry.getKey()));
            field.setAccessible(currentAccessible);

            assertThat(String.format(Locale.ROOT, "Analyzer %s in version %s seems to be closed", preBuiltAnalyzer.name(), luceneAnalyzerEntry.getKey()), storedValue, is(notNullValue()));
        }

    }

    /**
     * Searches for a field until it finds, loops through all superclasses
     */
    private Field getFieldFromClass(String fieldName, Object obj) {
        Field field = null;
        boolean storedValueFieldFound = false;
        Class clazz = obj.getClass();
        while (!storedValueFieldFound) {
            try {
                field = clazz.getDeclaredField(fieldName);
                storedValueFieldFound = true;
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }

            if (Object.class.equals(clazz)) throw new RuntimeException("Could not find storedValue field in class" + clazz);
        }

        return field;
    }

}
