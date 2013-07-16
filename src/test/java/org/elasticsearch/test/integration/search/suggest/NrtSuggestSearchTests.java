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
package org.elasticsearch.test.integration.search.suggest;

import com.google.common.collect.Lists;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.nrt.NrtSuggestionBuilder;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class NrtSuggestSearchTests extends AbstractNodesTests {

    private static String INDEX = "test";
    public static String TYPE = "testType";
    public static String FIELD = "testField";
    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        startNode("server1");
        // be quicker for now
        //startNode("server2");
        client = client("server1");
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }
    
    @Test
    public void testSimple() throws Exception{
        createIndexAndMapping();
        String[][] input = {{"Foo Fighters"}, {"Foo Fighters"}, {"Foo Fighters"}, {"Foo Fighters"},
                            {"Generator", "Foo Fighters Generator"}, {"Learn to Fly", "Foo Fighters Learn to Fly" }, 
                            {"The Prodigy"}, {"The Prodigy"}, {"The Prodigy"}, {"Firestarter", "The Prodigy Firestarter"},
                            {"Turbonegro"}, {"Turbonegro"}, {"Get it on", "Turbonegro Get it on"}}; // work with frequencies
        for (int i = 0; i < input.length; i++) {
            client.prepareIndex(INDEX, TYPE, "" + i)
                    .setSource(jsonBuilder()
                            .startObject().startObject(FIELD)
                            .startArray("input").value(input[i]).endArray()
                            .endObject()
                            .endObject()
                    )
                    .setRefresh(true)
                    .execute().actionGet();
        }
        
        // make sure we have global frequencies
        client.admin().indices().prepareOptimize(INDEX).execute().actionGet();
        
        SuggestResponse suggestResponse = client.prepareSuggest(INDEX).addSuggestion(
                new NrtSuggestionBuilder("foo").field(FIELD).text("f").size(10)
        ).execute().actionGet();
        

        assertThat(suggestResponse.getSuggest().size(), is(1));
        Suggest.Suggestion<Suggest.Suggestion.Entry<Suggest.Suggestion.Entry.Option>> suggestion = suggestResponse.getSuggest().getSuggestion("foo");

        assertThat(suggestion.getEntries().size(), is(1));
        List<String> names = getNames(suggestion.getEntries().get(0));
        assertThat(names, hasSize(4));
        assertThat(names.get(0), is("Foo Fighters"));
        
        suggestResponse = client.prepareSuggest(INDEX).addSuggestion(
                new NrtSuggestionBuilder("t").field(FIELD).text("t").size(10)
        ).execute().actionGet();
        

        assertThat(suggestResponse.getSuggest().size(), is(1));
        suggestion = suggestResponse.getSuggest().getSuggestion("t");

        assertThat(suggestion.getEntries().size(), is(1));
        names = getNames(suggestion.getEntries().get(0));
        assertThat(names, hasSize(4));
        assertThat(names.get(0), is("The Prodigy"));
        assertThat(names.get(1), is("Turbonegro"));
    }


    @Test
    public void testBasicNRTSuggestion() throws Exception {
        // TODO test payloads - currently not enabled in this test
        createIndexAndMapping();
        for (int i = 0; i < 2; i++) {
            createData(i==0);    
            SuggestResponse suggestResponse = client.prepareSuggest(INDEX).addSuggestion(
                    new NrtSuggestionBuilder("foo").field(FIELD).text("f").size(10)
            ).execute().actionGet();
    
            assertThat(suggestResponse.getSuggest().size(), is(1));
            Suggest.Suggestion<Suggest.Suggestion.Entry<Suggest.Suggestion.Entry.Option>> suggestion = suggestResponse.getSuggest().getSuggestion("foo");
    
            assertThat(suggestion.getEntries().size(), is(1));
            List<String> names = getNames(suggestion.getEntries().get(0));
            assertThat(names, hasSize(4));
            assertThat(names.get(0), is("Firestarter - The Prodigy"));
            assertThat(names.get(1), is("Foo Fighters"));
            assertThat(names.get(2), is("Generator - Foo Fighters"));
            assertThat(names.get(3), is("Learn to Fly - Foo Fighters"));
            
            suggestResponse = client.prepareSuggest(INDEX).addSuggestion(
                    new NrtSuggestionBuilder("ge").field(FIELD).text("ge").size(10)
            ).execute().actionGet();
    
            assertThat(suggestResponse.getSuggest().size(), is(1));
            suggestion = suggestResponse.getSuggest().getSuggestion("ge");
    
            assertThat(suggestion.getEntries().size(), is(1));
            names = getNames(suggestion.getEntries().get(0));
            assertThat(names, hasSize(2));
            assertThat(names.get(0), is("Generator - Foo Fighters"));
            assertThat(names.get(1), is("Get it on - Turbonegro"));
            
            
            suggestResponse = client.prepareSuggest(INDEX).addSuggestion(
                    new NrtSuggestionBuilder("ge").field(FIELD).text("ge").size(10)
            ).execute().actionGet();
    
            assertThat(suggestResponse.getSuggest().size(), is(1));
            suggestion = suggestResponse.getSuggest().getSuggestion("ge");
    
            assertThat(suggestion.getEntries().size(), is(1));
            names = getNames(suggestion.getEntries().get(0));
            assertThat(names, hasSize(2));
            assertThat(names.get(0), is("Generator - Foo Fighters"));
            assertThat(names.get(1), is("Get it on - Turbonegro"));
            
            suggestResponse = client.prepareSuggest(INDEX).addSuggestion(
                    new NrtSuggestionBuilder("t").field(FIELD).text("t").size(10)
            ).execute().actionGet();
    
            assertThat(suggestResponse.getSuggest().size(), is(1));
            suggestion = suggestResponse.getSuggest().getSuggestion("t");
    
            assertThat(suggestion.getEntries().size(), is(1));
            names = getNames(suggestion.getEntries().get(0));
            assertThat(names, hasSize(4));
            assertThat(names.get(0), is("The Prodigy"));
            assertThat(names.get(1), is("Firestarter - The Prodigy"));
            assertThat(names.get(2), is("Get it on - Turbonegro"));
            assertThat(names.get(3), is("Turbonegro"));
        }
    }

    private List<String> getNames(Suggest.Suggestion.Entry<Suggest.Suggestion.Entry.Option> suggestEntry) {
        List<String> names = Lists.newArrayList();
        for (Suggest.Suggestion.Entry.Option entry : suggestEntry.getOptions()) {
            names.add(entry.getText().string());
        }
        return names;
    }

    private void createIndexAndMapping() throws IOException {
        client.admin().indices().prepareDelete().execute().actionGet();
        client.admin().indices().prepareCreate(INDEX)
                .setSettings(settingsBuilder()
                        .put(SETTING_NUMBER_OF_SHARDS, 1)
                        .put(SETTING_NUMBER_OF_REPLICAS, 0))
                .execute().actionGet();
        client.admin().indices().preparePutMapping(INDEX).setType(TYPE).setSource(jsonBuilder().startObject()
                .startObject(TYPE).startObject("properties")
                .startObject(FIELD)
                .field("type", "suggest")
                   .field("index_analyzer", "simple")
                .field("search_analyzer", "simple")
                .field("payloads", true)
                .endObject()
                .endObject().endObject()
                .endObject()).execute().actionGet();
        client.admin().cluster().prepareHealth(INDEX).setWaitForGreenStatus().execute().actionGet();
    }

    private void createData(boolean optimize) throws IOException {
        String[][] input = {{"Foo Fighters"}, {"Generator", "Foo Fighters Generator"}, {"Learn to Fly", "Foo Fighters Learn to Fly" }, {"The Prodigy"}, {"Firestarter", "The Prodigy Firestarter"}, {"Turbonegro"}, {"Get it on", "Turbonegro Get it on"}};
        String[] surface = {"Foo Fighters", "Generator - Foo Fighters", "Learn to Fly - Foo Fighters", "The Prodigy", "Firestarter - The Prodigy", "Turbonegro", "Get it on - Turbonegro"};
        int[] weight = {10, 9, 8, 12, 11, 6, 7};
        for (int i = 0; i < input.length; i++) {
            client.prepareIndex(INDEX, TYPE, "" + i)
                    .setSource(jsonBuilder()
                            .startObject().startObject(FIELD)
                            .startArray("input").value(input[i]).endArray()
                            .field("surface_form",surface[i])
                            .field("payload", "id: " + i)
                            .field("weight", weight[i])
                            .endObject()
                            .endObject()
                    )
                    .setRefresh(true)
                    .execute().actionGet();
        }
        
        for (int i = 0; i < weight.length; i++) { // add them again to make sure we deduplicate on the surface form
            client.prepareIndex(INDEX, TYPE, "n" + i)
                    .setSource(jsonBuilder()
                            .startObject().startObject(FIELD)
                            .startArray("input").value(input[i]).endArray()
                            .field("surface_form",surface[i])
                            .field("payload", "id: " + i)
                            .field("weight", weight[i])
                            .endObject()
                            .endObject()
                    )
                    .setRefresh(true)
                    .execute().actionGet();
        }
        client.admin().indices().prepareRefresh(INDEX).execute().actionGet();
        if (optimize) {
            // make sure merging works just fine
            client.admin().indices().prepareFlush(INDEX).execute().actionGet();
            client.admin().indices().prepareOptimize(INDEX).execute().actionGet();
        }
    }
    
    // TODO add more tests for paylads etc. 
    // we need to test across shards etc. with more nodes and shards
}
