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

import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PairOutputs.Pair;

import org.apache.lucene.search.suggest.analyzing.XAnalyzingSuggester.XBuilder;

import org.apache.lucene.analysis.core.KeywordTokenizer;

import org.apache.lucene.util.CharsRef;

import org.apache.lucene.analysis.synonym.SynonymMap;

import org.apache.lucene.analysis.synonym.SynonymFilter;

import org.apache.lucene.analysis.core.LowerCaseFilter;

import org.apache.lucene.analysis.TokenFilter;

import org.apache.lucene.analysis.core.WhitespaceTokenizer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;

import org.apache.lucene.analysis.standard.StandardAnalyzer;

import org.apache.lucene.search.spell.TermFreqPayloadIterator;

import org.apache.lucene.search.spell.TermFreqPayloadIterator;

import org.apache.lucene.util.BytesRef;

import org.apache.lucene.search.spell.TermFreqIterator;

import org.apache.lucene.util.Version;

import org.apache.lucene.analysis.core.SimpleAnalyzer;

import org.apache.lucene.search.suggest.analyzing.XAnalyzingSuggester;

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
import java.io.Reader;
import java.util.Comparator;
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
        for (int i = 0; i < weight.length; i++) {
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
                    .execute().actionGet();
        }
        
        
//        
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
//       
//        if (optimize) {
//            // make sure merging works just fine
         
//            client.admin().indices().prepareOptimize(INDEX).execute().actionGet();
//        }
    }
    
    public static void main(String[] args) throws IOException {
//        foofightersgenerator 0 Generator - Foo Fighters
//        generator 0 Generator - Foo Fighters
        
        XBuilder b = new XBuilder(256, true);
        b.startTerm(new BytesRef("foofightersgenerator"));
        b.addSurface(new BytesRef("Generator - Foo Fighters"), new BytesRef(), 10);
        b.finishTerm(0);
        b.startTerm(new BytesRef("generator"));
        b.addSurface(new BytesRef("Generator - Foo Fighters"), new BytesRef(), 10);
        b.finishTerm(0);
        FST<Pair<Long, BytesRef>> build = b.build();
        Analyzer analyzer = new Analyzer() {
            
            @Override
            protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
                KeywordTokenizer wst = new KeywordTokenizer(reader);
                TokenFilter f = new LowerCaseFilter(Version.LUCENE_43, wst);
                SynonymMap.Builder m = new SynonymMap.Builder(false);
                m.add(new CharsRef("generator foo fighters"), new CharsRef("generator"), false);
                m.add(new CharsRef("generator foo fighters"), new CharsRef("foo fighters generator"), false);
                SynonymFilter filter;
                try {
                    filter = new SynonymFilter(f, m.build(), false);
                } catch (IOException e) {
                    throw new RuntimeException();
                }
                return new TokenStreamComponents(wst, filter);
            }
        };
        XAnalyzingSuggester sugg = new XAnalyzingSuggester(analyzer, analyzer, XAnalyzingSuggester.PRESERVE_SEP, 256, -1,  null, true, 0);
        final String[] inputs = {"generator foo fighters"};
        sugg.build(new TermFreqPayloadIterator() {
            int weight = 10;
            int i;
            @Override
            public BytesRef next() throws IOException {
                if (i == inputs.length) {
                    return null;
                }
                return new BytesRef(inputs[i++]);
            }
            
            @Override
            public Comparator<BytesRef> getComparator() {
                return null;
            }
            
            @Override
            public long weight() {
                return weight++;
            }
            
            @Override
            public BytesRef payload() {
                return new BytesRef("id: 1");
            }
        });
        sugg.lookup("ge", false, 10);
        
        sugg = new XAnalyzingSuggester(new StandardAnalyzer(Version.LUCENE_43), new StandardAnalyzer(Version.LUCENE_43), XAnalyzingSuggester.PRESERVE_SEP , 256, -1,  build, true, 3);
        sugg.lookup("ge", false, 10);


    }
}
