package org.elasticsearch.test.integration.search.query;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class MatchQueryTest extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        startNode("node1");
        client = getClient();
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
    public void testOptionalStopwords() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        
        
        
        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().loadFromSource(jsonBuilder()
                .startObject()
                .startObject("index").field("number_of_shards", 2).endObject()
                .startObject("analysis")
                    .startObject("analyzer")
                        .startObject("test")
                            .field("type", "custom")
                            .field("tokenizer", "whitespace")
                            .field("filter", new String[]{"stopword_marker"})
                        .endObject()
                    .endObject()
                .endObject()
            .endObject().string()))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("field1").field("type", "string").field("analyzer", "test").endObject().endObject().endObject().endObject())
                .execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource("field1", "the quick brown fox").setRefresh(true).execute().actionGet();
        client.prepareIndex("test", "type1", "2").setSource("field1", "a slow brown fox").setRefresh(true).execute().actionGet();
        client.prepareIndex("test", "type1", "3").setSource("field1", "the brown foxy").setRefresh(true).execute().actionGet();

        

        SearchResponse searchResponse = client.prepareSearch().setQuery(QueryBuilders.matchQuery("field1", "the brown fox").operator(Operator.AND).type(MatchQueryBuilder.Type.BOOLEAN).stopwordsOptional(true)).execute().actionGet();
        assertDocs(searchResponse.hits(), "1", "2");
        searchResponse = client.prepareSearch().setQuery(QueryBuilders.matchQuery("field1", "the brown fox").operator(Operator.OR).type(MatchQueryBuilder.Type.BOOLEAN).stopwordsOptional(true)).execute().actionGet();
        assertDocs(searchResponse.hits(), "1", "2", "3");
        searchResponse = client.prepareSearch().setQuery(QueryBuilders.matchQuery("field1", "the brown fox").operator(Operator.OR).minimumShouldMatch("2").type(MatchQueryBuilder.Type.BOOLEAN).stopwordsOptional(true)).execute().actionGet();
        assertDocs(searchResponse.hits(), "1", "2");
        
        searchResponse = client.prepareSearch().setQuery(QueryBuilders.matchQuery("field1", "the brown fox").operator(Operator.OR).type(MatchQueryBuilder.Type.BOOLEAN)).execute().actionGet();
        assertDocs(searchResponse.hits(), "1", "2", "3");
        
        searchResponse = client.prepareSearch().setQuery(QueryBuilders.matchQuery("field1", "the brown fox").operator(Operator.AND).type(MatchQueryBuilder.Type.BOOLEAN)).execute().actionGet();
        assertDocs(searchResponse.hits(), "1");
    }
    
    private static void assertDocs(SearchHits hits, String...docs) {
        Set<String> docSet = new HashSet<String>();
        docSet.addAll(Arrays.asList(docs));
        assertThat((long)docSet.size(), equalTo(hits.totalHits()));
        for (SearchHit hit : hits) {
           docSet.remove(hit.id());
        }
        
        assertThat("failed to retrieve docs: " + docSet, docSet.isEmpty());
    }
}
