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
package org.elasticsearch.action.bench;

import org.apache.lucene.util.English;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.action.bench.RestBenchAction;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.*;

/**
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE)
public class BenchIntegrationTest extends ElasticsearchIntegrationTest {

    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder().put("node.bench", true).build();
    }

    public void testBasic() throws Exception {
        client().admin()
                .indices()
                .prepareCreate("test")
                .addMapping(
                        "type1",
                        jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("field1")
                                .field("analyzer", "whitespace").field("type", "string").endObject().endObject().endObject().endObject())
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", between(1, 5)).put("index.number_of_replicas", 0)).execute().actionGet();
        ensureGreen();

        int numDocs = atLeast(50);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test", "type1", String.valueOf(i)).setSource("field1", English.intToEnglish(i));
        }
        indexRandom(true, docs);

        SearchRequest[] requests = new SearchRequest[between(1, 10)];
        for (int i = 0; i < requests.length; i++) {
            requests[i] = new SearchRequestBuilder(client()).setIndices("test").setQuery(QueryBuilders.queryString(English.intToEnglish(i))).request();
        }
        final int numIters = atLeast(3);
        for (int iters = 0; iters < numIters; iters++) {
            int numSlowest = between(0, 5);
            BenchRequest newRequest = newRequest(client().prepareBench().
                    setBenchmarkId("test").
                    addCompetitor(BenchSpecBuilder.competitorBuilder("test").
                            setName("local_doc_freq").
                            setWarmup(true).
                            setMultiplier(between(10, 100)).
                            setIterations(between(1, 5)).
                            setConcurrency(between(1, 5)).
                            setNumSlowest(numSlowest).
                            addSearchRequest(requests)
                    ).
                    addCompetitor(BenchSpecBuilder.competitorBuilder("test").
                            setName("distributed_doc_freq").
                            setWarmup(true).
                            setMultiplier(between(10, 100)).
                            setIterations(between(1, 5)).
                            setConcurrency(between(1, 5)).
                            setSearchType(SearchType.DFS_QUERY_THEN_FETCH).
                            setNumSlowest(numSlowest).
                            defaultSearchReqeust(new SearchRequestBuilder(client()).
                                    setFrom(between(0, 5)).setIndices(Strings.EMPTY_ARRAY).request())

                    )
                    .setNumExecutors(between(1, 2))
                    .addSearchRequest(requests).request());
            final AtomicReference<BenchResponse> res = new AtomicReference<BenchResponse>();
            final AtomicReference<Throwable> exception = new AtomicReference<Throwable>();
            final CountDownLatch latch = new CountDownLatch(1);
            client().bench(newRequest, new ActionListener<BenchResponse>() {
                @Override
                public void onResponse(BenchResponse benchResponse) {
                    res.set(benchResponse);
                    latch.countDown();
                }

                @Override
                public void onFailure(Throwable e) {
                    logger.info("failed!", e);
                    exception.set(e);
                    latch.countDown();
                }
            });
            latch.await();
            assertThat(exception.get(), nullValue());
            BenchResponse benchResponse = res.get();
            assertThat(benchResponse.failures(), emptyArray());
            assertThat(benchResponse, notNullValue());
            List<BenchSpecResult> competitorResults = benchResponse.competitorResults();
            assertThat(competitorResults, not(emptyIterable()));
            logger.debug("Current Response {}", benchResponse.toString());
            for (BenchSpecResult compRes : competitorResults) {
                List<BenchSpecResult.Iteration> rounds = compRes.iters();
                for (BenchSpecResult.Iteration r : rounds) {
                    assertThat(r.percentile90th(), greaterThanOrEqualTo(r.mean()));
                    if (numSlowest == 0) {
                        assertThat(r.slowest(), nullValue());
                    } else {
                        if (numSlowest <= requests.length) {
                            assertThat(r.slowest().requests().length, equalTo(numSlowest));
                        } else {
                            assertThat(r.slowest().requests().length, equalTo(requests.length));
                        }
                        long[] timeTaken = r.slowest().avgTimeTaken();
                        long[] maxTime = r.slowest().maxTimeTaken();
                        for (int i = 0; i < timeTaken.length; i++) {
                            if (i > 0) {
                                // we are sorting by avg time
                                assertThat(timeTaken[i - 1], greaterThanOrEqualTo(timeTaken[i]));
                            } else {
                                assertThat(maxTime[i], greaterThanOrEqualTo(r.percentile90th())); // is this correct also for very fast queries?
                            }
                        }
                    }
                }
            }
        }
    }



    @Test
    public void testAbort() throws Exception {
        client().admin()
                .indices()
                .prepareCreate("test")
                .addMapping(
                        "type1",
                        jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("field1")
                                .field("analyzer", "whitespace").field("type", "string").endObject().endObject().endObject().endObject())
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", between(1, 5)).put("index.number_of_replicas", 0)).execute().actionGet();
        ensureGreen();

        int numDocs = atLeast(50);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test", "type1", String.valueOf(i)).setSource("field1", English.intToEnglish(i));
        }
        indexRandom(true, docs);

        SearchRequest[] requests = new SearchRequest[between(1, 10)];
        for (int i = 0; i < requests.length; i++) {
            requests[i] = new SearchRequestBuilder(client()).setIndices("test").setQuery(QueryBuilders.queryString(English.intToEnglish(i))).request();
        }
        int numSlowest = between(0, 5);

        BenchRequest newRequest = newRequest(client().prepareBench().
                setBenchmarkId("test").
                addCompetitor(BenchSpecBuilder.competitorBuilder("test").
                        setName("local_doc_freq").
                        setWarmup(true).
                        setMultiplier(100). // FOREVER :)
                        setIterations(Integer.MAX_VALUE).
                        setConcurrency(between(1, 5)).
                        setNumSlowest(numSlowest).
                        addSearchRequest(requests)
                )
                .setNumExecutors(between(1, 2))
                .addSearchRequest(requests).request());

        final AtomicReference<BenchResponse> res = new AtomicReference<BenchResponse>();
        final AtomicReference<Throwable> exception = new AtomicReference<Throwable>();
        final CountDownLatch latch = new CountDownLatch(1);
        client().bench(newRequest, new ActionListener<BenchResponse>() {
            @Override
            public void onResponse(BenchResponse benchResponse) {
                res.set(benchResponse);
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable e) {
                logger.info("failed!", e);
                exception.set(e);
                latch.countDown();
            }
        });
        boolean await = latch.await(100, TimeUnit.MILLISECONDS);
        assertThat(await, is(false));
        assertAcked(client().prepareAbortBench("test").get());
        assertThat(latch.await(10, TimeUnit.SECONDS), is(true)); // should be quick
        BenchResponse benchResponse = res.get();
        assertThat(benchResponse, notNullValue());
        assertThat(benchResponse.isAborted(), is(true));
        try {
            client().prepareAbortBench("test").get();
        } catch (ElasticSearchIllegalStateException ex) {
            assertThat(ex.getMessage(), equalTo("Can't abort benchmark for id: [test] - illegal state [FAILED]"));
        }

        try {
            client().prepareAbortBench("foobar").get();
        } catch (ElasticSearchException ex) {
            assertThat(ex.getMessage(), equalTo("No Benchmark found for id: [foobar]"));
        }

    }

    /*
     * Here we randomly serialize a request and parse it via the static methods from RestBenchAction
     */
    public BenchRequest newRequest(BenchRequest request) throws Exception {
        if (randomBoolean()) {
            return request; // randomly serialize and parse
        }
        XContentBuilder jsonReqBuilder = jsonBuilder();
        jsonReqBuilder.startObject();
        Set<String> indices = new HashSet<String>();
        Set<String> types = new HashSet<String>();
        jsonReqBuilder.field("id", request.benchmarkId());
        jsonReqBuilder.startArray("requests");
        for (SearchRequest r : request.searchRequests()) {
            XContentHelper.writeDirect(r.source(), jsonReqBuilder, null);
            types.addAll(Arrays.asList(r.types()));
            indices.addAll(Arrays.asList(r.indices()));
        }
        jsonReqBuilder.endArray();

        List<BenchSpec> competitors = request.competiors();

        jsonReqBuilder.startArray("competitors");
        for (BenchSpec comp : competitors) {
            jsonReqBuilder.startObject()
                    .field("multiplier", comp.multiplier())
                    .field("iterations", comp.iterations())
                    .field("num_slowest", comp.numSlowest())
                    .field("warmup", comp.warmup())
                    .field("name", comp.name())
                    .field("search_type", comp.searchType().name().toLowerCase(Locale.ROOT))
                    .field("concurrency", comp.concurrency())
                    .field("indices", comp.indices());
            jsonReqBuilder.startArray("requests");
            for (SearchRequest r : comp.searchRequests()) {
                XContentHelper.writeDirect(r.source(), jsonReqBuilder, null);
                types.addAll(Arrays.asList(r.types()));
                indices.addAll(Arrays.asList(r.indices()));
            }
            jsonReqBuilder.endArray();
            if (comp.defaultSearchRequest() != null) {
                XContentHelper.writeRawField("defaults", comp.defaultSearchRequest().source(), jsonReqBuilder, null);
            }
            jsonReqBuilder.endObject();
        }
        jsonReqBuilder.endArray();
        jsonReqBuilder.endObject();

        BytesReference json = jsonReqBuilder.bytes();
        XContentBuilder builder = XContentFactory.jsonBuilder();
        XContentHelper.writeDirect(json, builder, null);
        logger.debug("Serialized Request: " + builder.string());

        BenchRequestBuilder rb = new BenchRequestBuilder(client(), request.indices());
        BenchRequest parse = RestBenchAction.parse(rb, jsonReqBuilder.bytes(), randomBoolean(), indices.toArray(new String[0]), types.toArray(new String[0]));
        assertThat(parse.benchmarkId(), equalTo(request.benchmarkId()));
        assertThat(parse.searchRequests().size(), equalTo(request.searchRequests().size()));
        for (int i = 0; i < request.searchRequests().size(); i++) {
            assertThat(sourceToString(parse.searchRequests().get(i)), equalTo(sourceToString(request.searchRequests().get(i))));
        }

        int j = 0;
        for (BenchSpec left : parse.competiors()) {
            BenchSpec right = request.competiors().get(j++);
            assertThat(left.numSlowest(), equalTo(right.numSlowest()));
            assertThat(left.iterations(), equalTo(right.iterations()));
            assertThat(left.multiplier(), equalTo(right.multiplier()));
            assertThat(left.concurrency(), equalTo(right.concurrency()));
            assertThat(left.warmup(), equalTo(right.warmup()));
            assertThat(left.searchRequests().size(), equalTo(right.searchRequests().size()));
            for (int i = 0; i < left.searchRequests().size(); i++) {
                assertThat(sourceToString(left.searchRequests().get(i)), equalTo(sourceToString(right.searchRequests().get(i))));
            }
        }
        return parse;
    }

    private static String sourceToString(SearchRequest req) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        XContentHelper.writeDirect(req.source(), builder, null);
        String res = builder.string();
        return res;
    }

}
