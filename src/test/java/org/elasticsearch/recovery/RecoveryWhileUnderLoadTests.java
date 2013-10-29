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

package org.elasticsearch.recovery;

import com.google.common.base.Predicate;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.junit.annotations.TestLogging;
import org.elasticsearch.test.AbstractIntegrationTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 *
 */
@AbstractIntegrationTest.ClusterScope(numNodes = 1, scope = AbstractIntegrationTest.Scope.TEST, transportClientRatio = 0.0)
public class RecoveryWhileUnderLoadTests extends AbstractIntegrationTest {

    private final ESLogger logger = Loggers.getLogger(RecoveryWhileUnderLoadTests.class);

    @Test @TestLogging("action.search.type:TRACE,action.admin.indices.refresh:TRACE")
    @Slow
    public void recoverWhileUnderLoadAllocateBackupsTest() throws Exception {
        final int numShards = between(1, 5);
        final int numReplicas = 1;
        logger.info("--> creating test index ... num_shards [{}] num_replicas [{}]", numShards, numReplicas);
        prepareCreate("test").setSettings(randomSettingsBuilder().put("number_of_shards", numShards).put("number_of_replicas", numReplicas).build()).get();

        final AtomicLong idGenerator = new AtomicLong();
        final AtomicLong indexCounter = new AtomicLong();
        final AtomicBoolean stop = new AtomicBoolean(false);
        Thread[] writers = new Thread[5];
        final CountDownLatch stopLatch = new CountDownLatch(writers.length);

        logger.info("--> starting {} indexing threads", writers.length);
        for (int i = 0; i < writers.length; i++) {
            final int indexerId = i;
            writers[i] = new Thread("TEST_INDEXER_THREAD") {
                @Override
                public void run() {
                    try {
                        logger.info("**** starting indexing thread {}", indexerId);
                        while (!stop.get()) {
                            long id = idGenerator.incrementAndGet();
                            if (id % 1000 == 0) {
                                client().admin().indices().prepareFlush().execute().actionGet();
                            }
                            client().prepareIndex("test", "type1", Long.toString(id))
                                    .setSource(MapBuilder.<String, Object>newMapBuilder().put("test", "value" + id).map()).execute().actionGet();
                            indexCounter.incrementAndGet();
                        }
                        logger.info("**** done indexing thread {}", indexerId);
                    } catch (Throwable e) {
                        logger.warn("**** failed indexing thread {}", e, indexerId);
                    } finally {
                        stopLatch.countDown();
                    }
                }
            };
            writers[i].start();
        }

        logger.info("--> waiting for 2000 docs to be indexed ...");
        waitForDocs(2000);
        logger.info("--> 2000 docs indexed");

        logger.info("--> flushing the index ....");
        // now flush, just to make sure we have some data in the index, not just translog
        client().admin().indices().prepareFlush().execute().actionGet();


        logger.info("--> waiting for 4000 docs to be indexed ...");
        waitForDocs(4000);
        logger.info("--> 4000 docs indexed");

        logger.info("--> allow 2 nodes for index [test] ...");
        // now start another node, while we index
        cluster().ensureAtLeastNumNodes(2);


        logger.info("--> waiting for GREEN health status ...");
        // make sure the cluster state is green, and all has been recovered
        assertStatus(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForGreenStatus().setWaitForNodes(">=2").execute().actionGet(), ClusterHealthStatus.GREEN);

        logger.info("--> waiting for 10000 docs to be indexed ...");
        waitForDocs(15000);
        logger.info("--> 10000 docs indexed");

        logger.info("--> marking and waiting for indexing threads to stop ...");
        stop.set(true);
        stopLatch.await();
        logger.info("--> indexing threads stopped");

        logger.info("--> refreshing the index");
        refreshAndAssert(numShards * (numReplicas+1));
        logger.info("--> verifying indexed content");
        iterateAssertCount(numShards, indexCounter.get(), 10);
    }

    @Test @TestLogging("action.search.type:TRACE,action.admin.indices.refresh:TRACE")
    @Slow
    public void recoverWhileUnderLoadAllocateBackupsRelocatePrimariesTest() throws Exception {
        final int numShards = between(1, 5);
        final int numReplicas = 1;
        logger.info("--> creating test index ... num_shards [{}] num_replicas [{}]", numShards, numReplicas);
        prepareCreate("test").setSettings(randomSettingsBuilder().put("number_of_shards", numShards).put("number_of_replicas", numReplicas).build()).get();

        final AtomicLong idGenerator = new AtomicLong();
        final AtomicLong indexCounter = new AtomicLong();
        final AtomicBoolean stop = new AtomicBoolean(false);
        Thread[] writers = new Thread[5];
        logger.info("--> starting {} indexing threads", writers.length);
        final CountDownLatch stopLatch = new CountDownLatch(writers.length);
        for (int i = 0; i < writers.length; i++) {
            final int indexerId = i;
            writers[i] = new Thread("TEST_INDEXER_THREAD") {
                @Override
                public void run() {
                    try {
                        logger.info("**** starting indexing thread {}", indexerId);
                        while (!stop.get()) {
                            long id = idGenerator.incrementAndGet();
                            client().prepareIndex("test", "type1", Long.toString(id))
                                    .setSource(MapBuilder.<String, Object>newMapBuilder().put("test", "value" + id).map()).execute().actionGet();
                            indexCounter.incrementAndGet();
                        }
                        logger.info("**** done indexing thread {}", indexerId);
                    } catch (Throwable e) {
                        logger.warn("**** failed indexing thread {}", e, indexerId);
                    } finally {
                        stopLatch.countDown();
                    }
                }
            };
            writers[i].start();
        }

        logger.info("--> waiting for 2000 docs to be indexed ...");
        waitForDocs(2000);
        logger.info("--> 2000 docs indexed");

        logger.info("--> flushing the index ....");
        // now flush, just to make sure we have some data in the index, not just translog
        client().admin().indices().prepareFlush().execute().actionGet();


        logger.info("--> waiting for 4000 docs to be indexed ...");
        waitForDocs(4000);
        logger.info("--> 4000 docs indexed");
        logger.info("--> allow at least 4 nodes for index [test] ...");
        cluster().ensureAtLeastNumNodes(4);

        logger.info("--> waiting for GREEN health status ...");
        assertStatus(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForGreenStatus().setWaitForNodes(">=4").execute().actionGet(), ClusterHealthStatus.GREEN);


        logger.info("--> waiting for 15000 docs to be indexed ...");
        waitForDocs(15000);
        logger.info("--> 15000 docs indexed");

        stop.set(true);
        stopLatch.await();

        logger.info("--> marking and waiting for indexing threads to stop ...");
        stop.set(true);
        stopLatch.await();
        logger.info("--> indexing threads stopped");

        logger.info("--> refreshing the index");
        refreshAndAssert(numShards * (numReplicas+1));
        logger.info("--> verifying indexed content");
        iterateAssertCount(numShards, indexCounter.get(), 10);
    }

    @Test @TestLogging("action.search.type:TRACE,action.admin.indices.refresh:TRACE")
    @Slow
    public void recoverWhileUnderLoadWithNodeShutdown() throws Exception {
        final int numShards = between(1, 5);
        final int numReplicas = 1;
        logger.info("--> creating test index ... num_shards [{}] num_replicas [{}]", numShards, numReplicas);
        prepareCreate("test").setSettings(randomSettingsBuilder().put("number_of_shards", numShards).put("number_of_replicas", numReplicas).build()).get();
        cluster().ensureAtLeastNumNodes(2);

        final AtomicLong idGenerator = new AtomicLong();
        final AtomicLong indexCounter = new AtomicLong();
        final AtomicBoolean stop = new AtomicBoolean(false);
        Thread[] writers = new Thread[5];
        final CountDownLatch stopLatch = new CountDownLatch(writers.length);
        logger.info("--> starting {} indexing threads", writers.length);
        for (int i = 0; i < writers.length; i++) {
            final Client client = client();
            final int indexerId = i;
            writers[i] = new Thread("TEST_INDEXER_THREAD") {
                @Override
                public void run() {
                    try {
                        logger.info("**** starting indexing thread {}", indexerId);
                        while (!stop.get()) {
                            long id = idGenerator.incrementAndGet();
                            Client currentclient = client;
                            // currentclient = client();  // use this and it won't fail / hang
                            IndexResponse response = currentclient.prepareIndex("test", "type1", Long.toString(id))
                                    .setSource(MapBuilder.<String, Object>newMapBuilder()
                                    .put("test", "value" + id).map()).execute().actionGet();

                            assertThat(response.isCreated(), is(true));
                            indexCounter.incrementAndGet();
                        }
                        logger.info("**** done indexing thread {}", indexerId);
                    } catch (Throwable e) {
                        logger.warn("**** failed indexing thread {}", e, indexerId);
                    } finally {
                        stopLatch.countDown();
                    }
                }
            };
            writers[i].start();
        }

        logger.info("--> waiting for 2000 docs to be indexed ...");
        waitForDocs(2000);
        logger.info("--> 2000 docs indexed");

        logger.info("--> flushing the index ....");
        // now flush, just to make sure we have some data in the index, not just translog
        client().admin().indices().prepareFlush().execute().actionGet();


        logger.info("--> waiting for 4000 docs to be indexed ...");
        waitForDocs(4000);
        logger.info("--> 4000 docs indexed");

        // now start more nodes, while we index
        logger.info("--> allow 4 nodes for index [test] ...");
        cluster().ensureAtLeastNumNodes(4);

        logger.info("--> waiting for GREEN health status ...");
        assertStatus(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForGreenStatus().setWaitForNodes(">=4").execute().actionGet(), ClusterHealthStatus.GREEN);


        logger.info("--> waiting for 10000 docs to be indexed ...");
        waitForDocs(15000);
        logger.info("--> 10000 docs indexed");

        // now, shutdown nodes
        logger.info("--> stop a random node for index [test] ... num nodes in the cluster: [" + cluster().numNodes() + "]");
        cluster().stopRandomNode();
        logger.info("--> waiting for GREEN health status ...");
        assertStatus(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForGreenStatus().setWaitForNodes(">=3").execute().actionGet(), ClusterHealthStatus.GREEN);

        logger.info("--> stop a random node for index [test] ... num nodes in the cluster: [" + cluster().numNodes() + "]");
        cluster().stopRandomNode();

        logger.info("--> waiting for GREEN health status ...");
        assertStatus(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForGreenStatus().setWaitForNodes(">=2").execute().actionGet(), ClusterHealthStatus.GREEN);
        
        logger.info("--> allow 1 nodes for index [test] ...");
        cluster().ensureAtMostNumNodes(1);
        logger.info("--> waiting for YELLOW health status ...");
        assertStatus(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForYellowStatus().setWaitForNodes(">=1").execute().actionGet(), ClusterHealthStatus.YELLOW);

        logger.info("--> marking and waiting for indexing threads to stop ...");
        stop.set(true);
        stopLatch.await();
        logger.info("--> indexing threads stopped");

        assertStatus(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForYellowStatus().setWaitForNodes(">=1").execute().actionGet(), ClusterHealthStatus.YELLOW);

        logger.info("--> refreshing the index");
        refreshAndAssert(numShards);
        logger.info("--> verifying indexed content");
        iterateAssertCount(numShards, indexCounter.get(), 10);

    }

    private void iterateAssertCount(final int numberOfShards, final long numberOfDocs, final int iterations) throws Exception {
        SearchResponse[] iterationResults = new SearchResponse[iterations];
        boolean error = false;
        for (int i = 0; i < iterations; i++) {
            SearchResponse searchResponse = client().prepareSearch().setSearchType(SearchType.COUNT).setQuery(matchAllQuery()).get();
            logSearchResponse(numberOfShards, numberOfDocs, i, searchResponse);
            iterationResults[i] = searchResponse;
            if (searchResponse.getHits().totalHits() != numberOfDocs) {
                error = true;
            }
        }

        if (error) {
            //Printing out shards and their doc count
            IndicesStatsResponse indicesStatsResponse = client().admin().indices().prepareStats().get();
            for (ShardStats shardStats : indicesStatsResponse.getShards()) {
                DocsStats docsStats = shardStats.getStats().docs;
                logger.info("shard [{}] - count {}, primary {}", shardStats.getShardId(), docsStats.getCount(), shardStats.getShardRouting().primary());
            }

            //if there was an error we try to wait and see if at some point it'll get fixed
            logger.info("--> trying to wait");
            assertThat(awaitBusy(new Predicate<Object>() {
                @Override
                public boolean apply(Object o) {
                    boolean error = false;
                    for (int i = 0; i < iterations; i++) {
                        SearchResponse searchResponse = client().prepareSearch().setSearchType(SearchType.COUNT).setQuery(matchAllQuery()).get();
                        if (searchResponse.getHits().totalHits() != numberOfDocs) {
                            error = true;
                        }
                    }
                    return !error;
                }
            }, 5, TimeUnit.MINUTES), equalTo(true));
        }

        //lets now make the test fail if it was supposed to fail
        for (int i = 0; i < iterations; i++) {
            assertHitCount(iterationResults[i], numberOfDocs);
        }
    }

    private void logSearchResponse(int numberOfShards, long numberOfDocs, int iteration, SearchResponse searchResponse) {
        logger.info("iteration [{}] - successful shards: {} (expected {})", iteration, searchResponse.getSuccessfulShards(), numberOfShards);
        logger.info("iteration [{}] - failed shards: {} (expected 0)", iteration, searchResponse.getFailedShards());
        if (searchResponse.getShardFailures() != null && searchResponse.getShardFailures().length > 0) {
            logger.info("iteration [{}] - shard failures: {}", iteration, Arrays.toString(searchResponse.getShardFailures()));
        }
        logger.info("iteration [{}] - returned documents: {} (expected {})", iteration, searchResponse.getHits().totalHits(), numberOfDocs);
    }

    private void refreshAndAssert(final int expectedSuccessfulShards) throws InterruptedException {
        assertThat(awaitBusy(new Predicate<Object>() {
            public boolean apply(Object o) {
                try {
                    RefreshResponse actionGet = client().admin().indices().prepareRefresh().execute().actionGet();
                    assertNoFailures(actionGet);
                    logger.debug("Asserting Refresh success [{}] total [{}] failed [{}] expectedSuccessfulShards [{}]", actionGet.getTotalShards(), actionGet.getSuccessfulShards(), actionGet.getFailedShards(), expectedSuccessfulShards);
                    return actionGet.getFailedShards() == 0 && actionGet.getSuccessfulShards() == expectedSuccessfulShards;
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            }
        }, 5, TimeUnit.MINUTES), equalTo(true));
    }
    
    private void waitForDocs(final long numDocs) throws InterruptedException {
        assertThat(awaitBusy(new Predicate<Object>() {
            public boolean apply(Object o) {
                return client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount() > numDocs;
            }
        }, 5, TimeUnit.MINUTES), equalTo(true)); // not really relevant here we just have to wait some time
    }
}
