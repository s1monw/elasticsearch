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
package org.elasticsearch;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Iterators;
import org.apache.lucene.util.AbstractRandomizedTest.IntegrationTests;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.optimize.OptimizeResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndexTemplateMissingException;
import org.elasticsearch.rest.RestStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;

/**
 * This abstract base testcase reuses a cluster instance internally and might
 * start an abitrary number of nodes in the background. This class might in the
 * future add random configureation options to created indices etc. unless
 * unless they are explicitly defined by the test.
 * <p/>
 * <p>
 * This test wipes all indices before a testcase is executed and uses
 * elasticsearch features like allocation filters to ensure an index is
 * allocated only on a certain number of nodes. The test doesn't expose explicit
 * information about the client or which client is returned, clients might be
 * node clients or transport clients and the returned client might be rotated.
 * </p>
 * <p/>
 * Tests that need more explict control over the cluster or that need to change
 * the cluster state aside of per-index settings should not use this class as a
 * baseclass. If your test modifies the cluster state with persistent or
 * transient settings the baseclass will raise and error.
 */
@Ignore
@IntegrationTests
public abstract class AbstractSharedClusterTest extends ElasticsearchTestCase {

    private static final TestCluster globalCluster = new TestCluster(SHARED_CLUSTER_SEED, TestCluster.clusterName("shared", ElasticsearchTestCase.CHILD_VM_ID, SHARED_CLUSTER_SEED));
    private static TestCluster currentCluster;
    private static final Map<Class<?>, TestCluster> clusters = new IdentityHashMap<Class<?>, TestCluster>();
    
    @Before
    public final void before() throws IOException {
        ClusterScope currentClusterScope = getCurrentClusterScope();
        if (currentClusterScope == ClusterScope.Suite) {
            TestCluster testCluster = clusters.get(this.getClass());
            if (testCluster == null) {
                currentCluster = buildTestCluster(currentClusterScope);
                clearClusters();
                clusters.put(this.getClass(), currentCluster);
            } else {
                currentCluster = testCluster;
            }
        } else if (currentClusterScope == ClusterScope.Test) {
            currentCluster = buildTestCluster(currentClusterScope);
            clearClusters();
            clusters.put(this.getClass(), currentCluster);
        } else {
            clearClusters();
            currentCluster = globalCluster;
        }
        currentCluster.beforeTest(getRandom());
        wipeIndices();
        wipeTemplates();
        logger.info("[{}#{}]: before test", getTestClass().getSimpleName(), getTestName());
    }
    
    private void clearClusters() throws IOException {
        if (!clusters.isEmpty()) {
            IOUtils.close(clusters.values());
            clusters.clear();
        }
    }
    
    

    @After
    public void after() throws IOException {
        logger.info("[{}#{}]: cleaning up after test", getTestClass().getSimpleName(), getTestName());
        MetaData metaData = client().admin().cluster().prepareState().execute().actionGet().getState().getMetaData();
        assertThat("test leaves persistent cluster metadata behind: " + metaData.persistentSettings().getAsMap(), metaData
                .persistentSettings().getAsMap().size(), equalTo(0));
        assertThat("test leaves transient cluster metadata behind: " + metaData.transientSettings().getAsMap(), metaData
                .persistentSettings().getAsMap().size(), equalTo(0));
        wipeIndices(); // wipe after to make sure we fail in the test that didn't ack the delete
        wipeTemplates();
        ensureAllFilesClosed();
        logger.info("[{}#{}]: cleaned up after test", getTestClass().getSimpleName(), getTestName());
        ClusterScope currentClusterScope = getCurrentClusterScope();
        if (currentClusterScope == ClusterScope.Test) {
            if (!clusters.isEmpty()) {
                IOUtils.close(clusters.values());
                clusters.clear();
            }
        }
        currentCluster = null;
    }

    public static TestCluster cluster() {
        return currentCluster;
    }
    
    public ClusterService clusterService() {
        return cluster().clusterService();
    }

    public static Client client() {
        return cluster().client();
    }

    public static Iterable<Client> clients() {
        return cluster().clients();
    }

    public ImmutableSettings.Builder randomSettingsBuilder() {
        // TODO RANDOMIZE
        return ImmutableSettings.builder();
    }
    // TODO Randomize MergePolicyProviderBase.INDEX_COMPOUND_FORMAT [true|false|"true"|"false"|[0..1]| toString([0..1])]

    public Settings getSettings() {
        return randomSettingsBuilder().build();
    }

    public static void wipeIndices(String... names) {
        if (cluster().numNodes() > 0) {
            try {
                assertAcked(client().admin().indices().prepareDelete(names));
            } catch (IndexMissingException e) {
                // ignore
            }
        }
    }

    public static void wipeIndex(String name) {
        wipeIndices(name);
    }

    /**
     * Deletes index templates, support wildcard notation.
     */
    public static void wipeTemplates(String... templates) {
        if (cluster().numNodes() > 0) {
            // if nothing is provided, delete all
            if (templates.length == 0) {
                templates = new String[]{"*"};
            }
            for (String template : templates) {
                try {
                    client().admin().indices().prepareDeleteTemplate(template).execute().actionGet();
                } catch (IndexTemplateMissingException e) {
                    // ignore
                }
            }
        }
    }

    public void createIndex(String... names) {
        for (String name : names) {
            try {
                assertAcked(prepareCreate(name).setSettings(getSettings()));
                continue;
            } catch (IndexAlreadyExistsException ex) {
                wipeIndex(name);
            }
            assertAcked(prepareCreate(name).setSettings(getSettings()));
        }
    }

    public CreateIndexRequestBuilder prepareCreate(String index, int numNodes) {
        return prepareCreate(index, numNodes, ImmutableSettings.builder());
    }

    public CreateIndexRequestBuilder prepareCreate(String index, int numNodes, ImmutableSettings.Builder builder) {
        cluster().ensureAtLeastNumNodes(numNodes);
        Settings settings = getSettings();
        builder.put(settings);
        if (numNodes > 0) {
            getExcludeSettings(index, numNodes, builder);
        }
        return client().admin().indices().prepareCreate(index).setSettings(builder.build());
    }

    private ImmutableSettings.Builder getExcludeSettings(String index, int num, ImmutableSettings.Builder builder) {
        String exclude = Joiner.on(',').join(cluster().allButN(num));
        builder.put("index.routing.allocation.exclude._name", exclude);
        return builder;
    }

    public Set<String> getExcludeNodes(String index, int num) {
        Set<String> nodeExclude = cluster().nodeExclude(index);
        Set<String> nodesInclude = cluster().nodesInclude(index);
        if (nodesInclude.size() < num) {
            Iterator<String> limit = Iterators.limit(nodeExclude.iterator(), num - nodesInclude.size());
            while (limit.hasNext()) {
                limit.next();
                limit.remove();
            }
        } else {
            Iterator<String> limit = Iterators.limit(nodesInclude.iterator(), nodesInclude.size() - num);
            while (limit.hasNext()) {
                nodeExclude.add(limit.next());
                limit.remove();
            }
        }
        return nodeExclude;
    }

    public void allowNodes(String index, int numNodes) {
        cluster().ensureAtLeastNumNodes(numNodes);
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        if (numNodes > 0) {
            getExcludeSettings(index, numNodes, builder);
        }
        Settings build = builder.build();
        if (!build.getAsMap().isEmpty()) {
            client().admin().indices().prepareUpdateSettings(index).setSettings(build).execute().actionGet();
        }
    }

    public CreateIndexRequestBuilder prepareCreate(String index) {
        return client().admin().indices().prepareCreate(index).setSettings(getSettings());
    }

    public void updateClusterSettings(Settings settings) {
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings).execute().actionGet();
    }

    public ClusterHealthStatus ensureGreen() {
        ClusterHealthResponse actionGet = client().admin().cluster()
                .health(Requests.clusterHealthRequest().waitForGreenStatus().waitForEvents(Priority.LANGUID).waitForRelocatingShards(0)).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("ensureGreen timed out, cluster state:\n{}\n{}", client().admin().cluster().prepareState().get().getState().prettyPrint(), client().admin().cluster().preparePendingClusterTasks().get().prettyPrint());
            assertThat("timed out waiting for green state", actionGet.isTimedOut(), equalTo(false));
        }
        assertThat(actionGet.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        return actionGet.getStatus();
    }

    public ClusterHealthStatus waitForRelocation() {
        return waitForRelocation(null);
    }

    public ClusterHealthStatus waitForRelocation(ClusterHealthStatus status) {
        ClusterHealthRequest request = Requests.clusterHealthRequest().waitForRelocatingShards(0);
        if (status != null) {
            request.waitForStatus(status);
        }
        ClusterHealthResponse actionGet = client().admin().cluster()
                .health(request).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("waitForRelocation timed out (status={}), cluster state:\n{}\n{}", status, client().admin().cluster().prepareState().get().getState().prettyPrint(), client().admin().cluster().preparePendingClusterTasks().get().prettyPrint());
            assertThat("timed out waiting for relocation", actionGet.isTimedOut(), equalTo(false));
        }
        if (status != null) {
            assertThat(actionGet.getStatus(), equalTo(status));
        }
        return actionGet.getStatus();
    }

    public ClusterHealthStatus ensureYellow() {
        ClusterHealthResponse actionGet = client().admin().cluster()
                .health(Requests.clusterHealthRequest().waitForRelocatingShards(0).waitForYellowStatus().waitForEvents(Priority.LANGUID)).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("ensureYellow timed out, cluster state:\n{}\n{}", client().admin().cluster().prepareState().get().getState().prettyPrint(), client().admin().cluster().preparePendingClusterTasks().get().prettyPrint());
            assertThat("timed out waiting for yellow", actionGet.isTimedOut(), equalTo(false));
        }
        return actionGet.getStatus();
    }

    public static String commaString(Iterable<String> strings) {
        return Joiner.on(',').join(strings);
    }

    // utils
    protected IndexResponse index(String index, String type, XContentBuilder source) {
        return client().prepareIndex(index, type).setSource(source).execute().actionGet();
    }

    protected IndexResponse index(String index, String type, String id, Map<String, Object> source) {
        return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
    }


    protected GetResponse get(String index, String type, String id) {
        return client().prepareGet(index, type, id).execute().actionGet();
    }

    protected IndexResponse index(String index, String type, String id, XContentBuilder source) {
        return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
    }

    protected IndexResponse index(String index, String type, String id, Object... source) {
        return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
    }

    protected RefreshResponse refresh() {
        waitForRelocation();
        // TODO RANDOMIZE with flush?
        RefreshResponse actionGet = client().admin().indices().prepareRefresh().execute().actionGet();
        assertNoFailures(actionGet);
        return actionGet;
    }

    protected void flushAndRefresh() {
        flush(true);
        refresh();
    }

    protected FlushResponse flush() {
        return flush(true);
    }

    protected FlushResponse flush(boolean ignoreNotAllowed) {
        waitForRelocation();
        FlushResponse actionGet = client().admin().indices().prepareFlush().execute().actionGet();
        if (ignoreNotAllowed) {
            for (ShardOperationFailedException failure : actionGet.getShardFailures()) {
                assertThat("unexpected flush failure " + failure.reason(), failure.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
            }
        } else {
            assertNoFailures(actionGet);
        }
        return actionGet;
    }

    protected OptimizeResponse optimize() {
        waitForRelocation();
        OptimizeResponse actionGet = client().admin().indices().prepareOptimize().execute().actionGet();
        assertNoFailures(actionGet);
        return actionGet;
    }

    protected Set<String> nodeIdsWithIndex(String... indices) {
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        GroupShardsIterator allAssignedShardsGrouped = state.routingTable().allAssignedShardsGrouped(indices, true);
        Set<String> nodes = new HashSet<String>();
        for (ShardIterator shardIterator : allAssignedShardsGrouped) {
            for (ShardRouting routing : shardIterator.asUnordered()) {
                if (routing.active()) {
                    nodes.add(routing.currentNodeId());
                }

            }
        }
        return nodes;
    }

    protected int numAssignedShards(String... indices) {
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        GroupShardsIterator allAssignedShardsGrouped = state.routingTable().allAssignedShardsGrouped(indices, true);
        return allAssignedShardsGrouped.size();
    }

    protected boolean indexExists(String index) {
        IndicesExistsResponse actionGet = client().admin().indices().prepareExists(index).execute().actionGet();
        return actionGet.isExists();
    }

    protected AdminClient admin() {
        return client().admin();
    }

    protected <Res extends ActionResponse> Res run(ActionRequestBuilder<?, Res, ?> builder) {
        Res actionGet = builder.execute().actionGet();
        return actionGet;
    }

    protected <Res extends BroadcastOperationResponse> Res run(BroadcastOperationRequestBuilder<?, Res, ?> builder) {
        Res actionGet = builder.execute().actionGet();
        assertNoFailures(actionGet);
        return actionGet;
    }

    // TODO move this into a base class for integration tests
    public void indexRandom(boolean forceRefresh, IndexRequestBuilder... builders) throws InterruptedException, ExecutionException {
        if (builders.length == 0) {
            return;
        }
        Random random = getRandom();
        Set<String> indicesSet = new HashSet<String>();
        for (int i = 0; i < builders.length; i++) {
            indicesSet.add(builders[i].request().index());
        }
        final String[] indices = indicesSet.toArray(new String[0]);
        List<IndexRequestBuilder> list = Arrays.asList(builders);
        Collections.shuffle(list, random);
        final CopyOnWriteArrayList<Throwable> errors = new CopyOnWriteArrayList<Throwable>();
        List<CountDownLatch> latches = new ArrayList<CountDownLatch>();
        if (frequently()) {
            logger.info("Index [{}] docs async: [{}]", list.size(), true);
            final CountDownLatch latch = new CountDownLatch(list.size());
            latches.add(latch);
            for (IndexRequestBuilder indexRequestBuilder : list) {
                indexRequestBuilder.execute(new LatchedActionListener<IndexResponse>(latch, errors));
                if (rarely()) {
                    if (rarely()) {
                        client().admin().indices().prepareRefresh(indices).setIgnoreIndices(IgnoreIndices.MISSING).execute(new LatchedActionListener<RefreshResponse>(newLatch(latches), errors));
                    } else if (rarely()) {
                        client().admin().indices().prepareFlush(indices).setIgnoreIndices(IgnoreIndices.MISSING).execute(new LatchedActionListener<FlushResponse>(newLatch(latches), errors));
                    } else if (rarely()) {
                        client().admin().indices().prepareOptimize(indices).setIgnoreIndices(IgnoreIndices.MISSING).setMaxNumSegments(between(1, 10)).setFlush(random.nextBoolean()).execute(new LatchedActionListener<OptimizeResponse>(newLatch(latches), errors));
                    }
                }
            }

        } else {
            logger.info("Index [{}] docs async: [{}]", list.size(), false);
            for (IndexRequestBuilder indexRequestBuilder : list) {
                indexRequestBuilder.execute().actionGet();
                if (rarely()) {
                    if (rarely()) {
                        client().admin().indices().prepareRefresh(indices).setIgnoreIndices(IgnoreIndices.MISSING).execute(new LatchedActionListener<RefreshResponse>(newLatch(latches), errors));
                    } else if (rarely()) {
                        client().admin().indices().prepareFlush(indices).setIgnoreIndices(IgnoreIndices.MISSING).execute(new LatchedActionListener<FlushResponse>(newLatch(latches), errors));
                    } else if (rarely()) {
                        client().admin().indices().prepareOptimize(indices).setIgnoreIndices(IgnoreIndices.MISSING).setMaxNumSegments(between(1, 10)).setFlush(random.nextBoolean()).execute(new LatchedActionListener<OptimizeResponse>(newLatch(latches), errors));
                    }
                }
            }
        }
        for (CountDownLatch countDownLatch : latches) {
            countDownLatch.await();
        }
        assertThat(errors, emptyIterable());
        if (forceRefresh) {
            assertNoFailures(client().admin().indices().prepareRefresh(indices).setIgnoreIndices(IgnoreIndices.MISSING).execute().get());
        }
    }

    private static final CountDownLatch newLatch(List<CountDownLatch> latches) {
        CountDownLatch l = new CountDownLatch(1);
        latches.add(l);
        return l;
    }

    private static class LatchedActionListener<Response> implements ActionListener<Response> {
        private final CountDownLatch latch;
        private final CopyOnWriteArrayList<Throwable> errors;

        public LatchedActionListener(CountDownLatch latch, CopyOnWriteArrayList<Throwable> errors) {
            this.latch = latch;
            this.errors = errors;
        }

        @Override
        public void onResponse(Response response) {
            latch.countDown();
        }

        @Override
        public void onFailure(Throwable e) {
            try {
                errors.add(e);
            } finally {
                latch.countDown();
            }
        }

    }

    public void clearScroll(String... scrollIds) {
        ClearScrollResponse clearResponse = client().prepareClearScroll()
                .setScrollIds(Arrays.asList(scrollIds)).get();
        assertThat(clearResponse.isSucceeded(), equalTo(true));
    }
    
    public static enum ClusterScope {
        Global, Suite, Test;
    }
    
    private ClusterScope getCurrentClusterScope() {
        SharedClusterScope annotation = this.getClass().getAnnotation(SharedClusterScope.class);
        return annotation == null ? ClusterScope.Global : annotation.scope();
    }
    
    private int getNumNodes() {
        SharedClusterScope annotation = this.getClass().getAnnotation(SharedClusterScope.class);
        return annotation == null ? -1 : annotation.numNodes();
    }
    
    
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.EMPTY;
    }
    
    protected TestCluster buildTestCluster(ClusterScope scope) {
        long currentClusterSeed = randomLong();
        Builder<Integer, Settings> ordinalMap = ImmutableMap.builder();
        int numNodes = getNumNodes();
        for (int i = 0; i < numNodes; i++) {
            ordinalMap.put(i, nodeSettings(i));
        }
        return new TestCluster(currentClusterSeed, getNumNodes(), TestCluster.clusterName(scope.name(), ElasticsearchTestCase.CHILD_VM_ID, currentClusterSeed), ordinalMap.build());
    }
    
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.TYPE})
    public @interface SharedClusterScope {
        ClusterScope scope() default ClusterScope.Global; 
        int numNodes() default -1;
    }

}
