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
package org.elasticsearch.index.shard;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.aliases.IndexAliasesService;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.bitset.ShardBitsetFilterCache;
import org.elasticsearch.index.cache.filter.ShardFilterCache;
import org.elasticsearch.index.cache.query.ShardQueryCache;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.deletionpolicy.SnapshotDeletionPolicy;
import org.elasticsearch.index.engine.EngineClosedException;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.ShadowEngine;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.fielddata.ShardFieldData;
import org.elasticsearch.index.get.ShardGetService;
import org.elasticsearch.index.indexing.ShardIndexingService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.merge.policy.MergePolicyProvider;
import org.elasticsearch.index.merge.scheduler.MergeSchedulerProvider;
import org.elasticsearch.index.percolator.PercolatorQueriesRegistry;
import org.elasticsearch.index.percolator.stats.ShardPercolateService;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.search.stats.ShardSearchService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.suggest.stats.ShardSuggestService;
import org.elasticsearch.index.termvectors.ShardTermVectorsService;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.warmer.ShardIndexWarmerService;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesWarmer;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.CountDownLatch;

/**
 * ShadowIndexShard extends {@link IndexShard} to add file synchronization
 * from the primary when a flush happens. It also ensures that a replica being
 * promoted to a primary causes the shard to fail, kicking off a re-allocation
 * of the primary shard.
 */
public class ShadowIndexShard extends IndexShard {
    private final RecoveryTarget recoveryTarget;
    private final ClusterService clusterService;

    private final Object mutex = new Object();

    @Inject
    public ShadowIndexShard(ShardId shardId, @IndexSettings Settings indexSettings, IndexSettingsService indexSettingsService,
                            IndicesLifecycle indicesLifecycle, Store store, MergeSchedulerProvider mergeScheduler,
                            Translog translog, ThreadPool threadPool, MapperService mapperService,
                            IndexQueryParserService queryParserService, IndexCache indexCache,
                            IndexAliasesService indexAliasesService, ShardIndexingService indexingService,
                            ShardGetService getService, ShardSearchService searchService,
                            ShardIndexWarmerService shardWarmerService, ShardFilterCache shardFilterCache,
                            ShardFieldData shardFieldData, PercolatorQueriesRegistry percolatorQueriesRegistry,
                            ShardPercolateService shardPercolateService, CodecService codecService,
                            ShardTermVectorsService termVectorsService, IndexFieldDataService indexFieldDataService,
                            IndexService indexService, ShardSuggestService shardSuggestService, ShardQueryCache shardQueryCache,
                            ShardBitsetFilterCache shardBitsetFilterCache, @Nullable IndicesWarmer warmer,
                            SnapshotDeletionPolicy deletionPolicy, SimilarityService similarityService,
                            MergePolicyProvider mergePolicyProvider, EngineFactory factory,
                            RecoveryTarget recoveryTarget, ClusterService clusterService) {
        super(shardId, indexSettings, indexSettingsService, indicesLifecycle, store, mergeScheduler,
                translog, threadPool, mapperService, queryParserService, indexCache, indexAliasesService,
                indexingService, getService, searchService, shardWarmerService, shardFilterCache,
                shardFieldData, percolatorQueriesRegistry, shardPercolateService, codecService,
                termVectorsService, indexFieldDataService, indexService, shardSuggestService,
                shardQueryCache, shardBitsetFilterCache, warmer, deletionPolicy, similarityService,
                mergePolicyProvider, factory);
        this.recoveryTarget = recoveryTarget;
        this.clusterService = clusterService;
    }

    /**
     * Flush the shard. In a regular {@link org.elasticsearch.index.shard.IndexShard}
     * this would usually flush the engine, however, with a shadow replica we
     * also may need to sync newly created segments (that were committed on
     * the primary) to the shadow replica
     */
    public void flush(FlushRequest request) throws ElasticsearchException {
        if (state() == IndexShardState.STARTED) {
            syncFilesFromPrimary();
        }
        super.flush(request);
    }

    private void syncFilesFromPrimary() {
        final ShardRouting shardRouting = routingEntry();
        if (IndexMetaData.isOnSharedFilesystem(indexSettings()) == false && shardRouting.primary() == false) {
            // nocommit - we are running a  full recovery here I wonder if we should do this only do this if request.waitIfOngoing() == true? Or if we need a new parameter?
            // I also wonder if we want to have an infrastructure for this instead that communicates with the primary etc?
            ClusterState state = clusterService.state();
            final CountDownLatch latch = new CountDownLatch(1);
            DiscoveryNode sourceNode = IndicesClusterStateService.findSourceNodeForPeerRecovery(state.routingTable(), state.nodes(), shardRouting, logger);
            if (sourceNode != null) {
                assert engine() instanceof ShadowEngine;
                recoveryTarget.startFileSync(this, sourceNode, new RecoveryTarget.RecoveryListener() {
                    @Override
                    public void onRecoveryDone(RecoveryState state) {
                        latch.countDown();
                        logger.info("shadow replica catchup done  {}", state);
                        // nocommit
                    }

                    @Override
                    public void onRecoveryFailure(RecoveryState state, RecoveryFailedException e, boolean sendShardFailure) {
                        latch.countDown();
                        logger.warn(" failed to catch up shadow replica can't find source node", e);
                        //nocommit
                    }
                });
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    // ignore
                }
            } else {
                logger.warn(" failed to catch up shadow replica can't find source node", shardId);
            }
        }
    }

    /**
     * In addition to the regular accounting done in
     * {@link IndexShard#routingEntry(org.elasticsearch.cluster.routing.ShardRouting)},
     * if this shadow replica needs to be promoted to a primary, the shard is
     * failed in order to allow a new primary to be re-allocated.
     */
    @Override
    public IndexShard routingEntry(ShardRouting newRouting) {
        ShardRouting shardRouting = this.routingEntry();
        super.routingEntry(newRouting);
        // check for a shadow replica that now needs to be transformed into
        // a normal primary today we simply fail it to force reallocation
        if (shardRouting != null && shardRouting.primary() == false && // currently a replica
                newRouting.primary() == true) {// becoming a primary
            failShard("can't promote shadow replica to primary",
                    new ElasticsearchIllegalStateException("can't promote shadow replica to primary"));
        }
        return this;
    }

    @Override
    protected void createNewEngine() {
        synchronized (mutex) {
            if (state == IndexShardState.CLOSED) {
                throw new EngineClosedException(shardId);
            }
            assert this.currentEngineReference.get() == null;
            assert this.shardRouting.primary() == false;
            // Use the read-only engine for shadow replicas
            this.currentEngineReference.set(engineFactory.newReadOnlyEngine(config));
        }
    }
    public void performRecoveryFinalization(boolean withFlush, RecoveryState recoveryState) throws ElasticsearchException {
        if (recoveryState.getType() == RecoveryState.Type.FILE_SYNC) {
            logger.debug("skipping recovery finalization file sync runs on a started engine");
        } else {
            super.performRecoveryFinalization(withFlush, recoveryState);
        }
    }
}
