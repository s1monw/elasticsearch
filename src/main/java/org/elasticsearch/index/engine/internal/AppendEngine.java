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
package org.elasticsearch.index.engine.internal;

import org.apache.lucene.index.IndexWriter;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.deletionpolicy.SnapshotDeletionPolicy;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.indexing.ShardIndexingService;
import org.elasticsearch.index.merge.policy.MergePolicyProvider;
import org.elasticsearch.index.merge.scheduler.MergeSchedulerProvider;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.warmer.IndicesWarmer;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;

public final class AppendEngine extends InternalEngine {

    private volatile boolean updateOnInsert = true;
    public static final String INDEX_UPDATE_ON_INSERT = "index.update_on_insert";

    @Inject
    public AppendEngine(ShardId shardId, @IndexSettings Settings indexSettings, ThreadPool threadPool, IndexSettingsService indexSettingsService, ShardIndexingService indexingService, @Nullable IndicesWarmer warmer, Store store, SnapshotDeletionPolicy deletionPolicy, Translog translog, MergePolicyProvider mergePolicyProvider, MergeSchedulerProvider mergeScheduler, AnalysisService analysisService, SimilarityService similarityService, CodecService codecService) throws EngineException {
        super(shardId, indexSettings, threadPool, indexSettingsService, indexingService, warmer, store, deletionPolicy, translog, mergePolicyProvider, mergeScheduler, analysisService, similarityService, codecService);
        this.updateOnInsert = indexSettings.getAsBoolean(INDEX_UPDATE_ON_INSERT, this.updateOnInsert);
    }

    @Override
    protected GetResult innerGet(Get get) {
        refresh(new Refresh("append_engine_get"));
        return null;
    }

    @Override
    protected void innerCreate(Create create, IndexWriter writer) throws IOException {
        if (create.docs().size() > 1) {
            writer.addDocuments(create.docs(), create.analyzer());
        } else {
            writer.addDocument(create.docs().get(0), create.analyzer());
        }
        translog.add(new Translog.Create(create));
        indexingService.postCreateUnderLock(create);
    }

    @Override
    protected void innerIndex(Index index, IndexWriter writer) throws IOException {
        if (updateOnInsert) {
            if (index.docs().size() > 1) {
                writer.updateDocuments(index.uid(), index.docs(), index.analyzer());
            } else {
                writer.updateDocument(index.uid(), index.docs().get(0), index.analyzer());
            }
            translog.add(new Translog.Index(index));
        } else {
            if (index.docs().size() > 1) {
                writer.addDocuments(index.docs(), index.analyzer());
            } else {
                writer.addDocument(index.docs().get(0), index.analyzer());
            }
            translog.add(new Translog.Create(new Create(index.docMapper(), index.uid(), index.parsedDoc())));
        }
        indexingService.postIndexUnderLock(index);
    }

    @Override
    protected void innerDelete(Delete delete, IndexWriter writer) throws IOException {
        writer.deleteDocuments(delete.uid());
        translog.add(new Translog.Delete(delete));
        indexingService.postDeleteUnderLock(delete);
    }
}
