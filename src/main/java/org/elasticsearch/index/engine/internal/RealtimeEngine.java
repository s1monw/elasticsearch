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
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.routing.operation.hash.djb.DjbHashFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.deletionpolicy.SnapshotDeletionPolicy;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.indexing.ShardIndexingService;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.merge.policy.MergePolicyProvider;
import org.elasticsearch.index.merge.scheduler.MergeSchedulerProvider;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogStreams;
import org.elasticsearch.indices.warmer.IndicesWarmer;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public class RealtimeEngine extends InternalEngine {

    private final Object[] dirtyLocks;

    // A uid (in the form of BytesRef) to the version map
    // we use the hashed variant since we iterate over it and check removal and additions on existing keys
    private final ConcurrentMap<HashedBytesRef, VersionValue> versionMap;

    @Inject
    public RealtimeEngine(ShardId shardId, @IndexSettings Settings indexSettings, ThreadPool threadPool, IndexSettingsService indexSettingsService, ShardIndexingService indexingService, @Nullable IndicesWarmer warmer, Store store, SnapshotDeletionPolicy deletionPolicy, Translog translog, MergePolicyProvider mergePolicyProvider, MergeSchedulerProvider mergeScheduler, AnalysisService analysisService, SimilarityService similarityService, CodecService codecService) throws EngineException {
        super(shardId, indexSettings, threadPool, indexSettingsService, indexingService, warmer, store, deletionPolicy, translog, mergePolicyProvider, mergeScheduler, analysisService, similarityService, codecService);
        this.versionMap = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
        this.dirtyLocks = new Object[indexConcurrency * 50]; // we multiply it to have enough...
        for (int i = 0; i < dirtyLocks.length; i++) {
            dirtyLocks[i] = new Object();
        }
    }

    @Override
    protected final GetResult innerGet(Get get) {
        if (get.realtime()) {
            VersionValue versionValue = versionMap.get(versionKey(get.uid()));
            if (versionValue != null) {
                if (versionValue.delete()) {
                    return GetResult.NOT_EXISTS;
                }
                if (get.version() != Versions.MATCH_ANY) {
                    if (get.versionType().isVersionConflict(versionValue.version(), get.version())) {
                        Uid uid = Uid.createUid(get.uid().text());
                        throw new VersionConflictEngineException(shardId, uid.type(), uid.id(), versionValue.version(), get.version());
                    }
                }
                if (!get.loadSource()) {
                    return new GetResult(true, versionValue.version(), null);
                }
                byte[] data = translog.read(versionValue.translogLocation());
                if (data != null) {
                    try {
                        Translog.Source source = TranslogStreams.readSource(data);
                        return new GetResult(true, versionValue.version(), source);
                    } catch (IOException e) {
                        // switched on us, read it from the reader
                    }
                }
            }
        }
        return null;
    }

    private Object dirtyLock(BytesRef uid) {
        int hash = DjbHashFunction.DJB_HASH(uid.bytes, uid.offset, uid.length);
        // abs returns Integer.MIN_VALUE, so we need to protect against it...
        if (hash == Integer.MIN_VALUE) {
            hash = 0;
        }
        return dirtyLocks[Math.abs(hash) % dirtyLocks.length];
    }

    private Object dirtyLock(Term uid) {
        return dirtyLock(uid.bytes());
    }

    private HashedBytesRef versionKey(Term uid) {
        return new HashedBytesRef(uid.bytes());
    }

    private long loadCurrentVersionFromIndex(Term uid) throws IOException {
        Searcher searcher = acquireSearcher("load_version");
        try {
            return Versions.loadVersion(searcher.reader(), uid);
        } finally {
            searcher.release();
        }
    }



    @Override
    protected final void innerCreate(Create create, IndexWriter writer) throws IOException {
        synchronized (dirtyLock(create.uid())) {
            HashedBytesRef versionKey = versionKey(create.uid());
            final long currentVersion;
            VersionValue versionValue = versionMap.get(versionKey);
            if (versionValue == null) {
                currentVersion = loadCurrentVersionFromIndex(create.uid());
            } else {
                if (enableGcDeletes && versionValue.delete() && (threadPool.estimatedTimeInMillis() - versionValue.time()) > gcDeletesInMillis) {
                    currentVersion = Versions.NOT_FOUND; // deleted, and GC
                } else {
                    currentVersion = versionValue.version();
                }
            }

            // same logic as index
            long updatedVersion;
            long expectedVersion = create.version();
            if (create.origin() == Operation.Origin.PRIMARY) {
                if (create.versionType().isVersionConflict(currentVersion, expectedVersion)) {
                    throw new VersionConflictEngineException(shardId, create.type(), create.id(), currentVersion, expectedVersion);
                }
                updatedVersion = create.versionType().updateVersion(currentVersion, expectedVersion);
            } else { // if (index.origin() == Operation.Origin.REPLICA || index.origin() == Operation.Origin.RECOVERY) {
                // replicas treat the version as "external" as it comes from the primary ->
                // only exploding if the version they got is lower or equal to what they know.
                if (VersionType.EXTERNAL.isVersionConflict(currentVersion, expectedVersion)) {
                    if (create.origin() == Operation.Origin.RECOVERY) {
                        return;
                    } else {
                        throw new VersionConflictEngineException(shardId, create.type(), create.id(), currentVersion, expectedVersion);
                    }
                }
                updatedVersion = VersionType.EXTERNAL.updateVersion(currentVersion, expectedVersion);
            }

            // if the doc does not exists or it exists but not delete
            if (versionValue != null) {
                if (!versionValue.delete()) {
                    if (create.origin() == Operation.Origin.RECOVERY) {
                        return;
                    } else {
                        throw new DocumentAlreadyExistsException(shardId, create.type(), create.id());
                    }
                }
            } else if (currentVersion != Versions.NOT_FOUND) {
                // its not deleted, its already there
                if (create.origin() == Operation.Origin.RECOVERY) {
                    return;
                } else {
                    throw new DocumentAlreadyExistsException(shardId, create.type(), create.id());
                }
            }

            create.version(updatedVersion);

            if (create.docs().size() > 1) {
                writer.addDocuments(create.docs(), create.analyzer());
            } else {
                writer.addDocument(create.docs().get(0), create.analyzer());
            }
            Translog.Location translogLocation = translog.add(new Translog.Create(create));

            versionMap.put(versionKey, new VersionValue(updatedVersion, false, threadPool.estimatedTimeInMillis(), translogLocation));

            indexingService.postCreateUnderLock(create);
        }
    }

    @Override
    protected final void innerIndex(Index index, IndexWriter writer) throws IOException {
        synchronized (dirtyLock(index.uid())) {
            HashedBytesRef versionKey = versionKey(index.uid());
            final long currentVersion;
            VersionValue versionValue = versionMap.get(versionKey);
            if (versionValue == null) {
                currentVersion = loadCurrentVersionFromIndex(index.uid());
            } else {
                if (enableGcDeletes && versionValue.delete() && (threadPool.estimatedTimeInMillis() - versionValue.time()) > gcDeletesInMillis) {
                    currentVersion = Versions.NOT_FOUND; // deleted, and GC
                } else {
                    currentVersion = versionValue.version();
                }
            }

            long updatedVersion;
            long expectedVersion = index.version();
            if (index.origin() == Operation.Origin.PRIMARY) {
                if (index.versionType().isVersionConflict(currentVersion, expectedVersion)) {
                    throw new VersionConflictEngineException(shardId, index.type(), index.id(), currentVersion, expectedVersion);
                }

                updatedVersion = index.versionType().updateVersion(currentVersion, expectedVersion);

            } else { // if (index.origin() == Operation.Origin.REPLICA || index.origin() == Operation.Origin.RECOVERY) {
                // replicas treat the version as "external" as it comes from the primary ->
                // only exploding if the version they got is lower or equal to what they know.
                if (VersionType.EXTERNAL.isVersionConflict(currentVersion, expectedVersion)) {
                    if (index.origin() == Operation.Origin.RECOVERY) {
                        return;
                    } else {
                        throw new VersionConflictEngineException(shardId, index.type(), index.id(), currentVersion, expectedVersion);
                    }
                }
                updatedVersion = VersionType.EXTERNAL.updateVersion(currentVersion, expectedVersion);
            }

            index.version(updatedVersion);
            if (currentVersion == Versions.NOT_FOUND) {
                // document does not exists, we can optimize for create
                index.created(true);
                if (index.docs().size() > 1) {
                    writer.addDocuments(index.docs(), index.analyzer());
                } else {
                    writer.addDocument(index.docs().get(0), index.analyzer());
                }
            } else {
                if (versionValue != null) {
                    index.created(versionValue.delete()); // we have a delete which is not GC'ed...
                }
                if (index.docs().size() > 1) {
                    writer.updateDocuments(index.uid(), index.docs(), index.analyzer());
                } else {
                    writer.updateDocument(index.uid(), index.docs().get(0), index.analyzer());
                }
            }
            Translog.Location translogLocation = translog.add(new Translog.Index(index));

            versionMap.put(versionKey, new VersionValue(updatedVersion, false, threadPool.estimatedTimeInMillis(), translogLocation));

            indexingService.postIndexUnderLock(index);
        }
    }

    @Override
    protected final void innerDelete(Delete delete, IndexWriter writer) throws IOException  {
        synchronized (dirtyLock(delete.uid())) {
            final long currentVersion;
            HashedBytesRef versionKey = versionKey(delete.uid());
            VersionValue versionValue = versionMap.get(versionKey);
            if (versionValue == null) {
                currentVersion = loadCurrentVersionFromIndex(delete.uid());
            } else {
                if (enableGcDeletes && versionValue.delete() && (threadPool.estimatedTimeInMillis() - versionValue.time()) > gcDeletesInMillis) {
                    currentVersion = Versions.NOT_FOUND; // deleted, and GC
                } else {
                    currentVersion = versionValue.version();
                }
            }

            long updatedVersion;
            long expectedVersion = delete.version();
            if (delete.origin() == Operation.Origin.PRIMARY) {
                if (delete.versionType().isVersionConflict(currentVersion, expectedVersion)) {
                    throw new VersionConflictEngineException(shardId, delete.type(), delete.id(), currentVersion, expectedVersion);
                }

                updatedVersion = delete.versionType().updateVersion(currentVersion, expectedVersion);

            } else { // if (index.origin() == Operation.Origin.REPLICA || index.origin() == Operation.Origin.RECOVERY) {
                // replicas treat the version as "external" as it comes from the primary ->
                // only exploding if the version they got is lower or equal to what they know.
                if (VersionType.EXTERNAL.isVersionConflict(currentVersion, expectedVersion)) {
                    if (delete.origin() == Operation.Origin.RECOVERY) {
                        return;
                    } else {
                        throw new VersionConflictEngineException(shardId, delete.type(), delete.id(), currentVersion - 1, expectedVersion);
                    }
                }
                updatedVersion = VersionType.EXTERNAL.updateVersion(currentVersion, expectedVersion);
            }

            if (currentVersion == Versions.NOT_FOUND) {
                // doc does not exists and no prior deletes
                delete.version(updatedVersion).found(false);
                Translog.Location translogLocation = translog.add(new Translog.Delete(delete));
                versionMap.put(versionKey, new VersionValue(updatedVersion, true, threadPool.estimatedTimeInMillis(), translogLocation));
            } else if (versionValue != null && versionValue.delete()) {
                // a "delete on delete", in this case, we still increment the version, log it, and return that version
                delete.version(updatedVersion).found(false);
                Translog.Location translogLocation = translog.add(new Translog.Delete(delete));
                versionMap.put(versionKey, new VersionValue(updatedVersion, true, threadPool.estimatedTimeInMillis(), translogLocation));
            } else {
                delete.version(updatedVersion).found(true);
                writer.deleteDocuments(delete.uid());
                Translog.Location translogLocation = translog.add(new Translog.Delete(delete));
                versionMap.put(versionKey, new VersionValue(updatedVersion, true, threadPool.estimatedTimeInMillis(), translogLocation));
            }

            indexingService.postDeleteUnderLock(delete);
        }
    }

    private void refreshVersioningTable(long time) {
        // we need to refresh in order to clear older version values
        refresh(new Refresh("version_table").force(true));
        for (Map.Entry<HashedBytesRef, VersionValue> entry : versionMap.entrySet()) {
            HashedBytesRef uid = entry.getKey();
            synchronized (dirtyLock(uid.bytes)) { // can we do it without this lock on each value? maybe batch to a set and get the lock once per set?
                VersionValue versionValue = versionMap.get(uid);
                if (versionValue == null) {
                    continue;
                }
                if (time - versionValue.time() <= 0) {
                    continue; // its a newer value, from after/during we refreshed, don't clear it
                }
                if (versionValue.delete()) {
                    if (enableGcDeletes && (time - versionValue.time()) > gcDeletesInMillis) {
                        versionMap.remove(uid);
                    }
                } else {
                    versionMap.remove(uid);
                }
            }
        }
    }

    protected final void onFlushSuccess(Flush.Type type) {
        if (type != Flush.Type.COMMIT) {
            refreshVersioningTable(threadPool.estimatedTimeInMillis());
        }
    }

    @Override
    protected final void innerClose() {
        super.innerClose();
        this.versionMap.clear();
    }

    @Override
    public final void delete(DeleteByQuery delete) throws EngineException {
        super.delete(delete);
        //TODO: This is heavy, since we refresh, but we really have to...
        refreshVersioningTable(System.currentTimeMillis());
    }

}
