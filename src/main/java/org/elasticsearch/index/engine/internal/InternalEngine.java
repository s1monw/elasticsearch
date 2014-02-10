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

import com.google.common.collect.Lists;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexWriter.IndexReaderWarmer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.XSearcherManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.cluster.routing.operation.hash.djb.DjbHashFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.SegmentReaderUtils;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.deletionpolicy.SnapshotDeletionPolicy;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.engine.*;
import org.elasticsearch.index.indexing.ShardIndexingService;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.index.merge.policy.IndexUpgraderMergePolicy;
import org.elasticsearch.index.merge.policy.MergePolicyProvider;
import org.elasticsearch.index.merge.scheduler.MergeSchedulerProvider;
import org.elasticsearch.index.search.nested.IncludeNestedDocsQuery;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogStreams;
import org.elasticsearch.indices.warmer.IndicesWarmer;
import org.elasticsearch.indices.warmer.InternalIndicesWarmer;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 */
public abstract class InternalEngine extends AbstractIndexShardComponent implements Engine {

    private volatile ByteSizeValue indexingBufferSize;
    protected volatile int indexConcurrency;
    private volatile boolean compoundOnFlush = true;

    protected long gcDeletesInMillis;
    protected volatile boolean enableGcDeletes = true;
    private volatile String codecName;

    protected final ThreadPool threadPool;

    protected final ShardIndexingService indexingService;
    private final IndexSettingsService indexSettingsService;
    @Nullable
    private final InternalIndicesWarmer warmer;
    private final Store store;
    private final SnapshotDeletionPolicy deletionPolicy;
    protected final Translog translog;
    private final MergePolicyProvider mergePolicyProvider;
    private final MergeSchedulerProvider mergeScheduler;
    private final AnalysisService analysisService;
    private final SimilarityService similarityService;
    private final CodecService codecService;


    private final ReadWriteLock rwl = new ReentrantReadWriteLock();

    private volatile IndexWriter indexWriter;

    private final SearcherFactory searcherFactory = new SearchFactory();
    private volatile XSearcherManager searcherManager;

    private volatile boolean closed = false;

    // flag indicating if a dirty operation has occurred since the last refresh
    private volatile boolean dirty = false;

    private volatile boolean possibleMergeNeeded = false;

    private final AtomicBoolean optimizeMutex = new AtomicBoolean();
    // we use flushNeeded here, since if there are no changes, then the commit won't write
    // will not really happen, and then the commitUserData and the new translog will not be reflected
    private volatile boolean flushNeeded = false;
    private final AtomicInteger flushing = new AtomicInteger();
    private final Lock flushLock = new ReentrantLock();

    private final RecoveryCounter onGoingRecoveries = new RecoveryCounter();

    private final Object refreshMutex = new Object();

    private final ApplySettings applySettings = new ApplySettings();

    private volatile boolean failOnMergeFailure;
    private Throwable failedEngine = null;
    private final Object failedEngineMutex = new Object();
    private final CopyOnWriteArrayList<FailedEngineListener> failedEngineListeners = new CopyOnWriteArrayList<FailedEngineListener>();

    private final AtomicLong translogIdGenerator = new AtomicLong();

    private SegmentInfos lastCommittedSegmentInfos;

    protected InternalEngine(ShardId shardId, @IndexSettings Settings indexSettings, ThreadPool threadPool,
                          IndexSettingsService indexSettingsService, ShardIndexingService indexingService, @Nullable IndicesWarmer warmer,
                          Store store, SnapshotDeletionPolicy deletionPolicy, Translog translog,
                          MergePolicyProvider mergePolicyProvider, MergeSchedulerProvider mergeScheduler,
                          AnalysisService analysisService, SimilarityService similarityService, CodecService codecService) throws EngineException {
        super(shardId, indexSettings);
        Preconditions.checkNotNull(store, "Store must be provided to the engine");
        Preconditions.checkNotNull(deletionPolicy, "Snapshot deletion policy must be provided to the engine");
        Preconditions.checkNotNull(translog, "Translog must be provided to the engine");

        this.gcDeletesInMillis = indexSettings.getAsTime(INDEX_GC_DELETES, TimeValue.timeValueSeconds(60)).millis();
        this.indexingBufferSize = componentSettings.getAsBytesSize("index_buffer_size", new ByteSizeValue(64, ByteSizeUnit.MB)); // not really important, as it is set by the IndexingMemory manager
        this.codecName = indexSettings.get(INDEX_CODEC, "default");

        this.threadPool = threadPool;
        this.indexSettingsService = indexSettingsService;
        this.indexingService = indexingService;
        this.warmer = (InternalIndicesWarmer) warmer;
        this.store = store;
        this.deletionPolicy = deletionPolicy;
        this.translog = translog;
        this.mergePolicyProvider = mergePolicyProvider;
        this.mergeScheduler = mergeScheduler;
        this.analysisService = analysisService;
        this.similarityService = similarityService;
        this.codecService = codecService;
        this.compoundOnFlush = indexSettings.getAsBoolean(INDEX_COMPOUND_ON_FLUSH, this.compoundOnFlush);
        this.indexConcurrency = indexSettings.getAsInt(INDEX_INDEX_CONCURRENCY, Math.max(IndexWriterConfig.DEFAULT_MAX_THREAD_STATES, (int) (EsExecutors.boundedNumberOfProcessors(indexSettings) * 0.65)));
        this.indexSettingsService.addListener(applySettings);

        this.failOnMergeFailure = indexSettings.getAsBoolean(INDEX_FAIL_ON_MERGE_FAILURE, true);
        if (failOnMergeFailure) {
            this.mergeScheduler.addFailureListener(new FailEngineOnMergeFailure());
        }
    }

    @Override
    public void updateIndexingBufferSize(ByteSizeValue indexingBufferSize) {
        ByteSizeValue preValue = this.indexingBufferSize;
        rwl.readLock().lock();
        try {
            this.indexingBufferSize = indexingBufferSize;
            IndexWriter indexWriter = this.indexWriter;
            if (indexWriter != null) {
                indexWriter.getConfig().setRAMBufferSizeMB(this.indexingBufferSize.mbFrac());
            }
        } finally {
            rwl.readLock().unlock();
        }
        if (preValue.bytes() != indexingBufferSize.bytes()) {
            // its inactive, make sure we do a full flush in this case, since the memory
            // changes only after a "data" change has happened to the writer
            if (indexingBufferSize == Engine.INACTIVE_SHARD_INDEXING_BUFFER && preValue != Engine.INACTIVE_SHARD_INDEXING_BUFFER) {
                logger.debug("updating index_buffer_size from [{}] to (inactive) [{}]", preValue, indexingBufferSize);
                try {
                    flush(new Flush().type(Flush.Type.NEW_WRITER));
                } catch (EngineClosedException e) {
                    // ignore
                } catch (FlushNotAllowedEngineException e) {
                    // ignore
                } catch (Throwable e) {
                    logger.warn("failed to flush after setting shard to inactive", e);
                }
            } else {
                logger.debug("updating index_buffer_size from [{}] to [{}]", preValue, indexingBufferSize);
            }
        }
    }

    @Override
    public void addFailedEngineListener(FailedEngineListener listener) {
        failedEngineListeners.add(listener);
    }

    @Override
    public void start() throws EngineException {
        rwl.writeLock().lock();
        try {
            if (indexWriter != null) {
                throw new EngineAlreadyStartedException(shardId);
            }
            if (closed) {
                throw new EngineClosedException(shardId);
            }
            if (logger.isDebugEnabled()) {
                logger.debug("starting engine");
            }
            try {
                this.indexWriter = createWriter();
            } catch (IOException e) {
                throw new EngineCreationFailureException(shardId, "failed to create engine", e);
            }

            try {
                // commit on a just opened writer will commit even if there are no changes done to it
                // we rely on that for the commit data translog id key
                if (Lucene.indexExists(store.directory())) {
                    Map<String, String> commitUserData = Lucene.readSegmentInfos(store.directory()).getUserData();
                    if (commitUserData.containsKey(Translog.TRANSLOG_ID_KEY)) {
                        translogIdGenerator.set(Long.parseLong(commitUserData.get(Translog.TRANSLOG_ID_KEY)));
                    } else {
                        translogIdGenerator.set(System.currentTimeMillis());
                        indexWriter.setCommitData(MapBuilder.<String, String>newMapBuilder().put(Translog.TRANSLOG_ID_KEY, Long.toString(translogIdGenerator.get())).map());
                        indexWriter.commit();
                    }
                } else {
                    translogIdGenerator.set(System.currentTimeMillis());
                    indexWriter.setCommitData(MapBuilder.<String, String>newMapBuilder().put(Translog.TRANSLOG_ID_KEY, Long.toString(translogIdGenerator.get())).map());
                    indexWriter.commit();
                }
                translog.newTranslog(translogIdGenerator.get());
                this.searcherManager = buildSearchManager(indexWriter);
                readLastCommittedSegmentsInfo();
            } catch (IOException e) {
                try {
                    indexWriter.rollback();
                } catch (IOException e1) {
                    // ignore
                } finally {
                    IOUtils.closeWhileHandlingException(indexWriter);
                }
                throw new EngineCreationFailureException(shardId, "failed to open reader on writer", e);
            }
        } finally {
            rwl.writeLock().unlock();
        }
    }

    private void readLastCommittedSegmentsInfo() throws IOException {
        SegmentInfos infos = new SegmentInfos();
        infos.read(store.directory());
        lastCommittedSegmentInfos = infos;
    }

    @Override
    public TimeValue defaultRefreshInterval() {
        return new TimeValue(1, TimeUnit.SECONDS);
    }

    @Override
    public void enableGcDeletes(boolean enableGcDeletes) {
        this.enableGcDeletes = enableGcDeletes;
    }

    public GetResult get(Get get) throws EngineException {
        rwl.readLock().lock();
        try {
            GetResult innerResult = innerGet(get);
            if (innerResult != null) {
                return innerResult;
            }
            // no version, get the version from the index, we know that we refresh on flush
            Searcher searcher = acquireSearcher("get");
            final Versions.DocIdAndVersion docIdAndVersion;
            try {
                docIdAndVersion = Versions.loadDocIdAndVersion(searcher.reader(), get.uid());
            } catch (Throwable e) {
                searcher.release();
                //TODO: A better exception goes here
                throw new EngineException(shardId(), "Couldn't resolve version", e);
            }

            if (get.version() != Versions.MATCH_ANY && docIdAndVersion != null) {
                if (get.versionType().isVersionConflict(docIdAndVersion.version, get.version())) {
                    searcher.release();
                    Uid uid = Uid.createUid(get.uid().text());
                    throw new VersionConflictEngineException(shardId, uid.type(), uid.id(), docIdAndVersion.version, get.version());
                }
            }

            if (docIdAndVersion != null) {
                // don't release the searcher on this path, it is the responsability of the caller to call GetResult.release
                return new GetResult(searcher, docIdAndVersion);
            } else {
                searcher.release();
                return GetResult.NOT_EXISTS;
            }

        } finally {
            rwl.readLock().unlock();
        }
    }

    protected abstract GetResult innerGet(Get get);

    @Override
    public void create(Create create) throws EngineException {
        rwl.readLock().lock();
        try {
            IndexWriter writer = this.indexWriter;
            if (writer == null) {
                throw new EngineClosedException(shardId, failedEngine);
            }
            innerCreate(create, writer);
            dirty = true;
            possibleMergeNeeded = true;
            flushNeeded = true;
        } catch (IOException e) {
            throw new CreateFailedEngineException(shardId, create, e);
        } catch (OutOfMemoryError e) {
            failEngine(e);
            throw new CreateFailedEngineException(shardId, create, e);
        } catch (IllegalStateException e) {
            if (e.getMessage().contains("OutOfMemoryError")) {
                failEngine(e);
            }
            throw new CreateFailedEngineException(shardId, create, e);
        } finally {
            rwl.readLock().unlock();
        }
    }

    protected abstract void innerCreate(Create create, IndexWriter writer) throws IOException;

    @Override
    public void index(Index index) throws EngineException {
        rwl.readLock().lock();
        try {
            IndexWriter writer = this.indexWriter;
            if (writer == null) {
                throw new EngineClosedException(shardId, failedEngine);
            }

            innerIndex(index, writer);
            dirty = true;
            possibleMergeNeeded = true;
            flushNeeded = true;
        } catch (IOException e) {
            throw new IndexFailedEngineException(shardId, index, e);
        } catch (OutOfMemoryError e) {
            failEngine(e);
            throw new IndexFailedEngineException(shardId, index, e);
        } catch (IllegalStateException e) {
            if (e.getMessage().contains("OutOfMemoryError")) {
                failEngine(e);
            }
            throw new IndexFailedEngineException(shardId, index, e);
        } finally {
            rwl.readLock().unlock();
        }
    }

    protected abstract void innerIndex(Index index, IndexWriter writer) throws IOException;

    @Override
    public void delete(Delete delete) throws EngineException {
        rwl.readLock().lock();
        try {
            IndexWriter writer = this.indexWriter;
            if (writer == null) {
                throw new EngineClosedException(shardId, failedEngine);
            }
            innerDelete(delete, writer);
            dirty = true;
            possibleMergeNeeded = true;
            flushNeeded = true;
        } catch (IOException e) {
            throw new DeleteFailedEngineException(shardId, delete, e);
        } catch (OutOfMemoryError e) {
            failEngine(e);
            throw new DeleteFailedEngineException(shardId, delete, e);
        } catch (IllegalStateException e) {
            if (e.getMessage().contains("OutOfMemoryError")) {
                failEngine(e);
            }
            throw new DeleteFailedEngineException(shardId, delete, e);
        } finally {
            rwl.readLock().unlock();
        }
    }

    protected abstract void innerDelete(Delete delete, IndexWriter writer) throws IOException;

    @Override
    public void delete(DeleteByQuery delete) throws EngineException {
        rwl.readLock().lock();
        try {
            IndexWriter writer = this.indexWriter;
            if (writer == null) {
                throw new EngineClosedException(shardId);
            }

            Query query;
            if (delete.nested() && delete.aliasFilter() != null) {
                query = new IncludeNestedDocsQuery(new XFilteredQuery(delete.query(), delete.aliasFilter()), delete.parentFilter());
            } else if (delete.nested()) {
                query = new IncludeNestedDocsQuery(delete.query(), delete.parentFilter());
            } else if (delete.aliasFilter() != null) {
                query = new XFilteredQuery(delete.query(), delete.aliasFilter());
            } else {
                query = delete.query();
            }

            writer.deleteDocuments(query);
            translog.add(new Translog.DeleteByQuery(delete));
            dirty = true;
            possibleMergeNeeded = true;
            flushNeeded = true;
        } catch (IOException e) {
            throw new DeleteByQueryFailedEngineException(shardId, delete, e);
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override
    public final Searcher acquireSearcher(String source) throws EngineException {
        XSearcherManager manager = this.searcherManager;
        if (manager == null) {
            throw new EngineClosedException(shardId);
        }
        try {
            IndexSearcher searcher = manager.acquire();
            return newSearcher(source, searcher, manager);
        } catch (Throwable ex) {
            logger.error("failed to acquire searcher, source {}", ex, source);
            throw new EngineException(shardId, ex.getMessage());
        }
    }

    protected Searcher newSearcher(String source, IndexSearcher searcher, XSearcherManager manager) {
        return new EngineSearcher(source, searcher, manager);
    }

    @Override
    public boolean refreshNeeded() {
        return dirty;
    }

    @Override
    public boolean possibleMergeNeeded() {
        return this.possibleMergeNeeded;
    }

    @Override
    public void refresh(Refresh refresh) throws EngineException {
        if (indexWriter == null) {
            throw new EngineClosedException(shardId);
        }
        // we obtain a read lock here, since we don't want a flush to happen while we are refreshing
        // since it flushes the index as well (though, in terms of concurrency, we are allowed to do it)
        rwl.readLock().lock();
        try {
            // this engine always acts as if waitForOperations=true
            IndexWriter currentWriter = indexWriter;
            if (currentWriter == null) {
                throw new EngineClosedException(shardId, failedEngine);
            }
            try {
                // maybeRefresh will only allow one refresh to execute, and the rest will "pass through",
                // but, we want to make sure not to loose ant refresh calls, if one is taking time
                synchronized (refreshMutex) {
                    if (dirty || refresh.force()) {
                        dirty = false;
                        searcherManager.maybeRefresh();
                    }
                }
            } catch (AlreadyClosedException e) {
                // an index writer got replaced on us, ignore
            } catch (OutOfMemoryError e) {
                failEngine(e);
                throw new RefreshFailedEngineException(shardId, e);
            } catch (IllegalStateException e) {
                if (e.getMessage().contains("OutOfMemoryError")) {
                    failEngine(e);
                }
                throw new RefreshFailedEngineException(shardId, e);
            } catch (Throwable e) {
                if (indexWriter == null) {
                    throw new EngineClosedException(shardId, failedEngine);
                } else if (currentWriter != indexWriter) {
                    // an index writer got replaced on us, ignore
                } else {
                    throw new RefreshFailedEngineException(shardId, e);
                }
            }
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override
    public void flush(Flush flush) throws EngineException {
        ensureOpen();
        if (flush.type() == Flush.Type.NEW_WRITER || flush.type() == Flush.Type.COMMIT_TRANSLOG) {
            // check outside the lock as well so we can check without blocking on the write lock
            if (onGoingRecoveries.get() > 0) {
                throw new FlushNotAllowedEngineException(shardId, "recovery is in progress, flush [" + flush.type() + "] is not allowed");
            }
        }
        int currentFlushing = flushing.incrementAndGet();
        if (currentFlushing > 1 && !flush.waitIfOngoing()) {
            flushing.decrementAndGet();
            throw new FlushNotAllowedEngineException(shardId, "already flushing...");
        }

        flushLock.lock();
        try {
            if (flush.type() == Flush.Type.NEW_WRITER) {
                rwl.writeLock().lock();
                try {
                    ensureOpen();
                    if (onGoingRecoveries.get() > 0) {
                        throw new FlushNotAllowedEngineException(shardId, "Recovery is in progress, flush is not allowed");
                    }
                    // disable refreshing, not dirty
                    dirty = false;
                    try {
                        // that's ok if the index writer failed and is in inconsistent state
                        // we will get an exception on a dirty operation, and will cause the shard
                        // to be allocated to a different node
                        indexWriter.close(false);
                        indexWriter = createWriter();

                        // commit on a just opened writer will commit even if there are no changes done to it
                        // we rely on that for the commit data translog id key
                        if (flushNeeded || flush.force()) {
                            flushNeeded = false;
                            long translogId = translogIdGenerator.incrementAndGet();
                            indexWriter.setCommitData(MapBuilder.<String, String>newMapBuilder().put(Translog.TRANSLOG_ID_KEY, Long.toString(translogId)).map());
                            indexWriter.commit();
                            translog.newTranslog(translogId);
                        }

                        XSearcherManager current = this.searcherManager;
                        this.searcherManager = buildSearchManager(indexWriter);
                        try {
                            IOUtils.close(current);
                        } catch (Throwable t) {
                            logger.warn("Failed to close current SearcherManager", t);
                        }
                        onFlushSuccess(Flush.Type.NEW_WRITER);
                    } catch (OutOfMemoryError e) {
                        failEngine(e);
                        throw new FlushFailedEngineException(shardId, e);
                    } catch (IllegalStateException e) {
                        if (e.getMessage().contains("OutOfMemoryError")) {
                            failEngine(e);
                        }
                        throw new FlushFailedEngineException(shardId, e);
                    } catch (Throwable e) {
                        throw new FlushFailedEngineException(shardId, e);
                    }
                } finally {
                    rwl.writeLock().unlock();
                }
            } else if (flush.type() == Flush.Type.COMMIT_TRANSLOG) {
                rwl.readLock().lock();
                try {
                    ensureOpen();
                    if (onGoingRecoveries.get() > 0) {
                        throw new FlushNotAllowedEngineException(shardId, "Recovery is in progress, flush is not allowed");
                    }

                    if (flushNeeded || flush.force()) {
                        flushNeeded = false;
                        try {
                            long translogId = translogIdGenerator.incrementAndGet();
                            translog.newTransientTranslog(translogId);
                            indexWriter.setCommitData(MapBuilder.<String, String>newMapBuilder().put(Translog.TRANSLOG_ID_KEY, Long.toString(translogId)).map());
                            indexWriter.commit();
                            onFlushSuccess(Flush.Type.COMMIT_TRANSLOG);
                            // we need to move transient to current only after we refresh
                            // so items added to current will still be around for realtime get
                            // when tans overrides it
                            translog.makeTransientCurrent();
                        } catch (OutOfMemoryError e) {
                            translog.revertTransient();
                            failEngine(e);
                            throw new FlushFailedEngineException(shardId, e);
                        } catch (IllegalStateException e) {
                            if (e.getMessage().contains("OutOfMemoryError")) {
                                failEngine(e);
                            }
                            throw new FlushFailedEngineException(shardId, e);
                        } catch (Throwable e) {
                            translog.revertTransient();
                            throw new FlushFailedEngineException(shardId, e);
                        }
                    }
                } finally {
                    rwl.readLock().unlock();
                }
            } else if (flush.type() == Flush.Type.COMMIT) {
                // note, its ok to just commit without cleaning the translog, its perfectly fine to replay a
                // translog on an index that was opened on a committed point in time that is "in the future"
                // of that translog
                rwl.readLock().lock();
                try {
                    ensureOpen();
                    // we allow to *just* commit if there is an ongoing recovery happening...
                    // its ok to use this, only a flush will cause a new translogId, and we are locked here from
                    // other flushes use flushLock
                    try {
                        long translogId = translog.currentId();
                        indexWriter.setCommitData(MapBuilder.<String, String>newMapBuilder().put(Translog.TRANSLOG_ID_KEY, Long.toString(translogId)).map());
                        indexWriter.commit();
                        onFlushSuccess(Flush.Type.COMMIT);
                    } catch (OutOfMemoryError e) {
                        translog.revertTransient();
                        failEngine(e);
                        throw new FlushFailedEngineException(shardId, e);
                    } catch (IllegalStateException e) {
                        if (e.getMessage().contains("OutOfMemoryError")) {
                            failEngine(e);
                        }
                        throw new FlushFailedEngineException(shardId, e);
                    } catch (Throwable e) {
                        throw new FlushFailedEngineException(shardId, e);
                    }
                } finally {
                    rwl.readLock().unlock();
                }
            } else {
                throw new ElasticsearchIllegalStateException("flush type [" + flush.type() + "] not supported");
            }

            // reread the last committed segment infos
            rwl.readLock().lock();
            try {
                ensureOpen();
                readLastCommittedSegmentsInfo();
            } catch (Throwable e) {
                if (!closed) {
                    logger.warn("failed to read latest segment infos on flush", e);
                }
            } finally {
                rwl.readLock().unlock();
            }

        } finally {
            flushLock.unlock();
            flushing.decrementAndGet();
        }
    }

    protected void onFlushSuccess(Flush.Type type) {
        //noop
    }

    private void ensureOpen() {
        if (indexWriter == null) {
            throw new EngineClosedException(shardId, failedEngine);
        }
    }

    @Override
    public void maybeMerge() throws EngineException {
        if (!possibleMergeNeeded) {
            return;
        }
        possibleMergeNeeded = false;
        rwl.readLock().lock();
        try {
            ensureOpen();
            indexWriter.maybeMerge();
        } catch (OutOfMemoryError e) {
            failEngine(e);
            throw new OptimizeFailedEngineException(shardId, e);
        } catch (IllegalStateException e) {
            if (e.getMessage().contains("OutOfMemoryError")) {
                failEngine(e);
            }
            throw new OptimizeFailedEngineException(shardId, e);
        } catch (Throwable e) {
            throw new OptimizeFailedEngineException(shardId, e);
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override
    public void optimize(Optimize optimize) throws EngineException {
        if (optimize.flush()) {
            flush(new Flush().force(true).waitIfOngoing(true));
        }
        if (optimizeMutex.compareAndSet(false, true)) {
            rwl.readLock().lock();
            try {
                ensureOpen();
                if (optimize.onlyExpungeDeletes()) {
                    indexWriter.forceMergeDeletes(false);
                } else if (optimize.maxNumSegments() <= 0) {
                    indexWriter.maybeMerge();
                    possibleMergeNeeded = false;
                } else {
                    indexWriter.forceMerge(optimize.maxNumSegments(), false);
                }
            } catch (OutOfMemoryError e) {
                failEngine(e);
                throw new OptimizeFailedEngineException(shardId, e);
            } catch (IllegalStateException e) {
                if (e.getMessage().contains("OutOfMemoryError")) {
                    failEngine(e);
                }
                throw new OptimizeFailedEngineException(shardId, e);
            } catch (Throwable e) {
                throw new OptimizeFailedEngineException(shardId, e);
            } finally {
                rwl.readLock().unlock();
                optimizeMutex.set(false);
            }
        }
        // wait for the merges outside of the read lock
        if (optimize.waitForMerge()) {
            indexWriter.waitForMerges();
        }
        if (optimize.flush()) {
            flush(new Flush().force(true).waitIfOngoing(true));
        }
    }

    @Override
    public <T> T snapshot(SnapshotHandler<T> snapshotHandler) throws EngineException {
        SnapshotIndexCommit snapshotIndexCommit = null;
        Translog.Snapshot traslogSnapshot = null;
        rwl.readLock().lock();
        try {
            snapshotIndexCommit = deletionPolicy.snapshot();
            traslogSnapshot = translog.snapshot();
        } catch (Throwable e) {
            if (snapshotIndexCommit != null) {
                snapshotIndexCommit.release();
            }
            throw new SnapshotFailedEngineException(shardId, e);
        } finally {
            rwl.readLock().unlock();
        }

        try {
            return snapshotHandler.snapshot(snapshotIndexCommit, traslogSnapshot);
        } finally {
            snapshotIndexCommit.release();
            traslogSnapshot.release();
        }
    }

    @Override
    public SnapshotIndexCommit snapshotIndex() throws EngineException {
        rwl.readLock().lock();
        try {
            flush(new Flush().type(Flush.Type.COMMIT).waitIfOngoing(true));
            ensureOpen();
            return deletionPolicy.snapshot();
        } catch (IOException e) {
            throw new SnapshotFailedEngineException(shardId, e);
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override
    public void recover(RecoveryHandler recoveryHandler) throws EngineException {
        // take a write lock here so it won't happen while a flush is in progress
        // this means that next commits will not be allowed once the lock is released
        rwl.writeLock().lock();
        try {
            if (closed) {
                throw new EngineClosedException(shardId);
            }
            onGoingRecoveries.increment();
        } finally {
            rwl.writeLock().unlock();
        }

        SnapshotIndexCommit phase1Snapshot;
        try {
            phase1Snapshot = deletionPolicy.snapshot();
        } catch (Throwable e) {
            onGoingRecoveries.decrement();
            throw new RecoveryEngineException(shardId, 1, "Snapshot failed", e);
        }

        try {
            recoveryHandler.phase1(phase1Snapshot);
        } catch (Throwable e) {
            onGoingRecoveries.decrement();
            phase1Snapshot.release();
            if (closed) {
                e = new EngineClosedException(shardId, e);
            }
            throw new RecoveryEngineException(shardId, 1, "Execution failed", e);
        }

        Translog.Snapshot phase2Snapshot;
        try {
            phase2Snapshot = translog.snapshot();
        } catch (Throwable e) {
            onGoingRecoveries.decrement();
            phase1Snapshot.release();
            if (closed) {
                e = new EngineClosedException(shardId, e);
            }
            throw new RecoveryEngineException(shardId, 2, "Snapshot failed", e);
        }

        try {
            recoveryHandler.phase2(phase2Snapshot);
        } catch (Throwable e) {
            onGoingRecoveries.decrement();
            phase1Snapshot.release();
            phase2Snapshot.release();
            if (closed) {
                e = new EngineClosedException(shardId, e);
            }
            throw new RecoveryEngineException(shardId, 2, "Execution failed", e);
        }

        rwl.writeLock().lock();
        Translog.Snapshot phase3Snapshot = null;
        try {
            phase3Snapshot = translog.snapshot(phase2Snapshot);
            recoveryHandler.phase3(phase3Snapshot);
        } catch (Throwable e) {
            throw new RecoveryEngineException(shardId, 3, "Execution failed", e);
        } finally {
            onGoingRecoveries.decrement();
            rwl.writeLock().unlock();
            phase1Snapshot.release();
            phase2Snapshot.release();
            if (phase3Snapshot != null) {
                phase3Snapshot.release();
            }
        }
    }


    private long getReaderRamBytesUsed(AtomicReaderContext reader) {
        return SegmentReaderUtils.segmentReader(reader.reader()).ramBytesUsed();
    }

    @Override
    public SegmentsStats segmentsStats() {
        rwl.readLock().lock();
        try {
            ensureOpen();
            Searcher searcher = acquireSearcher("segments_stats");
            try {
                SegmentsStats stats = new SegmentsStats();
                for (AtomicReaderContext reader : searcher.reader().leaves()) {
                    stats.add(1, getReaderRamBytesUsed(reader));
                }
                return stats;
            } finally {
                searcher.release();
            }
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override
    public List<Segment> segments() {
        rwl.readLock().lock();
        try {
            ensureOpen();
            Map<String, Segment> segments = new HashMap<String, Segment>();

            // first, go over and compute the search ones...
            Searcher searcher = acquireSearcher("segments");
            try {
                for (AtomicReaderContext reader : searcher.reader().leaves()) {
                    assert reader.reader() instanceof SegmentReader;
                    SegmentCommitInfo info = SegmentReaderUtils.segmentReader(reader.reader()).getSegmentInfo();
                    assert !segments.containsKey(info.info.name);
                    Segment segment = new Segment(info.info.name);
                    segment.search = true;
                    segment.docCount = reader.reader().numDocs();
                    segment.delDocCount = reader.reader().numDeletedDocs();
                    segment.version = info.info.getVersion();
                    segment.compound = info.info.getUseCompoundFile();
                    try {
                        segment.sizeInBytes = info.sizeInBytes();
                    } catch (IOException e) {
                        logger.trace("failed to get size for [{}]", e, info.info.name);
                    }
                    segment.memoryInBytes = getReaderRamBytesUsed(reader);
                    segments.put(info.info.name, segment);
                }
            } finally {
                searcher.release();
            }

            // now, correlate or add the committed ones...
            if (lastCommittedSegmentInfos != null) {
                SegmentInfos infos = lastCommittedSegmentInfos;
                for (SegmentCommitInfo info : infos) {
                    Segment segment = segments.get(info.info.name);
                    if (segment == null) {
                        segment = new Segment(info.info.name);
                        segment.search = false;
                        segment.committed = true;
                        segment.docCount = info.info.getDocCount();
                        segment.delDocCount = info.getDelCount();
                        segment.version = info.info.getVersion();
                        segment.compound = info.info.getUseCompoundFile();
                        try {
                            segment.sizeInBytes = info.sizeInBytes();
                        } catch (IOException e) {
                            logger.trace("failed to get size for [{}]", e, info.info.name);
                        }
                        segments.put(info.info.name, segment);
                    } else {
                        segment.committed = true;
                    }
                }
            }

            Segment[] segmentsArr = segments.values().toArray(new Segment[segments.values().size()]);
            Arrays.sort(segmentsArr, new Comparator<Segment>() {
                @Override
                public int compare(Segment o1, Segment o2) {
                    return (int) (o1.getGeneration() - o2.getGeneration());
                }
            });

            // fill in the merges flag
            Set<OnGoingMerge> onGoingMerges = mergeScheduler.onGoingMerges();
            for (OnGoingMerge onGoingMerge : onGoingMerges) {
                for (SegmentCommitInfo segmentInfoPerCommit : onGoingMerge.getMergedSegments()) {
                    for (Segment segment : segmentsArr) {
                        if (segment.getName().equals(segmentInfoPerCommit.info.name)) {
                            segment.mergeId = onGoingMerge.getId();
                            break;
                        }
                    }
                }
            }

            return Arrays.asList(segmentsArr);
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override
    public void close() throws ElasticsearchException {
        rwl.writeLock().lock();
        try {
            innerClose();
        } finally {
            rwl.writeLock().unlock();
        }
        try {
            // wait for recoveries to join and close all resources / IO streams
            int ongoingRecoveries = onGoingRecoveries.awaitNoRecoveries(5000);
            if (ongoingRecoveries > 0) {
                logger.debug("Waiting for ongoing recoveries timed out on close currently ongoing disoveries: [{}]", ongoingRecoveries);
            }
        } catch (InterruptedException e) {
            // ignore & restore interrupt
            Thread.currentThread().interrupt();
        }

    }

    class FailEngineOnMergeFailure implements MergeSchedulerProvider.FailureListener {
        @Override
        public void onFailedMerge(MergePolicy.MergeException e) {
            failEngine(e);
        }
    }

    private void failEngine(Throwable failure) {
        synchronized (failedEngineMutex) {
            if (failedEngine != null) {
                return;
            }
            logger.warn("failed engine", failure);
            failedEngine = failure;
            for (FailedEngineListener listener : failedEngineListeners) {
                listener.onFailedEngine(shardId, failure);
            }
            innerClose();
        }
    }

    protected void innerClose() {
        if (closed) {
            return;
        }
        indexSettingsService.removeListener(applySettings);
        closed = true;
        this.failedEngineListeners.clear();
        try {
            try {
                IOUtils.close(searcherManager);
            } catch (Throwable t) {
                logger.warn("Failed to close SearcherManager", t);
            }
            // no need to commit in this case!, we snapshot before we close the shard, so translog and all sync'ed
            if (indexWriter != null) {
                try {
                    indexWriter.rollback();
                } catch (AlreadyClosedException e) {
                    // ignore
                }
            }
        } catch (Throwable e) {
            logger.debug("failed to rollback writer on close", e);
        } finally {
            indexWriter = null;
        }
    }

    /**
     * Returns whether a leaf reader comes from a merge (versus flush or addIndexes).
     */
    private static boolean isMergedSegment(AtomicReader reader) {
        // We expect leaves to be segment readers
        final Map<String, String> diagnostics = SegmentReaderUtils.segmentReader(reader).getSegmentInfo().info.getDiagnostics();
        final String source = diagnostics.get(IndexWriter.SOURCE);
        assert Arrays.asList(IndexWriter.SOURCE_ADDINDEXES_READERS, IndexWriter.SOURCE_FLUSH, IndexWriter.SOURCE_MERGE).contains(source) : "Unknown source " + source;
        return IndexWriter.SOURCE_MERGE.equals(source);
    }

    private IndexWriter createWriter() throws IOException {
        try {
            // release locks when started
            if (IndexWriter.isLocked(store.directory())) {
                logger.warn("shard is locked, releasing lock");
                IndexWriter.unlock(store.directory());
            }
            boolean create = !Lucene.indexExists(store.directory());
            IndexWriterConfig config = new IndexWriterConfig(Lucene.VERSION, analysisService.defaultIndexAnalyzer());
            config.setOpenMode(create ? IndexWriterConfig.OpenMode.CREATE : IndexWriterConfig.OpenMode.APPEND);
            config.setIndexDeletionPolicy(deletionPolicy);
            config.setMergeScheduler(mergeScheduler.newMergeScheduler());
            MergePolicy mergePolicy = mergePolicyProvider.newMergePolicy();
            // Give us the opportunity to upgrade old segments while performing
            // background merges
            mergePolicy = new IndexUpgraderMergePolicy(mergePolicy);
            config.setMergePolicy(mergePolicy);
            config.setSimilarity(similarityService.similarity());
            config.setRAMBufferSizeMB(indexingBufferSize.mbFrac());
            config.setMaxThreadStates(indexConcurrency);
            config.setCodec(codecService.codec(codecName));
            /* We set this timeout to a highish value to work around
             * the default poll interval in the Lucene lock that is 
             * 1000ms by default. We might need to poll multiple times
             * here but with 1s poll this is only executed twice at most
             * in combination with the default writelock timeout*/
            config.setWriteLockTimeout(5000);
            config.setUseCompoundFile(this.compoundOnFlush);
            // Warm-up hook for newly-merged segments. Warming up segments here is better since it will be performed at the end
            // of the merge operation and won't slow down _refresh
            config.setMergedSegmentWarmer(new IndexReaderWarmer() {
                @Override
                public void warm(AtomicReader reader) throws IOException {
                    try {
                        assert isMergedSegment(reader);
                        final Engine.Searcher searcher = new SimpleSearcher("warmer", new IndexSearcher(reader));
                        final IndicesWarmer.WarmerContext context = new IndicesWarmer.WarmerContext(shardId, searcher);
                        if (warmer != null) warmer.warm(context);
                    } catch (Throwable t) {
                        // Don't fail a merge if the warm-up failed
                        if (!closed) {
                            logger.warn("Warm-up failed", t);
                        }
                        if (t instanceof Error) {
                            // assertion/out-of-memory error, don't ignore those
                            throw (Error) t;
                        }
                    }
                }
            });
            return new IndexWriter(store.directory(), config);
        } catch (LockObtainFailedException ex) {
            boolean isLocked = IndexWriter.isLocked(store.directory());
            logger.warn("Could not lock IndexWriter isLocked [{}]", ex, isLocked);
            throw ex;
        }
    }

    public static final String INDEX_INDEX_CONCURRENCY = "index.index_concurrency";
    public static final String INDEX_COMPOUND_ON_FLUSH = "index.compound_on_flush";
    public static final String INDEX_GC_DELETES = "index.gc_deletes";
    public static final String INDEX_FAIL_ON_MERGE_FAILURE = "index.fail_on_merge_failure";

    class ApplySettings implements IndexSettingsService.Listener {

        @Override
        public void onRefreshSettings(Settings settings) {
            long gcDeletesInMillis = settings.getAsTime(INDEX_GC_DELETES, TimeValue.timeValueMillis(InternalEngine.this.gcDeletesInMillis)).millis();
            if (gcDeletesInMillis != InternalEngine.this.gcDeletesInMillis) {
                logger.info("updating index.gc_deletes from [{}] to [{}]", TimeValue.timeValueMillis(InternalEngine.this.gcDeletesInMillis), TimeValue.timeValueMillis(gcDeletesInMillis));
                InternalEngine.this.gcDeletesInMillis = gcDeletesInMillis;
            }

            final boolean compoundOnFlush = settings.getAsBoolean(INDEX_COMPOUND_ON_FLUSH, InternalEngine.this.compoundOnFlush);
            if (compoundOnFlush != InternalEngine.this.compoundOnFlush) {
                logger.info("updating {} from [{}] to [{}]", InternalEngine.INDEX_COMPOUND_ON_FLUSH, InternalEngine.this.compoundOnFlush, compoundOnFlush);
                InternalEngine.this.compoundOnFlush = compoundOnFlush;
                indexWriter.getConfig().setUseCompoundFile(compoundOnFlush);
            }

            int indexConcurrency = settings.getAsInt(INDEX_INDEX_CONCURRENCY, InternalEngine.this.indexConcurrency);
            boolean failOnMergeFailure = settings.getAsBoolean(INDEX_FAIL_ON_MERGE_FAILURE, InternalEngine.this.failOnMergeFailure);
            String codecName = settings.get(INDEX_CODEC, InternalEngine.this.codecName);
            final boolean codecBloomLoad = settings.getAsBoolean(CodecService.INDEX_CODEC_BLOOM_LOAD, codecService.isLoadBloomFilter());
            boolean requiresFlushing = false;
            if (indexConcurrency != InternalEngine.this.indexConcurrency ||
                    !codecName.equals(InternalEngine.this.codecName) ||
                    failOnMergeFailure != InternalEngine.this.failOnMergeFailure ||
                    codecBloomLoad != codecService.isLoadBloomFilter()) {
                rwl.readLock().lock();
                try {
                    if (indexConcurrency != InternalEngine.this.indexConcurrency) {
                        logger.info("updating index.index_concurrency from [{}] to [{}]", InternalEngine.this.indexConcurrency, indexConcurrency);
                        InternalEngine.this.indexConcurrency = indexConcurrency;
                        // we have to flush in this case, since it only applies on a new index writer
                        requiresFlushing = true;
                    }
                    if (!codecName.equals(InternalEngine.this.codecName)) {
                        logger.info("updating index.codec from [{}] to [{}]", InternalEngine.this.codecName, codecName);
                        InternalEngine.this.codecName = codecName;
                        // we want to flush in this case, so the new codec will be reflected right away...
                        requiresFlushing = true;
                    }
                    if (failOnMergeFailure != InternalEngine.this.failOnMergeFailure) {
                        logger.info("updating {} from [{}] to [{}]", InternalEngine.INDEX_FAIL_ON_MERGE_FAILURE, InternalEngine.this.failOnMergeFailure, failOnMergeFailure);
                        InternalEngine.this.failOnMergeFailure = failOnMergeFailure;
                    }
                    if (codecBloomLoad != codecService.isLoadBloomFilter()) {
                        logger.info("updating {} from [{}] to [{}]", CodecService.INDEX_CODEC_BLOOM_LOAD, codecService.isLoadBloomFilter(), codecBloomLoad);
                        codecService.setLoadBloomFilter(codecBloomLoad);
                        // we need to flush in this case, to load/unload the bloom filters
                        requiresFlushing = true;
                    }
                } finally {
                    rwl.readLock().unlock();
                }
                if (requiresFlushing) {
                    flush(new Flush().type(Flush.Type.NEW_WRITER));
                }
            }
        }
    }

    private XSearcherManager buildSearchManager(IndexWriter indexWriter) throws IOException {
        return new XSearcherManager(indexWriter, true, searcherFactory);
    }

    class EngineSearcher implements Searcher {

        private final String source;
        private final IndexSearcher searcher;
        private final XSearcherManager manager;
        private final AtomicBoolean released;

        private EngineSearcher(String source, IndexSearcher searcher, XSearcherManager manager) {
            this.source = source;
            this.searcher = searcher;
            this.manager = manager;
            this.released = new AtomicBoolean(false);
        }

        @Override
        public String source() {
            return this.source;
        }

        @Override
        public IndexReader reader() {
            return searcher.getIndexReader();
        }

        @Override
        public IndexSearcher searcher() {
            return searcher;
        }

        @Override
        public boolean release() throws ElasticsearchException {
            if (!released.compareAndSet(false, true)) {
                /* In general, searchers should never be released twice or this would break reference counting. There is one rare case
                 * when it might happen though: when the request and the Reaper thread would both try to release it in a very short amount
                 * of time, this is why we only log a warning instead of throwing an exception.
                 */
                logger.warn("Searcher was released twice", new ElasticsearchIllegalStateException("Double release"));
                return false;
            }
            try {
                manager.release(searcher);
                return true;
            } catch (IOException e) {
                return false;
            } catch (AlreadyClosedException e) {
                /* this one can happen if we already closed the
                 * underlying store / directory and we call into the
                 * IndexWriter to free up pending files. */
                return false;
            }
        }
    }

    static class VersionValue {
        private final long version;
        private final boolean delete;
        private final long time;
        private final Translog.Location translogLocation;

        VersionValue(long version, boolean delete, long time, Translog.Location translogLocation) {
            this.version = version;
            this.delete = delete;
            this.time = time;
            this.translogLocation = translogLocation;
        }

        public long time() {
            return this.time;
        }

        public long version() {
            return version;
        }

        public boolean delete() {
            return delete;
        }

        public Translog.Location translogLocation() {
            return this.translogLocation;
        }
    }

    class SearchFactory extends SearcherFactory {

        @Override
        public IndexSearcher newSearcher(IndexReader reader) throws IOException {
            IndexSearcher searcher = new IndexSearcher(reader);
            searcher.setSimilarity(similarityService.similarity());
            if (warmer != null) {
                // we need to pass a custom searcher that does not release anything on Engine.Search Release,
                // we will release explicitly
                Searcher currentSearcher = null;
                IndexSearcher newSearcher = null;
                boolean closeNewSearcher = false;
                try {
                    if (searcherManager == null) {
                        // fresh index writer, just do on all of it
                        newSearcher = searcher;
                    } else {
                        currentSearcher = acquireSearcher("search_factory");
                        // figure out the newSearcher, with only the new readers that are relevant for us
                        List<IndexReader> readers = Lists.newArrayList();
                        for (AtomicReaderContext newReaderContext : searcher.getIndexReader().leaves()) {
                            if (isMergedSegment(newReaderContext.reader())) {
                                // merged segments are already handled by IndexWriterConfig.setMergedSegmentWarmer
                                continue;
                            }
                            boolean found = false;
                            for (AtomicReaderContext currentReaderContext : currentSearcher.reader().leaves()) {
                                if (currentReaderContext.reader().getCoreCacheKey().equals(newReaderContext.reader().getCoreCacheKey())) {
                                    found = true;
                                    break;
                                }
                            }
                            if (!found) {
                                readers.add(newReaderContext.reader());
                            }
                        }
                        if (!readers.isEmpty()) {
                            // we don't want to close the inner readers, just increase ref on them
                            newSearcher = new IndexSearcher(new MultiReader(readers.toArray(new IndexReader[readers.size()]), false));
                            closeNewSearcher = true;
                        }
                    }

                    if (newSearcher != null) {
                        IndicesWarmer.WarmerContext context = new IndicesWarmer.WarmerContext(shardId,
                                new SimpleSearcher("warmer", newSearcher));
                        warmer.warm(context);
                    }
                } catch (Throwable e) {
                    if (!closed) {
                        logger.warn("failed to prepare/warm", e);
                    }
                } finally {
                    // no need to release the fullSearcher, nothing really is done...
                    if (currentSearcher != null) {
                        currentSearcher.release();
                    }
                    if (newSearcher != null && closeNewSearcher) {
                        IOUtils.closeWhileHandlingException(newSearcher.getIndexReader()); // ignore
                    }
                }
            }
            return searcher;
        }
    }

    private static final class RecoveryCounter {
        private volatile int ongoingRecoveries = 0;

        synchronized void increment() {
            ongoingRecoveries++;
        }

        synchronized void decrement() {
            ongoingRecoveries--;
            if (ongoingRecoveries == 0) {
                notifyAll(); // notify waiting threads - we only wait on ongoingRecoveries == 0
            }
            assert ongoingRecoveries >= 0 : "ongoingRecoveries must be >= 0 but was: " + ongoingRecoveries;
        }

        int get() {
            // volatile read - no sync needed
            return ongoingRecoveries;
        }

        synchronized int awaitNoRecoveries(long timeout) throws InterruptedException {
            if (ongoingRecoveries > 0) { // no loop here - we either time out or we are done!
                wait(timeout);
            }
            return ongoingRecoveries;
        }
    }
}
