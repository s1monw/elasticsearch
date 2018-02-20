
package org.elasticsearch.index.engine;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogDeletionPolicy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.stream.Stream;

public class LazyEngine extends Engine {
    private final Translog translog;
    private final IndexCommit lastCommit;
    private SegmentInfos lastCommittedSegmentInfos;

    protected LazyEngine(EngineConfig engineConfig) throws IOException {
        super(engineConfig);
        final TranslogDeletionPolicy translogDeletionPolicy = new TranslogDeletionPolicy(
            engineConfig.getIndexSettings().getTranslogRetentionSize().getBytes(),
            engineConfig.getIndexSettings().getTranslogRetentionAge().getMillis()
        );
        lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
        final Map<String, String> commitUserData = lastCommittedSegmentInfos.getUserData();
        this.translog =  new Translog(engineConfig.getTranslogConfig(), commitUserData.get(Translog.TRANSLOG_UUID_KEY),
            translogDeletionPolicy, engineConfig.getGlobalCheckpointSupplier());
        List<IndexCommit> indexCommits = DirectoryReader.listCommits(store.directory());
        lastCommit = indexCommits.get(indexCommits.size()-1);
        searcherFactory = new RamAccountingSearcherFactory(engineConfig.getCircuitBreakerService());


    }

    @Override
    protected SegmentInfos getLastCommittedSegmentInfos() {
        return null;
    }

    @Override
    public String getHistoryUUID() {
        return null;
    }

    @Override
    public long getWritingBytes() {
        return 0;
    }

    @Override
    public long getIndexThrottleTimeInMillis() {
        return 0;
    }

    @Override
    public boolean isThrottled() {
        return false;
    }

    @Override
    public IndexResult index(Index index) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public DeleteResult delete(Delete delete) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public NoOpResult noOp(NoOp noOp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SyncedFlushResult syncFlush(String syncId, CommitId expectedCommitId) throws EngineException {
        throw new UnsupportedOperationException();
    }

    @Override
    public GetResult get(Get get, BiFunction<String, SearcherScope, Searcher> searcherFactory) throws EngineException {
        throw new UnsupportedOperationException(); // TODO fix this
    }

    private DirectoryReader currentReader; // this reader might be closed but we still hold the reference
    private final SearcherFactory searcherFactory;

    private IndexSearcher getOrCreateSearcher() throws IOException {
        synchronized (searcherFactory) {
            if (currentReader != null || currentReader.tryIncRef() == false) {
                currentReader = DirectoryReader.open(lastCommit);
                currentReader.getReaderCacheHelper().addClosedListener(key -> {
                    synchronized (searcherFactory) {
                        if (this.currentReader.getReaderCacheHelper().getKey() == key) {
                            this.currentReader = null; // null it out on close
                        }
                    }
                });
            }
            return searcherFactory.newSearcher(currentReader, null);
        }
    }

    @Override
    public Searcher acquireSearcher(String source, SearcherScope scope) throws EngineException {
        boolean success = false;
        store.incRef();
        try {
            final IndexSearcher searcher = searcherFactory.newSearcher(currentReader, null);
            Searcher engineSearcher = new Searcher(source, null) {
                private volatile IndexSearcher lazySearcher = searcher;
                private final AtomicBoolean released = new AtomicBoolean(false);

                @Override
                public IndexReader reader() {
                    return searcher().getIndexReader();
                }

                @Override
                public synchronized IndexSearcher searcher() {
                    if (lazySearcher == null) {
                        try {
                            lazySearcher = getOrCreateSearcher();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                    return lazySearcher;
                }

                @Override
                public synchronized void releaseResources() throws IOException {
                    final IndexSearcher lazySearcher = this.lazySearcher;
                    if (lazySearcher != null) {
                        this.lazySearcher = null;
                        lazySearcher.getIndexReader().decRef();
                    }
                }

                @Override
                public void close() {
                    if (!released.compareAndSet(false, true)) {
                        /* In general, searchers should never be released twice or this would break reference counting. There is one rare case
                         * when it might happen though: when the request and the Reaper thread would both try to release it in a very short amount
                         * of time, this is why we only log a warning instead of throwing an exception.
                         */
                        logger.warn("Searcher was released twice", new IllegalStateException("Double release"));
                        return;
                    }
                    try {
                        releaseResources();
                    } catch (IOException e) {
                        throw new IllegalStateException("Cannot close", e);
                    } catch (AlreadyClosedException e) {
                        // This means there's a bug somewhere: don't suppress it
                        throw new AssertionError(e);
                    } finally {
                        store.decRef();
                    }
                }
            };
            success = true;
            return engineSearcher;
        } catch (IOException ex) {
            ensureOpen(ex); // throw EngineCloseException here if we are already closed
            logger.error((Supplier<?>) () -> new ParameterizedMessage("failed to acquire searcher, source {}", source), ex);
            throw new EngineException(shardId, "failed to acquire searcher, source " + source, ex);
        } finally {
            if (success == false) {
                store.decRef();
            }
        }
    }

    @Override
    public Translog getTranslog() {
        return translog;
    }

    @Override
    public boolean ensureTranslogSynced(Stream<Translog.Location> locations) throws IOException {
        return false;
    }

    @Override
    public void syncTranslog() throws IOException {
    }

    @Override
    public LocalCheckpointTracker getLocalCheckpointTracker() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getIndexBufferRAMBytesUsed() {
        return 0;
    }

    @Override
    public List<Segment> segments(boolean verbose) {
        return Arrays.asList(getSegmentInfo(lastCommittedSegmentInfos, verbose));
    }

    @Override
    public void refresh(String source) throws EngineException {

    }

    @Override
    public void writeIndexingBuffer() throws EngineException {

    }

    @Override
    public boolean shouldPeriodicallyFlush() {
        return false;
    }

    @Override
    public CommitId flush(boolean force, boolean waitIfOngoing) throws EngineException {
        return new CommitId(lastCommittedSegmentInfos.getId());
    }

    @Override
    public CommitId flush() throws EngineException {
        return new CommitId(lastCommittedSegmentInfos.getId());
    }

    @Override
    public void trimTranslog() throws EngineException {
    }

    @Override
    public void rollTranslogGeneration() throws EngineException {
    }

    @Override
    public void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes, boolean upgrade, boolean upgradeOnlyAncientSegments) throws EngineException, IOException {
    }

    @Override
    public IndexCommitRef acquireLastIndexCommit(boolean flushFirst) throws EngineException {
        return new Engine.IndexCommitRef(lastCommit, () -> {});
    }

    @Override
    public IndexCommitRef acquireSafeIndexCommit() throws EngineException {
        return acquireLastIndexCommit(false);
    }

    @Override
    protected void closeNoLock(String reason, CountDownLatch closedLatch) {

    }

    @Override
    public void activateThrottling() {
        throw new UnsupportedOperationException("lazy engine can't throttle");
    }

    @Override
    public void deactivateThrottling() {
        throw new UnsupportedOperationException("lazy engine can't throttle");
    }

    @Override
    public void restoreLocalCheckpointFromTranslog() throws IOException {

    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        return 0;
    }

    @Override
    public Engine recoverFromTranslog() throws IOException {
        return this;
    }

    @Override
    public void skipTranslogRecovery() {
    }

    @Override
    public void maybePruneDeletes() {
    }
}
