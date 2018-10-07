/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

public final class FrozenEngine extends ReadOnlyEngine {
    public static final Setting<Boolean> INDEX_FROZEN = Setting.boolSetting("index.frozen", false, Setting.Property.IndexScope,
        Setting.Property.PrivateIndex);
    private final CounterMetric openedReaders = new CounterMetric();
    private volatile DirectoryReader lastOpenedReader;
    private final DirectoryReader canMatchReader;

    public FrozenEngine(EngineConfig config) {
        super(config, null, null, true, Function.identity());

        boolean success = false;
        Directory directory = store.directory();
        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            canMatchReader = new RewriteCachingDirectoryReader(directory, reader.leaves());
            success = true;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            if (success == false) {
                closeNoLock("failed on construction", new CountDownLatch(1));
            }
        }
    }

    @Override
    protected DirectoryReader open(Directory directory) throws IOException {
        // we fake an empty directly reader for the ReadOnlyEngine. this reader is only used
        // to initialize the reference manager and to make the refresh call happy which is essentially
        // a no-op now
        IndexCommit indexCommit = Lucene.getIndexCommit(getLastCommittedSegmentInfos(), directory);
        return new DirectoryReader(directory, new LeafReader[0]) {
            @Override
            protected DirectoryReader doOpenIfChanged() {
                return null;
            }

            @Override
            protected DirectoryReader doOpenIfChanged(IndexCommit commit) {
                return null;
            }

            @Override
            protected DirectoryReader doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes) {
                return null;
            }

            @Override
            public long getVersion() {
                return 0;
            }

            @Override
            public boolean isCurrent() {
                return true; // always current
            }

            @Override
            public IndexCommit getIndexCommit() {
                return indexCommit; // TODO maybe we can return an empty commit?
            }

            @Override
            protected void doClose() {
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return null;
            }
        };
    }

    private synchronized void onReaderClosed(IndexReader.CacheKey key) {
        if (lastOpenedReader != null && key == lastOpenedReader.getReaderCacheHelper().getKey()) {
            assert lastOpenedReader.getRefCount() == 0;
            lastOpenedReader = null;
        }
    }

    private synchronized DirectoryReader getOrOpenReader(boolean doOpen) throws IOException {
        DirectoryReader reader = null;
        boolean success = false;
        try {
            if (lastOpenedReader == null || lastOpenedReader.tryIncRef() == false) {
                if (doOpen) {
                    reader = DirectoryReader.open(engineConfig.getStore().directory());
                    searcherFactory.processReaders(reader, null);
                    openedReaders.inc();
                    reader = lastOpenedReader = wrapReader(reader, Function.identity());
                    reader.getReaderCacheHelper().addClosedListener(this::onReaderClosed);
                }
            } else {
                reader = lastOpenedReader;
            }
            success = true;
            return reader;
        } finally {
            if (success == false) {
                IOUtils.close(reader);
            }
        }
    }

    @Override
    public Searcher acquireSearcher(String source, SearcherScope scope) throws EngineException {
        store.incRef();
        boolean success = false;
        try  {
            final boolean openReader;
            switch (source) {
                case "load_seq_no":
                case "load_version":
                    assert false : "this is a read-only engine";
                case "doc_stats":
                    assert false : "doc_stats are overwritten";
                case "segments":
                case "segments_stats":
                case "completion_stats":
                case "refresh_needed":
                case "can_match": // special case for can_match phase - we use the cached point values reader
                    openReader = false;
                    break;
                default:
                    openReader = true;
            }
            // special case we only want to report segment stats if we have a reader open. in that case we only get a reader if we still
            // have one open at the time and can inc it's reference.
            DirectoryReader reader = getOrOpenReader(openReader);
            if (reader == null) {
                store.decRef();
                success = true;
                if ("can_match".equals(source)) {
                    canMatchReader.incRef();
                    return new Searcher(source, new IndexSearcher(canMatchReader), logger);
                }
                // we just hand out an empty searcher in this case
                return super.acquireSearcher(source, scope);
            } else {
                try {
                    LazyDirectoryReader lazyDirectoryReader = new LazyDirectoryReader(reader);
                    FrozenEngineSearcher newSearcher = new FrozenEngineSearcher(source, lazyDirectoryReader,
                        new IndexSearcher(lazyDirectoryReader),
                        s -> {
                        try {
                            s.getIndexReader().close();
                        } finally {
                            store.decRef();
                        }
                    }, logger);
                    success = true;
                    return newSearcher;
                } finally {
                    if (success == false) {
                        reader.decRef();
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            if (success == false) {
                store.decRef();
            }
        }
    }

    /**
     * A Searcher impl that makes it straight forward to release readers after a search phase
     */
    final class FrozenEngineSearcher extends Searcher {

        private final LazyDirectoryReader lazyDirectoryReader;

        FrozenEngineSearcher(String source, LazyDirectoryReader lazyDirectoryReader, IndexSearcher searcher,
                                    IOUtils.IOConsumer<IndexSearcher>
            onClose, Logger logger) {
            super(source, searcher, onClose, logger);
            this.lazyDirectoryReader = lazyDirectoryReader;
        }

        void releaseReader() throws IOException {
            lazyDirectoryReader.release();
        }

        void resetReader() throws IOException {
            lazyDirectoryReader.reset(getOrOpenReader(true));
        }
    }


    /*
     * We register this listener for a frozen index that will
     *  1. reset the reader every time the search context is validated which happens when the context is looked up ie. on a fetch phase
     *  etc.
     *  2. register a releasable resource that is cleaned after each phase that releases the reader for this searcher
     */
    public static class ReacquireEngineSearcherListener implements SearchOperationListener {

        @Override
        public void validateSearchContext(SearchContext context, TransportRequest transportRequest) {
            Searcher engineSearcher = context.searcher().getEngineSearcher();
            if (engineSearcher instanceof FrozenEngineSearcher) {
                try {
                    ((FrozenEngineSearcher) engineSearcher).resetReader();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            // also register a release resource in this case if we have multiple roundtrips like in DFS
            onNewContext(context);
        }

        @Override
        public void onNewContext(SearchContext context) {
            Searcher engineSearcher = context.searcher().getEngineSearcher();
            context.addReleasable(() -> {
                if (engineSearcher instanceof FrozenEngineSearcher) {
                    try {
                        ((FrozenEngineSearcher) engineSearcher).releaseReader();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            }, SearchContext.Lifetime.PHASE);
        }
    }

    static final class LazyDirectoryReader extends FilterDirectoryReader {

        private volatile DirectoryReader delegate; // volatile since it might be closed concurrently

        LazyDirectoryReader(DirectoryReader reader) throws IOException {
            super(reader, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    return new LazyLeafReader(reader);
                };
            });
            this.delegate = reader;
        }

        synchronized void release() throws IOException {
            if (delegate != null) { // we are lenient here it's ok to double close
                delegate.decRef();
                delegate = null;
                if (tryIncRef()) { // only do this if we are not closed already
                    try {
                        for (LeafReaderContext leaf : leaves()) {
                            LazyLeafReader reader = (LazyLeafReader) leaf.reader();
                            reader.in = null;
                        }
                    } finally {
                        decRef();
                    }
                }
            }
        }

        synchronized void reset(DirectoryReader delegate) {
            if (this.delegate != null) {
                throw new IllegalStateException("lazy reader is not released");
            }
            assert (delegate instanceof LazyDirectoryReader) == false : "must not be a LazyDirectoryReader";
            List<LeafReaderContext> leaves = delegate.leaves();
            int ord = 0;
            for (LeafReaderContext leaf : leaves()) {
                LazyLeafReader reader = (LazyLeafReader) leaf.reader();
                LeafReader newReader = leaves.get(ord++).reader();
                assert reader.in == null;
                reader.in = newReader;
                assert reader.info.info.equals(Lucene.segmentReader(newReader).getSegmentInfo().info);
            }
            this.delegate = delegate;
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) {
            throw new UnsupportedOperationException();
        }

        void ensureOpenOrReset() {
            ensureOpen();
            if (delegate == null) {
                throw new AlreadyClosedException("delegate is released");
            }
        }

        @Override
        public long getVersion() {
            ensureOpenOrReset();
            return delegate.getVersion();
        }

        @Override
        public boolean isCurrent() throws IOException {
            ensureOpenOrReset();
            return delegate.isCurrent();
        }

        @Override
        public IndexCommit getIndexCommit() throws IOException {
            ensureOpenOrReset();
            return delegate.getIndexCommit();
        }

        @Override
        protected void doClose() throws IOException {
            release();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            ensureOpenOrReset();
            return delegate.getReaderCacheHelper();
        }

        @Override
        public DirectoryReader getDelegate() {
            ensureOpenOrReset();
            return this.delegate;
        }
    }

    /**
     * We basically duplicate a FilterLeafReader here since we don't want the
     * incoming reader to register with this reader as a parent reader. This would mean we barf if the incoming
     * reader is closed and that is what we actually doing on purpose.
     */
    static final class LazyLeafReader extends FilterLeafReader {

        private volatile LeafReader in;
        private final SegmentCommitInfo info;
        private final int numDocs;
        private final int maxDocs;

        LazyLeafReader(LeafReader in) {
            super(Lucene.emptyReader(in.maxDoc())); // empty reader here to make FilterLeafReader happy
            this.info = Lucene.segmentReader(in).getSegmentInfo();
            this.in = in;
            numDocs = in.numDocs();
            maxDocs = in.maxDoc();
            // don't register in reader as a subreader here.
        }

        private void ensureOpenOrReleased() {
            ensureOpen();
            if (in == null) {
                throw new AlreadyClosedException("leaf is already released");
            }
        }

        @Override
        public Bits getLiveDocs() {
            ensureOpenOrReleased();
            return in.getLiveDocs();
        }

        @Override
        public FieldInfos getFieldInfos() {
            ensureOpenOrReleased();
            return in.getFieldInfos();
        }

        @Override
        public PointValues getPointValues(String field) throws IOException {
            ensureOpenOrReleased();
            return in.getPointValues(field);
        }

        @Override
        public Fields getTermVectors(int docID)
            throws IOException {
            ensureOpenOrReleased();
            return in.getTermVectors(docID);
        }

        @Override
        public int numDocs() {
            return numDocs;
        }

        @Override
        public int maxDoc() {
            return maxDocs;
        }

        @Override
        public void document(int docID, StoredFieldVisitor visitor) throws IOException {
            ensureOpenOrReleased();
            in.document(docID, visitor);
        }

        @Override
        protected void doClose() throws IOException {
            in.close();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            ensureOpenOrReleased();
            return in.getReaderCacheHelper();
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            ensureOpenOrReleased();
            return in.getCoreCacheHelper();
        }

        @Override
        public Terms terms(String field) throws IOException {
            ensureOpenOrReleased();
            return in.terms(field);
        }

        @Override
        public String toString() {
            final StringBuilder buffer = new StringBuilder("LazyLeafReader(");
            buffer.append(in);
            buffer.append(')');
            return buffer.toString();
        }

        @Override
        public NumericDocValues getNumericDocValues(String field) throws IOException {
            ensureOpenOrReleased();
            return in.getNumericDocValues(field);
        }

        @Override
        public BinaryDocValues getBinaryDocValues(String field) throws IOException {
            ensureOpenOrReleased();
            return in.getBinaryDocValues(field);
        }

        @Override
        public SortedDocValues getSortedDocValues(String field) throws IOException {
            ensureOpenOrReleased();
            return in.getSortedDocValues(field);
        }

        @Override
        public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
            ensureOpenOrReleased();
            return in.getSortedNumericDocValues(field);
        }

        @Override
        public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
            ensureOpenOrReleased();
            return in.getSortedSetDocValues(field);
        }

        @Override
        public NumericDocValues getNormValues(String field) throws IOException {
            ensureOpenOrReleased();
            return in.getNormValues(field);
        }

        @Override
        public LeafMetaData getMetaData() {
            ensureOpenOrReleased();
            return in.getMetaData();
        }

        @Override
        public void checkIntegrity() throws IOException {
            ensureOpenOrReleased();
            in.checkIntegrity();
        }

        @Override
        public LeafReader getDelegate() {
            return in;
        }
    }

    // TODO expose this as stats on master
    long getOpenedReaders() {
        return openedReaders.count();
    }

    synchronized boolean isReaderOpen() {
        return lastOpenedReader != null;
    }
}
