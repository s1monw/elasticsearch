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
package org.elasticsearch.common.lucene.index;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.Lucene.SoftLiveDocs;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A {@link org.apache.lucene.index.FilterDirectoryReader} that exposes
 * Elasticsearch internal per shard / index information like the shard ID.
 */
public final class ElasticsearchDirectoryReader extends FilterDirectoryReader {
    public static SoftLiveDocs ALL_DOCS_LIVE = new SoftLiveDocs(new Bits.MatchAllBits(0), 0, 0);

    private final ShardId shardId;
    private final SubReaderWrapper wrapper;

    private ElasticsearchDirectoryReader(DirectoryReader in, SubReaderWrapper wrapper, ShardId shardId) throws
        IOException {
        super(in, wrapper);
        this.wrapper = wrapper;
        this.shardId = shardId;
    }

    /**
     * Returns the shard id this index belongs to.
     */
    public ShardId shardId() {
        return this.shardId;
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        // safe to delegate since this reader does not alter the index
        return in.getReaderCacheHelper();
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return new ElasticsearchDirectoryReader(in, wrapper, shardId);
    }

    /**
     * Wraps the given reader in a {@link org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader} as
     * well as all it's sub-readers in {@link org.elasticsearch.common.lucene.index.ElasticsearchLeafReader} to
     * expose the given shard Id.
     *
     * @param reader the reader to wrap
     * @param shardId the shard ID to expose via the elasticsearch internal reader wrappers.
     */
    public static ElasticsearchDirectoryReader wrap(DirectoryReader reader, ShardId shardId) throws IOException {
        return wrap(reader, shardId, true);
    }

    public ElasticsearchDirectoryReader wrapWithoutSoftDeletes() throws IOException {
        return wrap(this.getDelegate(), shardId, false);
    }

    private static ElasticsearchDirectoryReader wrap(DirectoryReader reader, ShardId shardId, boolean useSoftDeletes) throws IOException {
        ConcurrentHashMap<Object, SoftLiveDocs> liveDocsCache = new ConcurrentHashMap<>();
        Function<LeafReader, SoftLiveDocs> cache;
        if (useSoftDeletes) {
            cache = leafReader -> {
            SoftLiveDocs softLiveDocs = liveDocsCache.computeIfAbsent(leafReader.getReaderCacheHelper().getKey(), (k) -> {
                final SoftLiveDocs bits;
                try {
                    NumericDocValues softDeleteDocValues = leafReader.getNumericDocValues(Lucene.SOFT_DELETE_FIELD);
                    bits = Lucene.getSoftLiveDocs(softDeleteDocValues, leafReader);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                leafReader.getReaderCacheHelper().addClosedListener(liveDocsCache::remove);
                return bits == null ? ALL_DOCS_LIVE : bits;
            });

            return softLiveDocs;
        };
        } else {
            cache = leafReader -> leafReader.getLiveDocs() == null ? ALL_DOCS_LIVE: new SoftLiveDocs(leafReader.getLiveDocs(),
                leafReader.numDocs(), leafReader.maxDoc());
        }
        SubReaderWrapper wrapper = new SubReaderWrapper(shardId, cache);
        return new ElasticsearchDirectoryReader(reader, wrapper, shardId);
    }

    private static class SubReaderWrapper extends FilterDirectoryReader.SubReaderWrapper {
        private final ShardId shardId;
        private final Function<LeafReader, SoftLiveDocs> liveDocsCache;

        SubReaderWrapper(ShardId shardId, Function<LeafReader, SoftLiveDocs> liveDocsCache) {
            this.shardId = shardId;
            this.liveDocsCache = liveDocsCache;
        }
        @Override
        public LeafReader wrap(LeafReader reader) {
            SoftLiveDocs softLiveDocs = liveDocsCache.apply(reader);
            return new ElasticsearchLeafReader(reader, shardId, softLiveDocs == ALL_DOCS_LIVE ? null : softLiveDocs);
        }
    }

    /**
     * Adds the given listener to the provided directory reader. The reader must contain an {@link ElasticsearchDirectoryReader} in it's hierarchy
     * otherwise we can't safely install the listener.
     *
     * @throws IllegalArgumentException if the reader doesn't contain an {@link ElasticsearchDirectoryReader} in it's hierarchy
     */
    @SuppressForbidden(reason = "This is the only sane way to add a ReaderClosedListener")
    public static void addReaderCloseListener(DirectoryReader reader, IndexReader.ClosedListener listener) {
        ElasticsearchDirectoryReader elasticsearchDirectoryReader = getElasticsearchDirectoryReader(reader);
        if (elasticsearchDirectoryReader == null) {
            throw new IllegalArgumentException("Can't install close listener reader is not an ElasticsearchDirectoryReader/ElasticsearchLeafReader");
        }
        IndexReader.CacheHelper cacheHelper = elasticsearchDirectoryReader.getReaderCacheHelper();
        if (cacheHelper == null) {
            throw new IllegalArgumentException("Reader " + elasticsearchDirectoryReader + " does not support caching");
        }
        assert cacheHelper.getKey() == reader.getReaderCacheHelper().getKey();
        cacheHelper.addClosedListener(listener);
    }

    /**
     * Tries to unwrap the given reader until the first {@link ElasticsearchDirectoryReader} instance is found or <code>null</code> if no instance is found;
     */
    public static ElasticsearchDirectoryReader getElasticsearchDirectoryReader(DirectoryReader reader) {
        if (reader instanceof FilterDirectoryReader) {
            if (reader instanceof ElasticsearchDirectoryReader) {
                return (ElasticsearchDirectoryReader) reader;
            } else {
                // We need to use FilterDirectoryReader#getDelegate and not FilterDirectoryReader#unwrap, because
                // If there are multiple levels of filtered leaf readers then with the unwrap() method it immediately
                // returns the most inner leaf reader and thus skipping of over any other filtered leaf reader that
                // may be instance of ElasticsearchLeafReader. This can cause us to miss the shardId.
                return getElasticsearchDirectoryReader(((FilterDirectoryReader) reader).getDelegate());
            }
        }
        return null;
    }

}
