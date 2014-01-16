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
package org.elasticsearch.index.search.child;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.search.NoCacheFilter;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.IdentityHashMap;

/**
 * Forked from {@link QueryWrapperFilter} to make sure the weight is only created once.
 * This filter should never be cached! This filter only exists for internal usage.
 *
 * @elasticsearch.internal
 */
public class CustomQueryWrappingFilter extends NoCacheFilter {

    private final Query query;

    private IndexSearcher searcher;
    private Weight weight;
    private IdentityHashMap<AtomicReader, DocIdSet> docIdSets = new IdentityHashMap<AtomicReader, DocIdSet>();

    /** Constructs a filter which only matches documents matching
     * <code>query</code>.
     */
    public CustomQueryWrappingFilter(Query query) {
        if (query == null)
            throw new NullPointerException("Query may not be null");
        this.query = query;
    }

    /** returns the inner Query */
    public final Query getQuery() {
        return query;
    }

    @Override
    public DocIdSet getDocIdSet(final AtomicReaderContext context, final Bits acceptDocs) throws IOException {
        final SearchContext searchContext = SearchContext.current();
        if (weight == null) {
            assert searcher == null;
            IndexSearcher searcher = searchContext.searcher();
            weight = searcher.createNormalizedWeight(query);
            this.searcher = searcher;
            for (final AtomicReaderContext leaf : searcher.getTopReaderContext().leaves()) {
                final DocIdSet set = DocIdSets.toCacheable(leaf.reader(), new DocIdSet() {
                    @Override
                    public DocIdSetIterator iterator() throws IOException {
                        return weight.scorer(leaf, true, false, null);
                    }
                    @Override
                    public boolean isCacheable() { return false; }
                });
                docIdSets.put(leaf.reader(), set);
            }
        } else {
            assert searcher == SearchContext.current().searcher();
        }
        final DocIdSet set = docIdSets.get(context.reader());
        if (set != null && acceptDocs != null) {
            return BitsFilteredDocIdSet.wrap(set, acceptDocs);
        }
        return set;
    }

    @Override
    public String toString() {
        return "CustomQueryWrappingFilter(" + query + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o != null && o instanceof CustomQueryWrappingFilter &&
                this.query.equals(((CustomQueryWrappingFilter)o).query)) {
            return true;
        }

        return false;
    }

    private IndexReader getThisOrContextReader() {
        if (this.searcher == null) {
            SearchContext searchContext = SearchContext.current();
            if (searchContext != null) {
                return searchContext.searcher().getIndexReader();
            }
        }
        return searcher.getIndexReader();
    }

    @Override
    public int hashCode() {
        return query.hashCode() ^ 0x823D64C9;
    }
}
