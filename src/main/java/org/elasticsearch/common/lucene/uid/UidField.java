/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.lucene.uid;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.codec.postingsformat.BloomFilterPostingsFormat;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

import java.io.Reader;

/**
 *
 */
// TODO: LUCENE 4 UPGRADE: Store version as doc values instead of as a payload.
public class UidField extends Field {

    public static class DocIdAndVersion {
        public final int docId;
        public final long version;
        public final AtomicReaderContext reader;

        public DocIdAndVersion(int docId, long version, AtomicReaderContext reader) {
            this.docId = docId;
            this.version = version;
            this.reader = reader;
        }
    }

    // this works fine for nested docs since they don't have the payload which has the version
    // so we iterate till we find the one with the payload
    public static DocIdAndVersion loadDocIdAndVersion(AtomicReaderContext context, Term term) {
        final String field = term.field();
        final AtomicReader reader = context.reader();
        int docId = Lucene.NO_DOC;
        try {
            final Terms terms = reader.terms(field);
            if (terms == null) {
                return null;
            }
            // hack to break early if we have a bloom filter...
            if (terms instanceof BloomFilterPostingsFormat.BloomFilteredFieldsProducer.BloomFilteredTerms) {
                if (!((BloomFilterPostingsFormat.BloomFilteredFieldsProducer.BloomFilteredTerms) terms).getFilter().mightContain(term.bytes())) {
                    return null;
                }
            }
            final TermsEnum termsEnum = terms.iterator(null);
            if (!termsEnum.seekExact(term.bytes(), true)) {
                return null;
            }
            final NumericDocValues numericDocValues = reader.getNumericDocValues(term.field());
            if (numericDocValues == null) {
                DocsAndPositionsEnum uid = termsEnum.docsAndPositions(reader.getLiveDocs(), null, DocsAndPositionsEnum.FLAG_PAYLOADS);
                if (uid == null || uid.nextDoc() == DocIdSetIterator.NO_MORE_DOCS) {
                    return null; // no doc
                }
                // Note, only master docs uid have version payload, so we can use that info to not
                // take them into account
                do {
                    docId = uid.docID();
                    uid.nextPosition();
                    if (uid.getPayload() == null) {
                        continue;
                    }
                    if (uid.getPayload().length < 8) {
                        continue;
                    }
                    byte[] payload = new byte[uid.getPayload().length];
                    System.arraycopy(uid.getPayload().bytes, uid.getPayload().offset, payload, 0, uid.getPayload().length);
                    return new DocIdAndVersion(docId, Numbers.bytesToLong(payload), context);
                } while (uid.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
                return new DocIdAndVersion(docId, -2, context);
            } else {
                final DocsEnum uid = termsEnum.docs(context.reader().getLiveDocs(), null);
                if (uid == null || uid.nextDoc() == DocIdSetIterator.NO_MORE_DOCS) {
                    return null; // no doc
                }
                do {
                    docId = uid.docID();
                } while (uid.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
                return new DocIdAndVersion(docId, numericDocValues.get(docId), context);
            }
        } catch (Exception e) {
            return new DocIdAndVersion(docId, -2, context);
        }
    }

    /**
     * Load the version for the uid from the reader, returning -1 if no doc exists, or -2 if
     * no version is available (for backward comp.)
     */
    public static long loadVersion(AtomicReaderContext context, Term term) {
        try {
            final String field = term.field();
            final AtomicReader reader = context.reader();
            final Terms terms = reader.terms(field);
            if (terms == null) {
                return -1;
            }
            // hack to break early if we have a bloom filter...
            if (terms instanceof BloomFilterPostingsFormat.BloomFilteredFieldsProducer.BloomFilteredTerms) {
                if (!((BloomFilterPostingsFormat.BloomFilteredFieldsProducer.BloomFilteredTerms) terms).getFilter().mightContain(term.bytes())) {
                    return -1;
                }
            }
            final TermsEnum termsEnum = terms.iterator(null);
            if (!termsEnum.seekExact(term.bytes(), true)) {
                return -1;
            }
            final NumericDocValues numericDocValues = context.reader().getNumericDocValues(term.field());
            if (numericDocValues == null) {
                DocsAndPositionsEnum uid = termsEnum.docsAndPositions(context.reader().getLiveDocs(), null, DocsAndPositionsEnum.FLAG_PAYLOADS);
                if (uid == null || uid.nextDoc() == DocIdSetIterator.NO_MORE_DOCS) {
                    return -1;
                }
                // Note, only master docs uid have version payload, so we can use that info to not
                // take them into account
                do {
                    uid.nextPosition();
                    if (uid.getPayload() == null) {
                        continue;
                    }
                    if (uid.getPayload().length < 8) {
                        continue;
                    }
                    byte[] payload = new byte[uid.getPayload().length];
                    System.arraycopy(uid.getPayload().bytes, uid.getPayload().offset, payload, 0, uid.getPayload().length);
                    return Numbers.bytesToLong(payload);
                } while (uid.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
                return -2;
            } else {
                int docId = DocIdSetIterator.NO_MORE_DOCS;
                final DocsEnum uid = termsEnum.docs(context.reader().getLiveDocs(), null);
                if (uid == null || uid.nextDoc() == DocIdSetIterator.NO_MORE_DOCS) {
                    return -2; // no doc
                }
                do {
                    docId = uid.docID();
                } while (uid.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
                return numericDocValues.get(docId);
            }
        } catch (Exception e) {
            return -2;
        }
    }

    private String uid;

    private long version;

    public UidField(String uid) {
        this(UidFieldMapper.NAME, uid, 0);
    }

    public UidField(String name, String uid, long version) {
        super(name, UidFieldMapper.Defaults.FIELD_TYPE);
        this.uid = uid;
        this.version = version;
    }

    public String uid() {
        return this.uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    @Override
    public String stringValue() {
        return uid;
    }

    @Override
    public Reader readerValue() {
        return null;
    }

    public long version() {
        return this.version;
    }

    public void version(long version) {
        this.version = version;
    }

    @Override
    public Number numericValue() {
        return version;
    }
   
}
