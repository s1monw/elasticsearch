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

package org.elasticsearch.index.fielddata;

import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.index.fielddata.util.IntArrayRef;
import org.elasticsearch.index.fielddata.util.LongArrayRef;

/**
 */
public interface LongValues {

    static final LongValues EMPTY = new Empty();

    /**
     * Is one of the documents in this field data values is multi valued?
     */
    boolean isMultiValued();

    /**
     * Is there a value for this doc?
     */
    boolean hasValue(int docId);

    long getValue(int docId);

    long getValueMissing(int docId, long missingValue);

    LongArrayRef getValues(int docId);

    Iter getIter(int docId);

    void forEachValueInDoc(int docId, ValueInDocProc proc);

    static interface ValueInDocProc {

        void onValue(int docId, long value);

        void onMissing(int docId);
    }

    static interface Iter {

        boolean hasNext();

        long next();

        static class Empty implements Iter {

            public static final Empty INSTANCE = new Empty();

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public long next() {
                throw new ElasticSearchIllegalStateException();
            }
        }

        static class Single implements Iter {

            public long value;
            public boolean done;

            public Single reset(long value) {
                this.value = value;
                this.done = false;
                return this;
            }

            @Override
            public boolean hasNext() {
                return !done;
            }

            @Override
            public long next() {
                assert !done;
                done = true;
                return value;
            }
        }
    }

    static class Empty implements LongValues {
        @Override
        public boolean isMultiValued() {
            return false;
        }

        @Override
        public boolean hasValue(int docId) {
            return false;
        }

        @Override
        public long getValue(int docId) {
            throw new ElasticSearchIllegalStateException("Can't retrieve a value from an empty LongValues");
        }

        @Override
        public long getValueMissing(int docId, long missingValue) {
            return missingValue;
        }

        @Override
        public LongArrayRef getValues(int docId) {
            return LongArrayRef.EMPTY;
        }

        @Override
        public Iter getIter(int docId) {
            return Iter.Empty.INSTANCE;
        }

        @Override
        public void forEachValueInDoc(int docId, ValueInDocProc proc) {
            proc.onMissing(docId);
        }
    }
    
    public static class FilteredByteValues implements LongValues {

        protected final LongValues delegate;

        public FilteredByteValues(LongValues delegate) {
            this.delegate = delegate;
        }

        public boolean isMultiValued() {
            return delegate.isMultiValued();
        }

        public boolean hasValue(int docId) {
            return delegate.hasValue(docId);
        }

        public long getValue(int docId) {
            return delegate.getValue(docId);
        }

        public long getValueMissing(int docId, long missingValue) {
            return delegate.getValueMissing(docId, missingValue);
        }

        public LongArrayRef getValues(int docId) {
            return delegate.getValues(docId);
        }

        public Iter getIter(int docId) {
            return delegate.getIter(docId);
        }

        public void forEachValueInDoc(int docId, ValueInDocProc proc) {
            delegate.forEachValueInDoc(docId, proc);
        }
    }

}
