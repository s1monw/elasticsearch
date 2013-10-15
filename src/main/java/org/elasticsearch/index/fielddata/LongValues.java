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
import org.elasticsearch.index.fielddata.FieldDataIterable.LongRef;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.Ordinals.Docs;

/**
 */
public abstract class LongValues implements FieldDataIterable<LongRef>, FieldDataIterable.Scratchable<LongRef> {

    public static final LongValues EMPTY = new Empty();
    protected boolean multiValued;
    protected Enum<LongRef> sharedIter;


    protected LongValues(boolean multiValued) {
        this.multiValued = multiValued;
    }

    /**
     * Is one of the documents in this field data values is multi valued?
     */
    public final boolean isMultiValued() {
        return multiValued;
    }

    /**
     * Is there a value for this doc?
     */
    public abstract boolean hasValue(int docId);

    public abstract long getValue(int docId);

    public long getValueMissing(int docId, long missingValue) {
        if (hasValue(docId)) {
            return getValue(docId);
        }
        return missingValue;
    }
    
    @Override
    public LongRef getValueScratch(int docId,
            LongRef spare) {
        if (hasValue(docId)) {
            spare.value = getValue(docId);
            return spare;
        }
        return null;
    }

    @Override
    public LongRef newScratch() {
        return new LongRef();
    }

    public Iter getIter(int docId) {
        if (sharedIter == null) {
            sharedIter = newIter(ConsumptionHint.LAZY);
        }
        return new Iter() {
            long val;
            @Override
            public boolean hasNext() {
                LongRef next = sharedIter.next();
                if (next == null) {
                    return false;
                }
                val = next.value;
                return true;
            }

            @Override
            public long next() {
                return val;
            }
            
        };
        
    }
    @Override
    public Enum<LongRef> newIter(ConsumptionHint hint) {
        if (isMultiValued()) {
            return new SingelValueEnum<LongRef>(this);    
        } else {
            assert this instanceof WithOrdinals;
            WithOrdinals withOrds = ((WithOrdinals)this);
            return new MultiValueIterator<LongRef>(withOrds, withOrds.ordinals());
        }
    }
    


    public static abstract class Dense extends LongValues {


        protected Dense(boolean multiValued) {
            super(multiValued);
        }

        @Override
        public final boolean hasValue(int docId) {
            return true;
        }

        public final long getValueMissing(int docId, long missingValue) {
            assert hasValue(docId);
            assert !isMultiValued();
            return getValue(docId);
        }

    }

    public static abstract class WithOrdinals extends LongValues implements FieldDataIterable.OrdScratchable<LongRef> {

        protected final Docs ordinals;

        protected WithOrdinals(Ordinals.Docs ordinals) {
            super(ordinals.isMultiValued());
            this.ordinals = ordinals;
        }

        public Docs ordinals() {
            return this.ordinals;
        }

        @Override
        public final boolean hasValue(int docId) {
            return ordinals.getOrd(docId) != 0;
        }

        @Override
        public final long getValue(int docId) {
            return getValueByOrd(ordinals.getOrd(docId));
        }

        public abstract long getValueByOrd(long ord);

        @Override
        public final long getValueMissing(int docId, long missingValue) {
            final long ord = ordinals.getOrd(docId);
            if (ord == 0) {
                return missingValue;
            } else {
                return getValueByOrd(ord);
            }
        }
        
        @Override
        public LongRef getValueScratchByOrd(long ordinal,
                LongRef spare) {
            spare.value = getValueByOrd(ordinal);
            return spare;
        }


    }

    public static interface Iter {

        boolean hasNext();

        long next();

        public static class Empty implements Iter {

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
    }

    static class Empty extends LongValues {

        public Empty() {
            super(false);
        }

        @Override
        public boolean hasValue(int docId) {
            return false;
        }

        @Override
        public long getValue(int docId) {
            // conforms with all other impls when there is no value
            return 0;
        }
    }

    public static class Filtered extends LongValues {

        protected final LongValues delegate;

        public Filtered(LongValues delegate) {
            super(delegate.isMultiValued());
            this.delegate = delegate;
        }

        public boolean hasValue(int docId) {
            return delegate.hasValue(docId);
        }

        public long getValue(int docId) {
            return delegate.getValue(docId);
        }

        public Iter getIter(int docId) {
            return delegate.getIter(docId);
        }
    }

}
