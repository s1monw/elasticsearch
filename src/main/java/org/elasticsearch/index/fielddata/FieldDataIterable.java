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

import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.Ordinals.Docs;
import org.elasticsearch.index.fielddata.ordinals.Ordinals.Docs.Iter;


public interface FieldDataIterable<T> {
    
    public boolean hasValue(int docId);

    public static enum ConsumptionHint {
        MIN_VAULE, MAX_VAULE, LAZY, BULK,
    }

    static interface Scratchable<T> {
        
        
        T getValueScratch(int docId, T spare);
        
        T newScratch();
    }
    
    static interface OrdScratchable<T> extends Scratchable<T> {
        T getValueScratchByOrd(long ordinal, T spare);

    }
    /**
     * 
     */
    public static abstract class Enum<T> {

        public static final Object NO_MORE_VALUES = null;
        protected Scratchable<T> filler;
        protected T next = null;
        protected T sharedInstance;
        
        public Enum(Scratchable<T> filler) {
            this.sharedInstance = filler.newScratch();
            this.filler = filler; 
        }

        /**
         * Resets the iterator to the given doc ID
         */
        public abstract int reset(int docId);


        /**
         * Returns the next value for the current document or
         * {@link #NO_MORE_VALUES} if the iterator has been exhausted for the
         * current document. Note: The returned instances are shared instance
         * and must not be used in auxilary datastructures or modifed in the
         * consumer code.
         */
        public abstract T next();
    }

    public Enum<T> newIter(ConsumptionHint hint);
    
    public static class DoubleRef {
        double value;
    }

    public static class LongRef {
        long value;
    }
    
    public static final class SingelValueEnum<T> extends Enum<T> {
        
        public SingelValueEnum(Scratchable<T> filler) {
            super(filler);
        }
        public int reset(int docId) {
            next = filler.getValueScratch(docId, sharedInstance);
            return next == null ? 0 : 1;
        }
        
        public T next() {
            assert next != null;
            T retVal = next;
            next = null;
            return retVal;
        }
    }
    
    public static final class MultiValueIterator<T> extends Enum<T> {
        private Docs docs;
        protected Iter ordsIter;
        protected OrdScratchable<T> filler;

        public MultiValueIterator(OrdScratchable<T> filler, Ordinals.Docs docs) {
            super(filler);
            this.docs = docs;
            this.filler = filler;
        }
        public int reset(int docId) {
            this.ordsIter = docs.getIter(docId);
            return ordsIter.size();
        }
        
        public T next() {
            long next = ordsIter.next();
            assert next != 0;
            return filler.getValueScratchByOrd(next, sharedInstance);
        }
    }
}
