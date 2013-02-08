package org.elasticsearch.index.fielddata.fieldcomparator;
/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LongValues;

abstract class LongValuesComparatorBase<T extends Number> extends FieldComparator<T> {

    protected final IndexNumericFieldData indexFieldData;
    protected final long missingValue;
    protected long bottom;
    protected LongValues readerValues;
    protected final boolean reversed;

    public LongValuesComparatorBase(IndexNumericFieldData indexFieldData, long missingValue, boolean reversed) {
        this.indexFieldData = indexFieldData;
        this.missingValue = missingValue;
        this.reversed = reversed;
    }

    @Override
    public final int compareBottom(int doc) throws IOException {
        long v2 = readerValues.getValueMissing(doc, missingValue);

        if (bottom > v2) {
            return 1;
        } else if (bottom < v2) {
            return -1;
        } else {
            return 0;
        }
    }
  
    @Override
    public final int compareDocToValue(int doc, T valueObj) throws IOException {
        final long value = valueObj.longValue();
        long docValue = readerValues.getValueMissing(doc, missingValue);
        if (docValue < value) {
            return -1;
        } else if (docValue > value) {
            return 1;
        } else {
            return 0;
        }
    }
    
    @Override
    public final FieldComparator<T> setNextReader(AtomicReaderContext context) throws IOException {
        readerValues = indexFieldData.load(context).getLongValues();
        if (readerValues.isMultiValued()) {
            readerValues = new MultiValuedBytesWrapper(readerValues, reversed);
        }
        return this;
    }

    private static final class MultiValuedBytesWrapper extends LongValues.FilteredByteValues {

        private final boolean reversed;

        public MultiValuedBytesWrapper(LongValues delegate, boolean reversed) {
            super(delegate);
            this.reversed = reversed;
        }

        @Override
        public long getValueMissing(int docId, long missing) {
            LongValues.Iter iter = delegate.getIter(docId);
            if (!iter.hasNext()) {
                return missing;
            }

            long currentVal = iter.next();
            long relevantVal = currentVal;
            while (true) {
                if (reversed) {
                    if (currentVal > relevantVal) {
                        relevantVal = currentVal;
                    }
                } else {
                    if (currentVal < relevantVal) {
                        relevantVal = currentVal;
                    }
                }
                if (!iter.hasNext()) {
                    break;
                }
                currentVal = iter.next();
            }
            return relevantVal;
            // If we have a method on readerValues that tells if the values emitted by Iter or ArrayRef are sorted per
            // document that we can do this or something similar:
            // (This is already possible, if values are loaded from index, but we just need a method that tells us this
            // For example a impl that read values from the _source field might not read values in order)
            /*if (reversed) {
                // Would be nice if there is a way to get highest value from LongValues. The values are sorted anyway.
                LongArrayRef ref = readerValues.getValues(doc);
                if (ref.isEmpty()) {
                    return missing;
                } else {
                    return ref.values[ref.end - 1]; // last element is the highest value.
                }
            } else {
                return readerValues.getValueMissing(doc, missing); // returns lowest
            }*/
        }

    }

}
