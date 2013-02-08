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

package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.index.fielddata.util.LongArrayRef;

import java.io.IOException;

/**
 */
public class LongValuesComparator extends LongValuesComparatorBase<Long> {

    protected final long[] values;

    public LongValuesComparator(IndexNumericFieldData indexFieldData, long missingValue, int numHits, boolean reversed) {
        super(indexFieldData, missingValue, reversed);
        this.values = new long[numHits];
    }

    @Override
    public int compare(int slot1, int slot2) {
        final long v1 = values[slot1];
        final long v2 = values[slot2];
        if (v1 > v2) {
            return 1;
        } else if (v1 < v2) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override
    public void setBottom(int slot) {
        this.bottom = values[slot];
    }

    public void copy(int slot, int doc) throws IOException {
        values[slot] = readerValues.getValueMissing(doc, missingValue);
    }
    
    @Override
    public Long value(int slot) {
        return Long.valueOf(values[slot]);
    }
}
