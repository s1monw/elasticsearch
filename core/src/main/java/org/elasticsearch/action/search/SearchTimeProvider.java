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
package org.elasticsearch.action.search;

import java.util.function.LongSupplier;

/**
 * Search operations need two clocks. One clock is to fulfill real clock needs (e.g., resolving
 * "now" to an index name). Another clock is needed for measuring how long a search operation
 * took. These two uses are at odds with each other. There are many issues with using a real
 * clock for measuring how long an operation took (they often lack precision, they are subject
 * to moving backwards due to NTP and other such complexities, etc.). There are also issues with
 * using a relative clock for reporting real time. Thus, we simply separate these two uses.
 */
final class SearchTimeProvider {

    private final long absoluteStartMillis;
    private final long relativeStartNanos;
    private final LongSupplier relativeCurrentNanosProvider;

    /**
     * Instantiates a new search time provider. The absolute start time is the real clock time
     * used for resolving index expressions that include dates. The relative start time is the
     * start of the search operation according to a relative clock. The total time the search
     * operation took can be measured against the provided relative clock and the relative start
     * time.
     *
     * @param absoluteStartMillis          the absolute start time in milliseconds since the epoch
     * @param relativeStartNanos           the relative start time in nanoseconds
     * @param relativeCurrentNanosProvider provides the current relative time
     */
    SearchTimeProvider(
        final long absoluteStartMillis,
        final long relativeStartNanos,
        final LongSupplier relativeCurrentNanosProvider) {
        this.absoluteStartMillis = absoluteStartMillis;
        this.relativeStartNanos = relativeStartNanos;
        this.relativeCurrentNanosProvider = relativeCurrentNanosProvider;
    }

    long getAbsoluteStartMillis() {
        return absoluteStartMillis;
    }

    long getRelativeStartNanos() {
        return relativeStartNanos;
    }

    long getRelativeCurrentNanos() {
        return relativeCurrentNanosProvider.getAsLong();
    }

}
