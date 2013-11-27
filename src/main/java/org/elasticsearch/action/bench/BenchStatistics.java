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
package org.elasticsearch.action.bench;

import java.util.Arrays;

/*
 * A trival approach to calculate basic statistics over benchmark runs
 */
final class BenchStatistics {
    private final long[] data;
    private Double mean = null;
    private Double variance = null;
    private Long median = null;
    private Long sum = null;
    private long[] sortedCopy;
    private Long max;

    BenchStatistics(long[] data) {
        this.data = data;
    }

    public long mean() {
        if (mean == null) {
            this.mean = (sum() / (double) data.length);
        }
        return mean.longValue();
    }

    public double variance() {
        if (this.variance == null) {
            double mean = mean();
            double temp = 0;
            for (int i = 0; i < data.length; i++) {
                sum += data[i];
                temp += (mean - data[i]) * (mean - data[i]);
            }
            this.variance = temp / data.length;
        }
        return variance.doubleValue();
    }

    public double stdDev() {
        return Math.sqrt(variance());
    }

    public long sum() {
        if (this.sum == null) {
            long sum = 0;
            for (int i = 0; i < data.length; i++) {
                sum += data[i];
            }
            this.sum = sum;
        }
        return this.sum.longValue();
    }

    public long median() {
        if (median == null) {
            median = percentile(0.5);
        }
        return median;
    }
    
    public long max() {
        if (max == null) {
            long maxValue = Long.MIN_VALUE;
            for (int i = 0; i < data.length; i++) {
                maxValue = Math.max(maxValue, data[i]);
            }
            this.max = maxValue;
        }
        return max.longValue();
    }

    public long percentile(double fraction) {
            if (sortedCopy == null) {
                long[] copy = Arrays.copyOf(data, data.length);
                Arrays.sort(copy);
                sortedCopy = copy;
            }
            long percentile;
            long point = sortedCopy[(int)(sortedCopy.length * fraction)];
            if (sortedCopy.length % 2 == 0) {
                percentile = (long)((sortedCopy[((int)(sortedCopy.length * fraction)) - 1] + point) * 0.5);
            } else {
                percentile = sortedCopy[(int)(sortedCopy.length * fraction)];
            }
        return percentile;
    }
}