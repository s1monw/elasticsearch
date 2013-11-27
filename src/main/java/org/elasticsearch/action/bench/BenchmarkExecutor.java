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

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.logging.ESLogger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

/**
 */
public class BenchmarkExecutor {

    private final Client client;
    private final ESLogger logger;
    private ImmutableOpenMap<String, StoppableSemaphore> runningBenchmarks = ImmutableOpenMap.of();

    public BenchmarkExecutor(ESLogger logger, Client client) {
        this.client = client;
        this.logger = logger;
    }


    public void abortBenchmark(String benchmarkId) {
        if (!runningBenchmarks.containsKey(benchmarkId)) {
            throw new ElasticSearchException("Benchmark with id [" + benchmarkId + "] is missing");
        }
        StoppableSemaphore semaphore = runningBenchmarks.get(benchmarkId);
        semaphore.stop();
    }

    public BenchResponse benchmark(BenchRequest request) throws ElasticSearchException {
        final StoppableSemaphore semaphore = new StoppableSemaphore(1);
        synchronized (this) {
            if (runningBenchmarks.containsKey(request.benchmarkId())) {
                throw new ElasticSearchException("Benchmark with id [" + request.benchmarkId() + "] is already running");
            }
            runningBenchmarks = ImmutableOpenMap.builder(runningBenchmarks).fPut(request.benchmarkId(), semaphore).build();
        }
        try {
            final List<SearchRequest> baseSearchRequests = request.searchRequests();

            final List<String> errorMessages = new ArrayList<String>();
            final List<BenchSpec> competiors = request.competiors();
            final List<BenchSpecResult> results = new ArrayList<BenchSpecResult>();
            long warmUpTook = -1;
            BenchResponse benchResponse = new BenchResponse(results);
            try {
                for (BenchSpec competitor : competiors) {
                    int iterations = competitor.iterations();

                    final List<BenchSpecResult.Iteration> iterationResults = new ArrayList<BenchSpecResult.Iteration>(Math.min(100, iterations));
                    final List<SearchRequest> searchRequests = competitor.searchRequests().isEmpty() ? baseSearchRequests : competitor.searchRequests();
                    prepareRequests(searchRequests, competitor.defaultSearchRequest(), competitor.searchType());
                    if (competitor.warmup()) {
                        final long beforeWarmup = System.nanoTime();
                        final List<String> warmUpErrors = warmUp(competitor, searchRequests, semaphore);
                        final long aftereWarmup = System.nanoTime();
                        warmUpTook = TimeUnit.MILLISECONDS.convert(aftereWarmup - beforeWarmup, TimeUnit.NANOSECONDS);
                        if (!warmUpErrors.isEmpty()) {
                            BenchSpecResult compResult = new BenchSpecResult(competitor.name(), iterationResults, iterations, competitor.multiplier(), 0, competitor.concurrency());
                            compResult.warmupTook(warmUpTook);
                            results.add(compResult);
                            benchResponse.failures(warmUpErrors.toArray(Strings.EMPTY_ARRAY));
                            return benchResponse;
                        }
                    }

                    int requestsPerRound = competitor.multiplier() * searchRequests.size();
                    //NOCOMMIT: do we need an upperbound here where we reject maybe based on the heap?
                    final long[] timeBuckets = new long[requestsPerRound];
                    final long[] docBuckets = new long[requestsPerRound];

                    for (int i = 0; i < iterations; i++) {
                        if (competitor.clearCaches() != null || request.clearCaches() != null) {
                            try {
                                if (competitor.clearCaches() != null) {
                                    client.admin().indices().clearCache(competitor.clearCaches()).get();
                                } else {
                                    client.admin().indices().clearCache(request.clearCaches()).get();
                                }
                            } catch (ExecutionException e) {
                                logger.info("Failed to clear caches before benchmark round");
                                throw new ElasticSearchException("Failed to clear caches before benchmark round", e);
                            }

                        }
                        BenchSpecResult.Iteration benchmark = runIteration(competitor, searchRequests, timeBuckets, docBuckets, errorMessages, semaphore);
                        if (errorMessages.isEmpty()) {
                            iterationResults.add(benchmark);
                        } else {
                            final int totalQueries = competitor.multiplier() * searchRequests.size() * (1 + i);
                            BenchSpecResult compResult = new BenchSpecResult(competitor.name(), iterationResults, iterations, competitor.multiplier(), totalQueries, competitor.concurrency());
                            compResult.warmupTook(warmUpTook);
                            results.add(compResult);
                            benchResponse.failures(errorMessages.toArray(Strings.EMPTY_ARRAY));
                            return benchResponse;
                        }
                    }
                    final int totalQueries = competitor.multiplier() * searchRequests.size() * iterations;
                    final BenchSpecResult compResult = new BenchSpecResult(competitor.name(), iterationResults, iterations, competitor.multiplier(), totalQueries, competitor.concurrency());
                    compResult.warmupTook(warmUpTook);
                    results.add(compResult);
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                benchResponse.aborted(true);
            }
            return benchResponse;
        } finally {
            synchronized (this) {
                if (!runningBenchmarks.containsKey(request.benchmarkId())) {
                    throw new ElasticSearchException("Benchmark with id [" + request.benchmarkId() + "] is missing");
                }
                semaphore.stop();
                runningBenchmarks = ImmutableOpenMap.builder(runningBenchmarks).fRemove(request.benchmarkId()).build();
            }
        }
    }

    private List<String> warmUp(BenchSpec competitor, List<SearchRequest> searchRequests, StoppableSemaphore stoppableSemaphore) throws InterruptedException {
        final StoppableSemaphore semaphore = stoppableSemaphore.reset(competitor.concurrency());
        final CountDownLatch totalCount = new CountDownLatch(searchRequests.size());
        final CopyOnWriteArrayList<String> errorMessages = new CopyOnWriteArrayList<String>();
        for (SearchRequest searchRequest : searchRequests) {
            semaphore.acquire();
            client.search(searchRequest, new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    semaphore.release();
                    totalCount.countDown();
                }

                @Override
                public void onFailure(Throwable e) {
                    semaphore.release();
                    totalCount.countDown();
                    if (errorMessages.size() < 5) {
                        e = ExceptionsHelper.unwrapCause(e);
                        errorMessages.add(e.getLocalizedMessage());
                    }

                }
            });
        }
        totalCount.await();
        return errorMessages;
    }

    private void prepareRequests(List<SearchRequest> requests, SearchRequest defaultSearchRequest, final SearchType searchType) {
        if (defaultSearchRequest != null) {
            defaultSearchRequest.beforeStart(); // make safe
        }
        for (SearchRequest searchRequest : requests) {
            if (defaultSearchRequest != null) {
                searchRequest.extraSource(defaultSearchRequest.source(), true);
            }
            if (searchType != SearchType.DEFAULT) {
                searchRequest.searchType(searchType);
            }
        }
    }

    private BenchSpecResult.Iteration runIteration(BenchSpec competitor, List<SearchRequest> searchRequests, final long[] timeBuckets, final long[] docBuckets, List<String> errors, StoppableSemaphore stoppableSemaphore) throws InterruptedException {
        assert timeBuckets.length == competitor.multiplier() * searchRequests.size();
        final StoppableSemaphore semaphore = stoppableSemaphore.reset(competitor.concurrency());
        Arrays.fill(timeBuckets, -1); // wipe CPU cache     ;)
        Arrays.fill(docBuckets, -1); // wipe CPU cache      ;)
        int id = 0;
        final CountDownLatch totalCount = new CountDownLatch(timeBuckets.length);
        final CopyOnWriteArrayList<String> errorMessages = new CopyOnWriteArrayList<String>();
        final long beforeRun = System.nanoTime();
        for (int i = 0; i < competitor.multiplier(); i++) {
            for (SearchRequest searchRequest : searchRequests) {
                StatisticCollectionActionListener statsListener = new StatisticCollectionActionListener(semaphore, timeBuckets, docBuckets, id++, totalCount, errorMessages);
                semaphore.acquire();
                client.search(searchRequest, statsListener);
            }
        }
        totalCount.await();
        assert id == timeBuckets.length;
        final long afterRun = System.nanoTime();
        if (!errorMessages.isEmpty()) {
            errors.addAll(errorMessages);
            return null;
        }
        assert assertBuckets(timeBuckets); // make sure they are all set
        assert assertBuckets(docBuckets); // make sure they are all set


        final long totalTime = TimeUnit.MILLISECONDS.convert(afterRun - beforeRun, TimeUnit.NANOSECONDS);
        BenchStatistics statistics = new BenchStatistics(timeBuckets);
        final long sumDocs = new BenchStatistics(docBuckets).sum();

        BenchSpecResult.SlowestRequests topN = competitor.numSlowest() > 0 ? getTopN(timeBuckets, searchRequests, competitor.multiplier(), competitor.numSlowest()) : null;
        BenchSpecResult.Iteration round = new BenchSpecResult.Iteration(topN, totalTime, timeBuckets.length, sumDocs, statistics);
        return round;
    }

    private final boolean assertBuckets(long[] buckets) { // assert only
        for (int i = 0; i < buckets.length; i++) {
            assert buckets[i] >= 0 : "Bucket value was negative: " + buckets[i];
        }
        return true;

    }

    private BenchSpecResult.SlowestRequests getTopN(long[] buckets, List<SearchRequest> requests, int multiplier, int topN) {
        final int numRequests = requests.size();
        // collect the top N
        final PriorityQueue<IndexAndTime> topNQueue = new PriorityQueue<IndexAndTime>(topN) {
            @Override
            protected boolean lessThan(IndexAndTime a, IndexAndTime b) {
                return a.avgTime < b.avgTime;
            }
        };
        assert multiplier > 0;
        for (int i = 0; i < numRequests; i++) {
            long sum = 0;
            long max = Long.MIN_VALUE;
            for (int j = 0; j < multiplier; j++) {
                // NOCOMMIT should we use an average here or have an option for min/max/avg/sum? - we might even discard the slowest?
                final int base = (numRequests * j);
                sum += buckets[i + base];
                max = Math.max(buckets[i + base], max);
            }
            final long avg = sum / multiplier;
            if (topNQueue.size() < topN) {
                topNQueue.add(new IndexAndTime(i, max, avg));
            } else if (topNQueue.top().avgTime < max) {
                topNQueue.top().update(i, max, avg);
                topNQueue.updateTop();

            }
        }
        final long[] avgTimes = new long[topNQueue.size()];
        final long[] maxTimes = new long[topNQueue.size()];

        final SearchRequest[] topNRequests = new SearchRequest[topNQueue.size()];
        int i = avgTimes.length - 1;
        while (topNQueue.size() > 0) {
            IndexAndTime pop = topNQueue.pop();
            avgTimes[i] = pop.avgTime;
            maxTimes[i] = pop.maxTime;
            topNRequests[i--] = requests.get(pop.index);
        }
        return new BenchSpecResult.SlowestRequests(topNRequests, maxTimes, avgTimes);
    }
    private static class IndexAndTime {
        int index;
        long maxTime;
        long avgTime;

        public IndexAndTime(int index, long maxTime, long avgTime) {
            this.index = index;
            this.maxTime = maxTime;
            this.avgTime = avgTime;
        }

        public void update(int index, long maxTime, long avgTime) {
            this.index = index;
            this.maxTime = maxTime;
            this.avgTime = avgTime;
        }
    }

    private class StatisticCollectionActionListener implements ActionListener<SearchResponse> {

        private final StoppableSemaphore semaphore;
        private final long[] timeBuckets;
        private final int bucketId;
        private final CountDownLatch totalCount;
        private final CopyOnWriteArrayList<String> errorMessages;
        private final long[] docBuckets;

        public StatisticCollectionActionListener(StoppableSemaphore semaphore, long[] timeBuckets, long[] docs, int bucketId, CountDownLatch totalCount, CopyOnWriteArrayList<String> errorMessages) {
            this.semaphore = semaphore;
            this.bucketId = bucketId;
            this.timeBuckets = timeBuckets;
            this.docBuckets = docs;
            this.totalCount = totalCount;
            this.errorMessages = errorMessages;
        }

        @Override
        public void onResponse(SearchResponse searchResponse) {
            semaphore.release();
            timeBuckets[bucketId] = searchResponse.getTookInMillis();
            if (searchResponse.getHits() != null) {
                docBuckets[bucketId] = searchResponse.getHits().getTotalHits();
            }
            totalCount.countDown();

        }

        @Override
        public void onFailure(Throwable e) {
            semaphore.release();
            timeBuckets[bucketId] = -1;
            docBuckets[bucketId] = -1;
            totalCount.countDown();
            if (errorMessages.size() < 5) {
                e = ExceptionsHelper.unwrapCause(e);
                errorMessages.add(e.getLocalizedMessage());
            }
        }
    }

    private final static class StoppableSemaphore {
        private Semaphore semaphore;
        private volatile boolean stopped = false;

        public StoppableSemaphore(int concurrency) {
            semaphore = new Semaphore(concurrency);
        }

        public StoppableSemaphore reset(int concurrency) {
            semaphore = new Semaphore(concurrency);
            return this;
        }

        public void acquire() throws InterruptedException {
            if (stopped) {
                throw new InterruptedException("Benchmark Interrupted");
            }
            semaphore.acquire();
        }

        public void release() {
            semaphore.release();
        }

        public void stop() {
            stopped = true;
        }

        public boolean isRunning() {
            return stopped;
        }
    }
}
