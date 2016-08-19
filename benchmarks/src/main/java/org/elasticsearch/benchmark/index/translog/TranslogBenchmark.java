package org.elasticsearch.benchmark.index.translog;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Fork(3)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused") //invoked by benchmarking framework
public class TranslogBenchmark {

    @Param({
        "8k, 10",
        "16k, 10",
        "32k, 10",
        "8k, 5",
        "16k, 5",
        "32k, 5",
    })
    public String parmeter = "8k, 10";

    public ByteSizeValue bufferSize;
    private Thread[] threads;
    private CountDownLatch start;

    @Setup
    public void setup() throws IOException {
        final String[] params = parmeter.split(",");
        bufferSize = ByteSizeValue.parseBytesSizeValue(params[0], "bench param");
        final int numThreads = Integer.parseInt(params[1]);
        Path path = Files.createTempDirectory("tlog-bench");
        Settings build = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetaData metaData = IndexMetaData.builder("_na_").settings(build).build();
        IndexSettings settings = new IndexSettings(metaData, Settings.EMPTY);
        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        TranslogConfig config = new TranslogConfig(new ShardId(settings.getIndex(), 1), path, settings, bigArrays);
        Translog translog = new Translog(config, null);
        this.threads = new Thread[numThreads];
        start = new CountDownLatch(1);
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        start.await();
                        addOps(translog, 10000, 512);
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                }
            };
        }
    }

    public void addOps(Translog translog, int numIters, int sizeByteArray) throws IOException {
        for (int i = 0; i < numIters; i++) {
            translog.add(new Translog.Index("foo", Integer.toString(i), new byte[sizeByteArray]));
        }
    }

    @Benchmark
    public void bench() throws InterruptedException {
        start.countDown();
       for (Thread t : threads) {
           t.join();
       }
    }
}
