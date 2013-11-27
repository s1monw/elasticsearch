package org.elasticsearch.action.bench;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProcessedClusterStateUpdateTask;
import org.elasticsearch.cluster.TimeoutClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.BenchmarkMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

public class BenchService extends AbstractLifecycleComponent<BenchService> {
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final BenchmarkExecutor executor;

    @Inject
    public BenchService(Settings settings, ClusterService clusterService, ThreadPool threadPool,
                        Client client, TransportService transportService) {
        super(settings);
        this.threadPool = threadPool;
        this.executor = new BenchmarkExecutor(logger, client);
        this.clusterService = clusterService;
        this.transportService = transportService;
        transportService.registerHandler(BenchExecutionHandler.ACTION, new BenchExecutionHandler());
        transportService.registerHandler(AbortExecutionHandler.ACTION, new AbortExecutionHandler());
    }


    public boolean isLocalNode(DiscoveryNode node) {
        return clusterService.state().nodes().getLocalNode().equals(node);
    }

    @Override
    protected void doStart() throws ElasticSearchException {
    }

    @Override
    protected void doStop() throws ElasticSearchException {
    }

    @Override
    protected void doClose() throws ElasticSearchException {
    }

    public void abortBenchmark(final String id, final ActionListener<AbortBenchResponse> listener) {
        BenchmarkStateListener benchmarkStateListener = new BenchmarkStateListener() {
            @Override
            public void onResponse(final ClusterState newState, final BenchmarkMetaData.Entry entry) {
                listener.onResponse(new AbortBenchResponse(true));
                threadPool.executor(ThreadPool.Names.GENERIC).execute(new Runnable() {
                    @Override
                    public void run() {
                        final ImmutableOpenMap<String, DiscoveryNode> nodes = newState.nodes().nodes();
                        for (String nodeId : entry.nodes()) {
                            final DiscoveryNode node = nodes.get(nodeId);
                            if (node != null) {
                                transportService.sendRequest(node, AbortExecutionHandler.ACTION, new NodeAbortRequest(id), EmptyTransportResponseHandler.INSTANCE_SAME);
                            }   else {
                                logger.info("Node for ID [" + nodeId + "] not found in cluster state - skipping");
                            }
                        }
                    }
                });
            }

            @Override
            public void onFailure(Throwable t) {
                listener.onFailure(t);
            }
        };
        AbortBenchmarkTask abortBenchmarkTask = new AbortBenchmarkTask(id, benchmarkStateListener);
        clusterService.submitStateUpdateTask(abortBenchmarkTask.reason(), abortBenchmarkTask);
    }


    public void startBenchmark(final BenchRequest request, final ActionListener<BenchResponse> listener) {
        final BenchmarkStateListener benchListener = new BenchmarkStateListener() {
            @Override
            public void onResponse(final ClusterState newState, final BenchmarkMetaData.Entry entry) {
                threadPool.executor(ThreadPool.Names.GENERIC).execute(new Runnable() {
                    @Override
                    public void run() {
                        final ImmutableOpenMap<String, DiscoveryNode> nodes = newState.nodes().nodes();
                        final AsyncHandler asyncHandler = new AsyncHandler(entry.nodes().length, request, listener);
                        for (String nodeId : entry.nodes()) {
                            final DiscoveryNode node = nodes.get(nodeId);
                            if (node == null) {
                                asyncHandler.handleExceptionInternal(new ElasticSearchException("Node for ID [" + nodeId + "] not found in cluster state - skipping"));
                            }
                            transportService.sendRequest(node, BenchExecutionHandler.ACTION, new NodeBenchRequest(request), asyncHandler);
                        }
                    }
                });
            }
            @Override
            public void onFailure(Throwable t) {
                listener.onFailure(t);

            }
        };
        clusterService.submitStateUpdateTask("start_benchmark", new StartBenchmarkTask(request, benchListener));
    }

    private void finishBenchmark(final BenchResponse benchResponse, final String benchmarkId, final ActionListener<BenchResponse> listener) {

        clusterService.submitStateUpdateTask("finish_benchmark", new FinishBenchmarkTask("finish_benchmark", benchmarkId, new BenchmarkStateListener() {
            @Override
            public void onResponse(ClusterState newClusterState, BenchmarkMetaData.Entry changed) {
                listener.onResponse(benchResponse);
            }

            @Override
            public void onFailure(Throwable t) {
                listener.onFailure(t);

            }
        }, !benchResponse.isAborted() && !benchResponse.hasFailures()));
    }

    private final boolean isBenchmarkNode(DiscoveryNode node) {
        ImmutableMap<String, String> attributes = node.getAttributes();
        if (attributes.containsKey("bench")) {
            String bench = attributes.get("bench");
            return Boolean.parseBoolean(bench);
        }
        return false;
    }

    private List<DiscoveryNode> findNodes(BenchRequest request) {
        final int numNodes = request.numExecutors();
        final DiscoveryNodes nodes = clusterService.state().nodes();
        DiscoveryNode localNode = nodes.localNode();
        List<DiscoveryNode> benchmarkNodes = new ArrayList<DiscoveryNode>();
        if (isBenchmarkNode(localNode)) {
            benchmarkNodes.add(localNode);
        }
        for (DiscoveryNode node : nodes) {
            if (benchmarkNodes.size() >= numNodes) {
                return benchmarkNodes;
            }
            if (node != localNode && isBenchmarkNode(node)) {
                benchmarkNodes.add(node);
            }
        }
        return benchmarkNodes;
    }

    private class BenchExecutionHandler extends BaseTransportRequestHandler<NodeBenchRequest> {

        static final String ACTION = "bench/run";

        @Override
        public NodeBenchRequest newInstance() {
            return new NodeBenchRequest();
        }

        @Override
        public void messageReceived(NodeBenchRequest request, TransportChannel channel) throws Exception {
            try {
                BenchResponse response = executor.benchmark(request.request);
                channel.sendResponse(response);
            } catch (Throwable ex) {
                logger.info("FAILED", ex);
                throw new Exception(ex);
            }
        }

        @Override
        public String executor() {
            return ThreadPool.Names.GENERIC; // NOCOMMIT does it need it's own  ThreadPool?
        }
    }

    private class AbortExecutionHandler extends BaseTransportRequestHandler<NodeAbortRequest> {

        static final String ACTION = "bench/abort";

        @Override
        public NodeAbortRequest newInstance() {
            return new NodeAbortRequest();
        }

        @Override
        public void messageReceived(NodeAbortRequest request, TransportChannel channel) throws Exception {
            executor.abortBenchmark(request.benchmarkId);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.GENERIC; // NOCOMMIT does it need it's own  ThreadPool?
        }
    }

    public static class NodeAbortRequest extends TransportRequest {
        private String benchmarkId;

        public NodeAbortRequest(String benchmarkId) {
            this.benchmarkId = benchmarkId;
        }

        public NodeAbortRequest() {
            this(null);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            benchmarkId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(benchmarkId);
        }
    }


    public static class NodeBenchRequest extends TransportRequest {
        final BenchRequest request;

        public NodeBenchRequest(BenchRequest request) {
            this.request = request;
        }

        public NodeBenchRequest() {
            this(new BenchRequest());
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            request.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }

    private class AsyncHandler implements TransportResponseHandler<BenchResponse> {
        private final CopyOnWriteArrayList<Throwable> failures = new CopyOnWriteArrayList<Throwable>();
        private final CopyOnWriteArrayList<BenchResponse> responses = new CopyOnWriteArrayList<BenchResponse>();
        private final CountDown latch;
        private final ActionListener<BenchResponse> listener;
        private final BenchRequest request;

        public AsyncHandler(int size, BenchRequest request, ActionListener<BenchResponse> listener) {
            latch = new CountDown(size);
            this.listener = listener;
            this.request = request;

        }

        @Override
        public BenchResponse newInstance() {
            return new BenchResponse();
        }

        @Override
        public void handleResponse(BenchResponse response) {
            responses.add(response);
            if (latch.countDown()) {
                sendResponse();
            }
        }

        private void sendResponse() {
            List<BenchSpecResult> mergedResults = new ArrayList<BenchSpecResult>();
            BenchResponse benchResponse = new BenchResponse(mergedResults);
            List<String> errorMessages = new ArrayList<String>();
            if (!failures.isEmpty()) {
                for (Throwable e : failures) {
                    e = ExceptionsHelper.unwrapCause(e);
                    errorMessages.add(e.getLocalizedMessage());
                }
            }

            for (BenchResponse r : responses) {
                if (r.hasFailures()) {
                    errorMessages.addAll(Arrays.asList(r.failures()));
                }
                mergedResults.addAll(r.competitorResults());
                if (r.isAborted()) {
                    benchResponse.aborted(true);
                }
            }

            if (!errorMessages.isEmpty()) {
                benchResponse.failures(errorMessages.toArray(Strings.EMPTY_ARRAY));
            }
            finishBenchmark(benchResponse, request.benchmarkId(), listener);
        }

        @Override
        public void handleException(TransportException exp) {
            handleExceptionInternal(exp);
        }

        public void handleExceptionInternal(Throwable t) {
            failures.add(t);
            if (latch.countDown()) {
                sendResponse();
            }
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

    /**
     * Listener for delete snapshot operation
     */
    public static interface BenchmarkStateListener {

        /**
         * Called if delete operation was successful
         */
        void onResponse(ClusterState newClusterState, BenchmarkMetaData.Entry changed);

        /**
         * Called if delete operation failed
         */
        void onFailure(Throwable t);
    }

    public final class StartBenchmarkTask extends BenchmarkStateChangeAction<BenchRequest> {

        private final BenchmarkStateListener stateListener;
        private BenchmarkMetaData.Entry newBenchmark = null;

        public StartBenchmarkTask(BenchRequest request, BenchmarkStateListener stateListener) {
            super(request);
            this.stateListener = stateListener;
        }

        @Override
        public ClusterState execute(ClusterState currentState) {
            MetaData metaData = currentState.getMetaData();
            BenchmarkMetaData bmd = metaData.custom(BenchmarkMetaData.TYPE);
            MetaData.Builder mdBuilder = MetaData.builder(metaData);
            ImmutableList.Builder<BenchmarkMetaData.Entry> builder = ImmutableList.builder();

            if (bmd != null) {
                for (BenchmarkMetaData.Entry entry : bmd.entries()) {
                    if (request.benchmarkId().equals(entry.benchmarkId())){
                        if (entry.state() != BenchmarkMetaData.State.SUCCESS && entry.state() != BenchmarkMetaData.State.FAILED) {
                            throw new ElasticSearchException("a benchmark with benchmark ID [" + request.benchmarkId() + "] is already running in state [" + entry.state() + "]");
                        }
                        // just drop the entry it it has finished successfully or it failed!
                    } else {
                        builder.add(entry);
                    }
                }
            }
            logger.debug("Starting benchmark for ID [{}]", request.benchmarkId());
            List<DiscoveryNode> nodes = findNodes(request);
            String[] nodeIds = new String[nodes.size()];
            int i = 0;
            for (DiscoveryNode node : nodes) {
                nodeIds[i++] = node.getId();
            }
            newBenchmark = new BenchmarkMetaData.Entry(request.benchmarkId(), BenchmarkMetaData.State.STARTED, nodeIds);
            bmd = new BenchmarkMetaData(builder.add(newBenchmark).build());
            mdBuilder.putCustom(BenchmarkMetaData.TYPE, bmd);
            return ClusterState.builder(currentState).metaData(mdBuilder).build();
        }

        @Override
        public void onFailure(String source, Throwable t) {
            logger.warn("failed to start benchmark: [{}]", t, request.benchmarkId());
            newBenchmark = null;
            stateListener.onFailure(t);
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, final ClusterState newState) {
            if (newBenchmark != null) {
                stateListener.onResponse(newState, newBenchmark);
            }
        }

        @Override
        public TimeValue timeout() {
            return request.masterNodeTimeout();
        }
    }

    public final class FinishBenchmarkTask extends UpdateBenchmarkStateTask {

        private final boolean success;

        public FinishBenchmarkTask(String reason, String benchmarkId, BenchmarkStateListener listener, boolean success) {
            super(reason, benchmarkId, listener);
            this.success = success;
        }

        @Override
        protected BenchmarkMetaData.Entry process(BenchmarkMetaData.Entry entry) {
            BenchmarkMetaData.State state = entry.state();
            assert state == BenchmarkMetaData.State.STARTED || state == BenchmarkMetaData.State.ABORTED :  "Expected state: STARTED or ABORTED but was: " + entry.state();
            if (success) {
                return new BenchmarkMetaData.Entry(entry, BenchmarkMetaData.State.SUCCESS);
            } else {
                return new BenchmarkMetaData.Entry(entry, BenchmarkMetaData.State.FAILED);
            }
        }
    }

    public final class AbortBenchmarkTask extends UpdateBenchmarkStateTask {
        public AbortBenchmarkTask(String benchmarkId, BenchmarkStateListener listener) {
            super("abort_benchmark", benchmarkId, listener);
        }

        @Override
        protected BenchmarkMetaData.Entry process(BenchmarkMetaData.Entry entry) {
            BenchmarkMetaData.State state = entry.state();
            if (state != BenchmarkMetaData.State.STARTED) {
                throw new ElasticSearchIllegalStateException("Can't abort benchmark for id: [" + benchmarkId + "] - illegal state [" + state + "]");
            }
            return new BenchmarkMetaData.Entry(entry, BenchmarkMetaData.State.ABORTED);
        }
    }

    public final class PruneBenchmarkTask extends UpdateBenchmarkStateTask {

        public PruneBenchmarkTask(String reason, BenchmarkStateListener listener) {
            super(reason, listener);
        }

        @Override
        protected BenchmarkMetaData.Entry process(BenchmarkMetaData.Entry entry) {
            BenchmarkMetaData.State state = entry.state();
            switch (state) {
                case SUCCESS:
                case FAILED:
                    return null;
                default:
                    return entry;
            }
        }
    }


    public abstract class UpdateBenchmarkStateTask implements ProcessedClusterStateUpdateTask {

        private final String reason;
        protected final String benchmarkId;
        private final BenchmarkStateListener listener;
        private BenchmarkMetaData.Entry instance;

        protected UpdateBenchmarkStateTask(String reason, BenchmarkStateListener listener) {
            this(reason, null, listener);
        }

        protected UpdateBenchmarkStateTask(String reason, String benchmarkId, BenchmarkStateListener listener) {
            this.reason = reason;
            this.listener = listener;
            this.benchmarkId = benchmarkId;
        }

        @Override
        public ClusterState execute(ClusterState currentState) {
            MetaData metaData = currentState.getMetaData();
            BenchmarkMetaData bmd = metaData.custom(BenchmarkMetaData.TYPE);
            MetaData.Builder mdBuilder = MetaData.builder(metaData);
            if (bmd != null && !bmd.entries().isEmpty()) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Removing benchmark state for ID [{}] reason [{}]", benchmarkId, reason);
                }
                ImmutableList.Builder<BenchmarkMetaData.Entry> builder = new ImmutableList.Builder<BenchmarkMetaData.Entry>();
                boolean found = false;
                for (BenchmarkMetaData.Entry e : bmd.entries()) {
                    if (benchmarkId == null || e.benchmarkId().equals(benchmarkId)) {
                        e = process(e);
                        if (benchmarkId != null) {
                            assert instance == null : "Illegal state more than one benchmark with he same id [" + benchmarkId + "]";
                            instance = e;
                        }
                        found = true;
                    }
                    if (e != null) {
                        builder.add(e);
                    }
                }
                if (!found) {
                    throw new ElasticSearchException("No Benchmark found for id: [" + benchmarkId + "]");
                }
                bmd = new BenchmarkMetaData(builder.build());
            } else if (benchmarkId != null) {
                throw new ElasticSearchException("No Benchmark found for id: [" + benchmarkId + "]");
            }
            mdBuilder.putCustom(BenchmarkMetaData.TYPE, bmd);
            return ClusterState.builder(currentState).metaData(mdBuilder).build();
        }

        protected abstract BenchmarkMetaData.Entry process(BenchmarkMetaData.Entry entry);

        @Override
        public void onFailure(String source, Throwable t) {
            logger.warn("Failed updating benchmark state for ID [{}] triggered by: [{}]", t, benchmarkId, reason);
            listener.onFailure(t);
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, final ClusterState newState) {
            listener.onResponse(newState, instance);
        }

        public String reason() {
            return reason;
        }
    }

    public abstract class BenchmarkStateChangeAction<R extends MasterNodeOperationRequest> implements TimeoutClusterStateUpdateTask {
        protected final R request;

        public BenchmarkStateChangeAction(R request) {
            this.request = request;
        }

        @Override
        public TimeValue timeout() {
            return request.masterNodeTimeout();
        }
    }

}
