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

package org.elasticsearch.plugin.ingest;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.processor.Processor;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.*;

public class PipelineStore extends AbstractLifecycleComponent {

    /* simonw: the .ingest index do we have a template for this index or are we using defaults? would be good to document that here */
    public final static String INDEX = ".ingest";
    public final static String TYPE = "pipeline";

    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final TimeValue pipelineUpdateInterval;
    private final PipelineStoreClient client;
    private final Pipeline.Factory factory = new Pipeline.Factory();
    private final Map<String, Processor.Factory> processorFactoryRegistry;
    private volatile Map<String, PipelineReference> pipelines = new HashMap<>();

    @Inject
    public PipelineStore(Settings settings, ThreadPool threadPool, Environment environment, ClusterService clusterService, PipelineStoreClient client, Map<String, Processor.Factory> processors) {
        super(settings);
        this.threadPool = threadPool;
        // simonw: it would be really nice if we could not depend on ClusterService, I think this interface makes a lot of things pretty complicated since it's hard to mock and replace in tests
        // what I think we should do is have a class on top of this class that does all the wireing. That way we can in the top-level class register a listener and refresh stuff if needed.
        // we can also remove the dependency from ThreadPool since we might not need to do the scheduling in here but in the top level. We can also as a next step only implement Closeable instead
        // AbstractLifecycleComponent and have that way a very simple and clean class that is easy to test.
        this.clusterService = clusterService;
        this.pipelineUpdateInterval = settings.getAsTime("ingest.pipeline.store.update.interval", TimeValue.timeValueSeconds(1));
        this.client = client;
        for (Processor.Factory factory : processors.values()) {
            factory.setConfigDirectory(environment.configFile());
        }
        this.processorFactoryRegistry = Collections.unmodifiableMap(processors);
        clusterService.add(new PipelineStoreListener());
    }

    @Override
    protected void doStart() {
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() {
        // use IOUtils to release closeables
        for (Processor.Factory factory : processorFactoryRegistry.values()) {
            try {
                factory.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public Pipeline get(String id) {
        PipelineReference ref = pipelines.get(id);
        if (ref != null) {
            return ref.getPipeline();
        } else {
            // simonw: maybe we should just throw an exception and fail here?
            return null;
        }
    }

    public Map<String, Processor.Factory> getProcessorFactoryRegistry() {
        return processorFactoryRegistry;
    }

    public List<PipelineReference> getReference(String... ids) {
        List<PipelineReference> result = new ArrayList<>(ids.length);
        for (String id : ids) {
            if (Regex.isSimpleMatchPattern(id)) {
                for (Map.Entry<String, PipelineReference> entry : pipelines.entrySet()) {
                    if (Regex.simpleMatch(id, entry.getKey())) {
                        result.add(entry.getValue());
                    }
                }
            } else {
                PipelineReference reference = pipelines.get(id);
                if (reference != null) {
                    result.add(reference);
                }
            }
        }
        return result;
    }

    public Pipeline constructPipeline(String id, Map<String, Object> config) throws IOException {
        return factory.create(id, config, processorFactoryRegistry);
    }

    /* simonw: can we make this synchronized otherwise this is prone to race-conditions */
    void updatePipelines() throws IOException {
        // note: this process isn't fast or smart, but the idea is that there will not be many pipelines,
        // so for that reason the goal is to keep the update logic simple.

        int changed = 0;
        Map<String, PipelineReference> newPipelines = new HashMap<>(pipelines);
        for (SearchHit hit : client.readAllPipelines()) {
            String pipelineId = hit.getId();
            BytesReference pipelineSource = hit.getSourceRef();
            PipelineReference previous = newPipelines.get(pipelineId);
            if (previous != null) {
                if (previous.getSource().equals(pipelineSource)) {
                    continue;
                }
            }

            changed++;
            Pipeline pipeline = constructPipeline(hit.getId(), hit.sourceAsMap());
            newPipelines.put(pipelineId, new PipelineReference(pipeline, hit.getVersion(), pipelineSource));
        }

        int removed = 0;
        for (String existingPipelineId : pipelines.keySet()) {
            if (!client.existPipeline(existingPipelineId)) {
                newPipelines.remove(existingPipelineId);
                removed++;
            }
        }

        if (changed != 0 || removed != 0) {
            logger.debug("adding or updating [{}] pipelines and [{}] pipelines removed", changed, removed);
            pipelines = newPipelines;
        } else {
            logger.debug("no pipelines changes detected");
        }
    }

    /* simonw: why is this an async process - can't we make this sync somehow without the update interval
       and force folks to kick off an update or in the put action we can just notify all nodes to refresh?
       I really like the fact that we in-general are getting rid of things that are async since they are very very hard to test.
       Bottom line I think we should make this not a scheduled task but a simple API that gets called from a transport action we can
       also have a dedicated refresh API that is called after the a pipline is modified...*/
    void startUpdateWorker() {
        if (lifecycleState() == Lifecycle.State.STARTED) {
            threadPool.schedule(pipelineUpdateInterval, ThreadPool.Names.GENERIC, new Updater());
        }
    }

    class Updater implements Runnable {

        @Override
        public void run() {
            try {
                updatePipelines();
            } catch (Exception e) {
                logger.error("pipeline store update failure", e);
            } finally {
                startUpdateWorker();
            }
        }

    }

    class PipelineStoreListener implements ClusterStateListener {

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) == false) {
                startUpdateWorker();
                clusterService.remove(this);
            }
        }
    }

    public static class PipelineReference {

        private final Pipeline pipeline;
        private final long version;
        private final BytesReference source;

        PipelineReference(Pipeline pipeline, long version, BytesReference source) {
            this.pipeline = pipeline;
            this.version = version;
            this.source = source;
        }

        public Pipeline getPipeline() {
            return pipeline;
        }

        public long getVersion() {
            return version;
        }

        /* simonw: do we need to expose this - it's mutable and very lowlevel? */
        public BytesReference getSource() {
            return source;
        }

        // simonw: hmm seems like we have to compare version too?
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PipelineReference holder = (PipelineReference) o;
            return source.equals(holder.source);
        }

        @Override
        public int hashCode() {
            return source.hashCode();
        }
    }

}
