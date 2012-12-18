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

package org.elasticsearch.cluster.routing.allocation.allocator;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.lucene.util.SorterTemplate;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.FailedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.StartedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.Decision.Type;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

/**
 * 1) Maximize the number of nodes that keep a primary 2) Minimize the number of
 * primaries per node 3) Minimize the distance of the maximal and the minimal
 * number of shards per node 4) Maximize the Number of Indices per Node 5) Keep
 * care of replicas of the same shard on the same Node 6) Minimize the Number of
 * Move-Operations
 */
public class BalancedShardsAllocator extends AbstractComponent implements ShardsAllocator {
    
    public static final String SETTING_TRESHOLD = "cluster.routing.allocation.balance.treshold";
    public static final String SETTING_INDEX_BALANCE_FACTOR = "cluster.routing.allocation.balance.index";
    public static final String SETTING_REPLICA_BALANCE_FACTOR = "cluster.routing.allocation.balance.replica";
    public static final String SETTING_PRIMARY_BALANCE_FACTOR = "cluster.routing.allocation.balance.primary";
    
    
    static {
        MetaData.addDynamicSettings(
                SETTING_INDEX_BALANCE_FACTOR,
                SETTING_PRIMARY_BALANCE_FACTOR,
                SETTING_REPLICA_BALANCE_FACTOR,
                SETTING_TRESHOLD
        );
    }
    
    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            float indexBalance = settings.getAsFloat(SETTING_INDEX_BALANCE_FACTOR, 0.4f);
            float replicaBalance = settings.getAsFloat(SETTING_REPLICA_BALANCE_FACTOR, 0.5f);
            float primaryBalance = settings.getAsFloat(SETTING_PRIMARY_BALANCE_FACTOR, 0.1f);
            BalancedShardsAllocator.this.treshold = settings.getAsFloat(SETTING_TRESHOLD, 1.0f);
            BalancedShardsAllocator.this.balance = new WeightFunction(indexBalance, replicaBalance, primaryBalance);
        }
    }
    
    private volatile WeightFunction balance;
    private volatile float treshold;
    private final ESLogger logger = Loggers.getLogger(getClass());
    
    public BalancedShardsAllocator(Settings settings) {
        this(settings, new NodeSettingsService(settings));
    }
    
    
    @Inject
    public BalancedShardsAllocator(Settings settings, NodeSettingsService nodeSettingsService) {
        super(settings);
        ApplySettings applySettings = new ApplySettings();
        applySettings.onRefreshSettings(settings);
        nodeSettingsService.addListener(applySettings);
    }

    @Override
    public void applyStartedShards(StartedRerouteAllocation allocation) {
        // ONLY FOR GATEWAYS
    }

    @Override
    public void applyFailedShards(FailedRerouteAllocation allocation) {
    }

    @Override
    public boolean allocateUnassigned(RoutingAllocation allocation) {
        return new Balancer(logger, allocation, balance, treshold).balance();
    }

    @Override
    public boolean rebalance(RoutingAllocation allocation) {
        return new Balancer(logger, allocation, balance, treshold).balance();
    }

    @Override
    public boolean move(MutableShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return new Balancer(logger, allocation, balance, treshold).move(shardRouting, node);
    }


    /**
     *  It is based on three
     * values:
     * <ul>
     * <li><code>index balance</code></li>
     * <li><code>replica balance</code></li>
     * <li><code>primary balance</code></li>
     * </ul>
     * <p>
     * The <code>index balance</code> defines the factor of the distribution of
     * shards per index on nodes is weighted. The factor
     * <code>replica balance</code> defines the weight of the current number of
     * replicas allocated on a node compared to the average number of replicas
     * per node. Analogically the <code>primary balance</code> factor defines
     * the number of allocated primaries per node according to the average
     * number of primaries per node.
     * </p>
     * <ul>
     * <li>
     * <code>weight<sub>index</sub>(node, index) = indexBalance * (node.numReplicas(index) - avgReplicasPerNode(index))</code>
     * </li>
     * <li>
     * <code>weight<sub>node</sub>(node, index) = replicaBalance * (node.numReplicas() - avgReplicasPerNode)</code>
     * </li>
     * <li>
     * <code>weight<sub>primary</sub>(node, index) = primaryBalance * (node.numPrimaries() - avgPrimariesPerNode)</code>
     * </li>
     * </ul>
     * <code>weight(node, index) = weight<sub>index</sub>(node, index) + weight<sub>node</sub>(node, index) + weight<sub>primary</sub>(node, index)</code>
     * 
     * @author schilling
     */
    public static class WeightFunction  {

        private final float indexBalance;
        private final float replicaBalance;
        private final float primaryBalance;

        protected WeightFunction(float indexBalance, float replicaBalance, float primaryBalance) {
            final float sum = indexBalance + replicaBalance + primaryBalance;
            if (sum <= 0.0f) {
                throw new ElasticSearchIllegalArgumentException("Balance factors must sum to a value > 0 but was: " + sum);
            }
            this.indexBalance = indexBalance / sum;
            this.replicaBalance = replicaBalance / sum;
            this.primaryBalance = primaryBalance / sum;
        }

        public float weight(Balancer balancer, ModelNode node, String index) {
            final float weightReplica = replicaBalance * (node.numReplicas() - balancer.avgReplicasPerNode());
            final float weightIndex = indexBalance * (node.numReplicas(index) - balancer.avgReplicasOfIndexPerNode(index));
            final float weightPrimary = primaryBalance * (node.numPrimaries() - balancer.avgPrimariesPerNode());
            return weightReplica + weightIndex + weightPrimary;
        }

    }

    /**
     * A {@link Balancer}
     */
    static class Balancer {

        private final ESLogger logger;

        private final Map<String, ModelNode> nodes = new HashMap<String, ModelNode>();
        private final HashSet<String> indices = new HashSet<String>();
        private final RoutingAllocation allocation;
        private final WeightFunction weight;

        private final float treshold;
        private ModelNode[] nodesArray;
        private final MetaData metaData;
        
        private final Predicate<MutableShardRouting> assignedFilter = new Predicate<MutableShardRouting>() {
            @Override
            public boolean apply(MutableShardRouting input) {
                return input.assignedToNode();
            }
        };

        public Balancer(ESLogger logger, RoutingAllocation allocation, WeightFunction weight, float treshold) {
            this.logger = logger;
            this.allocation = allocation;
            this.weight = weight;
            this.treshold = treshold;
            for (RoutingNode node : allocation.routingNodes()) {
                nodes.put(node.nodeId(), new ModelNode(node.nodeId()));
            }
            metaData = allocation.routingNodes().metaData();
        }

        private ModelNode[] nodesArray() {
            if (nodesArray == null) {
                return nodesArray = nodes.values().toArray(new ModelNode[nodes.size()]);
            }
            return nodesArray;
        }

        public float avgReplicasOfIndexPerNode(String index) {
            return ((float) metaData.index(index).totalNumberOfShards()) / nodes.size();
        }

        public float avgReplicasPerNode() {
            return ((float) metaData.totalNumberOfShards()) / nodes.size();
        }

        public float avgPrimariesPerNode() {
            return ((float) metaData.numberOfShards()) / nodes.size();
        }

        public float avgPrimariesOfIndexPerNode(String index) {
            return ((float) metaData.index(index).numberOfShards()) / nodes.size();
        }

        private NodeSorter newNodeSorter(String index) {
            final NodeSorter sorter = new NodeSorter(nodesArray(), weight, index, this);
            sorter.quickSort(0, sorter.modelNodes.length - 1);
            return sorter;
        }

        public boolean move(MutableShardRouting replica, RoutingNode node) {
            if (!replica.started()) {
                return false;
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Try moving shard [{}] from [{}]", replica, node);
            }
            boolean changed = initialize(allocation.routingNodes());

            final ModelNode sourceNode = nodes.get(node.nodeId());
            assert sourceNode != null;
            final NodeSorter sorter = newNodeSorter(replica.getIndex());
            final ModelNode[] nodes = sorter.modelNodes;
            assert sourceNode.containsReplica(replica);

            for (ModelNode currentNode : nodes) {
                if (currentNode.getNodeId().equals(node.nodeId())) {
                    continue;
                }
                RoutingNode target = allocation.routingNodes().node(currentNode.getNodeId());
                Decision decision = allocation.deciders().canAllocate(replica, target, allocation);
                if (decision.type() == Type.YES) {
                    sourceNode.removeReplica(replica);
                    MutableShardRouting initializingShard = new MutableShardRouting(replica.index(), replica.id(), currentNode.getNodeId(),
                            replica.currentNodeId(), replica.primary(), ShardRoutingState.INITIALIZING, replica.version() + 1);
                    currentNode.addReplica(initializingShard, decision);
                    target.add(initializingShard);
                    replica.relocate(target.nodeId());
                    if (logger.isDebugEnabled()) {
                        logger.debug("Moved Replica [{}] to node [{}]", replica, currentNode.getNodeId());
                    }
                    return true;
                }
            }

            return changed;
        }

        private void buildModelFromAssigned(Iterable<MutableShardRouting> replicas) {
            for (MutableShardRouting replica : replicas) {
                assert replica.assignedToNode();
                /* we skip relocating shards here since we expect an initializing shard with the same id coming in */
                if (replica.state() == ShardRoutingState.RELOCATING) {
                    continue; 
                }
                ModelNode node = nodes.get(replica.currentNodeId());
                assert node != null;
                if (node.addReplica(replica, Decision.single(Type.YES, "Already allocated on node", node.getNodeId()))) {
                    if (logger.isDebugEnabled()) {
                        logger.info("Assigned Replica [{}] to node [{}]", replica, node.getNodeId());
                    }
                }
            }
        }

        private boolean allocateUnassigned(Iterable<MutableShardRouting> replicas) {
            boolean changed = false;
            if (logger.isDebugEnabled()) {
                logger.debug("Start allocating unassigned replicas");
            }
            final RoutingNodes routingNodes = allocation.routingNodes();
            final AllocationDeciders deciders = allocation.deciders();
            for (MutableShardRouting replica : replicas) {
                assert !replica.assignedToNode();
                assert !nodes.isEmpty();
                /* find an allocatable node with minimal weight */
                float minWeight = Float.POSITIVE_INFINITY;
                ModelNode minNode = null;
                Decision decision = null;
                for (ModelNode node : nodes.values()) {
                    /*
                     * The replica we add is removed below to simulate the
                     * addition for weight calculation we use Decision.ALWAYS to
                     * not violate the not null condition.
                     */
                    if (node.addReplica(replica, Decision.ALWAYS)) {
                        float currentWeight = weight.weight(this, node, replica.index());
                        /* Unless the operation is not providing any gains we don't check deciders */
                        if (currentWeight < minWeight) {
                            Decision currentDecision = deciders.canAllocate(replica, routingNodes.node(node.getNodeId()), allocation);
                            if (currentDecision.type() == Type.YES) {
                                minNode = node;
                                minWeight = currentWeight;
                                decision = currentDecision;
                            }
                        }
                        /* Remove the replica from the node again this is only a simulation */
                        Decision removed = node.removeReplica(replica);
                        assert removed != null;
                    }
                }
                assert decision != null && minNode != null || decision == null && minNode == null;
                if (minNode != null) {
                    minNode.addReplica(replica, decision);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Assigned Replica [{}] to [{}]", replica, minNode.getNodeId());
                    }
                    routingNodes.node(minNode.getNodeId()).add(replica);
                    changed |= true;
                } else if (logger.isDebugEnabled()) {
                    logger.debug("No Node found to assign replica [{}]", replica);
                }
            }
            return changed;
        }

        protected MutableShardRouting simulateRelocation(ModelNode srcNode, ModelNode dstNode, String idx) {
            final ModelIndex index = srcNode.getIndexInfo(idx);
            if (index != null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Try relocating replica for index index [{}] from node [{}] to node [{}]", idx, srcNode.getNodeId(),
                            dstNode.getNodeId());
                }
                final RoutingNode node = allocation.routingNodes().node(dstNode.getNodeId());
                float minCost = Float.POSITIVE_INFINITY;
                MutableShardRouting candidate = null;
                Decision decision = null;
                final AllocationDeciders deciders = allocation.deciders();
                /* make a copy since we modify this list in the loop */
                final ArrayList<MutableShardRouting> replicas = new ArrayList<MutableShardRouting>(index.getAllReplicas());
                for (MutableShardRouting replica : replicas) {

                    Decision allocationDecision = deciders.canAllocate(replica, node, allocation);
                    Decision rebalanceDecission = deciders.canRebalance(replica, allocation);

                    if (((allocationDecision.type() == Type.YES) || (allocationDecision.type() == Type.THROTTLE))
                            && ((rebalanceDecission.type() == Type.YES) || (rebalanceDecission.type() == Type.THROTTLE))) {
                        Decision srcDecision;
                        if ((srcDecision = srcNode.removeReplica(replica)) != null) {
                            if (dstNode.addReplica(replica, srcDecision)) {
                                final float delta = weight.weight(this, dstNode, idx) - weight.weight(this, srcNode, idx);
                                if (delta < minCost) {
                                    minCost = delta;
                                    candidate = replica;
                                    decision = new Decision.Multi().add(allocationDecision).add(rebalanceDecission);
                                }
                                dstNode.removeReplica(replica);
                            }
                            srcNode.addReplica(replica, srcDecision);
                        }
                    }
                }

                if (candidate != null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Relocate replica [{}] from node [{}] to node [{}]", candidate, srcNode.getNodeId(),
                                dstNode.getNodeId());
                    }
                    srcNode.removeReplica(candidate);
                    dstNode.addReplica(candidate, decision);
                    return candidate;
                }
            }
            return null;
        }

        private boolean initialize(RoutingNodes routing) {
            Collection<MutableShardRouting> replicas = new ArrayList<MutableShardRouting>();
            if (logger.isDebugEnabled()) {
                logger.debug("Start distributing Replicas");
            }

            for (IndexRoutingTable index : allocation.routingTable().indicesRouting().values()) {
                indices.add(index.index());
                for (IndexShardRoutingTable shard : index.getShards().values()) {
                    replicas.addAll(routing.shardsRoutingFor(index.index(), shard.shardId().id()));
                }
            }
            buildModelFromAssigned(Iterables.filter(replicas, assignedFilter));
            final boolean changed = allocateUnassigned(Iterables.filter(replicas, Predicates.not(assignedFilter)));
            return changed;
        }

        /**
         * balance the shards allocated on the nodes according to a given
         * <code>treshold</code>. Operations below this value will not be
         * handled.
         * 
         * @param treshold
         *            operations treshold
         * 
         * @return <code>true</code> if the current configuration has been
         *         changed
         */
        public boolean balance() {
            if (logger.isDebugEnabled()) {
                logger.debug("Start balancing cluster");
            }
            boolean changed = initialize(allocation.routingNodes());
            if (nodes.size() > 1) {
                for (String index : indices) {
                    final NodeSorter sorter = newNodeSorter(index); // already sorted
                    final float[] weights = sorter.weights;
                    final ModelNode[] modelNodes = sorter.modelNodes;
                    int lowIdx = 0;
                    int highIdx = weights.length - 1;
                    while (true) {

                        final ModelNode minNode = modelNodes[lowIdx];
                        final ModelNode maxNode = modelNodes[highIdx];
                        boolean relocated = false;
                        if (maxNode.numReplicas(index) > 0) {
                            if ((weights[highIdx] - weights[lowIdx]) <= treshold) {
                                break;
                            }
                            if (logger.isDebugEnabled()) {
                                logger.debug("Balancing from node [{}] weight: [{}] to node [{}] weight: [{}]  delta: [{}]",
                                        maxNode.getNodeId(), weights[highIdx], minNode.getNodeId(), weights[lowIdx], (weights[highIdx] - weights[lowIdx]));
                            }
                            relocated = tryRelocateReplica(index, minNode, maxNode);
                        }

                        if (relocated) {
                            weights[lowIdx] = sorter.weight(modelNodes[lowIdx]);
                            weights[highIdx] = sorter.weight(modelNodes[highIdx]);
                            sorter.quickSort(0, weights.length - 1);
                            lowIdx = 0;
                            highIdx = weights.length - 1;
                            changed = true;
                        } else if (lowIdx < highIdx - 1) {
                            lowIdx++;
                        } else if (lowIdx > 0) {
                            lowIdx = 0;
                            highIdx--;
                        } else {
                            break;
                        }
                    }
                }
            }
            return changed;
        }

        private boolean tryRelocateReplica(String index, ModelNode minNode, ModelNode maxNode) {
            boolean relocated = false;
            final MutableShardRouting replica = simulateRelocation(maxNode, minNode, index);
            if (replica != null) {
                // Move on Cluster
                if (replica.started()) {
                    String rId = replica.relocating() ? replica.relocatingNodeId() : replica.currentNodeId();

                    if (!minNode.getNodeId().equals(rId)) {
                        RoutingNode lowRoutingNode = allocation.routingNodes().node(minNode.getNodeId());

                        lowRoutingNode.add(new MutableShardRouting(replica.index(), replica.id(), lowRoutingNode.nodeId(), replica
                                .currentNodeId(), replica.primary(), INITIALIZING, replica.version() + 1));

                        replica.relocate(lowRoutingNode.nodeId());
                        relocated = true;
                    }
                } else if (replica.unassigned()) {
                    allocation.routingNodes().node(minNode.getNodeId()).add(replica);
                    relocated = true;
                }
            }
            return relocated;
        }

    }

    static class ModelNode implements Iterable<ModelIndex> {
        private final String id;
        private final Map<String, ModelIndex> indices = new HashMap<String, ModelIndex>();
        /* cached stats - invalidated on add/remove and lazily calcualated */
        private int numReplicas = -1;
        private int numPrimaries = -1;

        public ModelNode(String id) {
            this.id = id;
        }

        public ModelIndex getIndexInfo(String indexId) {
            return indices.get(indexId);
        }

        public String getNodeId() {
            return id;
        }

        public int numReplicas() {
            if (numReplicas == -1) {
                int sum = 0;
                for (ModelIndex index : indices.values()) {
                    sum += index.numReplicas();
                }
                numReplicas = sum;
            }
            return numReplicas;
        }

        public int numReplicas(String idx) {
            ModelIndex index = indices.get(idx);
            return index == null ? 0 : index.numReplicas();
        }

        public int numPrimaries(String idx) {
            ModelIndex index = indices.get(idx);
            return index == null ? 0 : index.numPrimaries();
        }

        public int numPrimaries() {
            if (numPrimaries == -1) {
                int sum = 0;
                for (ModelIndex index : indices.values()) {
                    sum += index.numPrimaries();
                }
                numPrimaries = sum;
            }
            return numPrimaries;
        }

        public Collection<MutableShardRouting> replicas() {
            Collection<MutableShardRouting> result = new ArrayList<MutableShardRouting>();
            for (ModelIndex index : indices.values()) {
                result.addAll(index.getAllReplicas());
            }
            return result;
        }

        public boolean addReplica(MutableShardRouting info, Decision decision) {
            numPrimaries = numReplicas = -1;
            ModelIndex index = indices.get(info.index());
            if (index == null) {
                index = new ModelIndex(info.index());
                indices.put(index.getIndexId(), index);
            }
            return index.addReplica(info, decision);
        }

        public Decision removeReplica(MutableShardRouting info) {
            numPrimaries = numReplicas = -1;
            ModelIndex index = indices.get(info.index());
            Decision removed = null;
            if (index != null) {
                removed = index.removeReplica(info);
                if (removed != null && index.numReplicas() == 0) {
                    indices.remove(info.index());
                }
            }
            return removed;
        }
        
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Node(").append(id).append(")");
            return sb.toString();
        }

        @Override
        public Iterator<ModelIndex> iterator() {
            return indices.values().iterator();
        }

        public boolean containsReplica(MutableShardRouting replica) {
            ModelIndex info = getIndexInfo(replica.getIndex());
            return info == null ? false : info.containsReplica(replica);
        }

    }

    static class ModelIndex {
        private final String id;
        private final Map<MutableShardRouting, Decision> replicas = new HashMap<MutableShardRouting, Decision>();
        private int numPrimaries = -1;

        public ModelIndex(String id) {
            this.id = id;
        }

        public String getIndexId() {
            return id;
        }

        public Decision getDecicion(MutableShardRouting info) {
            return replicas.get(info);
        }

        public int numReplicas() {
            return replicas.size();
        }

        public Collection<MutableShardRouting> getAllReplicas() {
            return replicas.keySet();
        }

        public int numPrimaries() {
            if (numPrimaries == -1) {
                int num = 0;
                for (MutableShardRouting info : replicas.keySet()) {
                    if (info.primary()) {
                        num++;
                    }
                }
                return numPrimaries = num;
            }
            return numPrimaries;
        }

        public Decision removeReplica(MutableShardRouting replica) {
            numPrimaries = -1;
            return replicas.remove(replica);
        }

        public boolean addReplica(MutableShardRouting replica, Decision decision) {
            numPrimaries = -1;
            assert decision != null;
            /* Return true if the replica is not in the map */
            return replicas.put(replica, decision) == null;
        }

        public boolean containsReplica(MutableShardRouting replica) {
            return replicas.containsKey(replica);
        }
    }

    static class NodeSorter extends SorterTemplate {

        final ModelNode[] modelNodes;
        final float[] weights;
        private WeightFunction function;
        private String index;
        private Balancer balancer;
        private float pivotWeight;

        public NodeSorter(ModelNode[] modelNodes, WeightFunction function, String index, Balancer balancer) {
            this.function = function;
            this.index = index;
            this.balancer = balancer;
            this.modelNodes = modelNodes;
            weights = new float[modelNodes.length];
            for (int i = 0; i < weights.length; i++) {
                weights[i] = weight(modelNodes[i]);
            }

        }

        public float weight(ModelNode node) {
            return function.weight(balancer, node, index);
        }

        @Override
        protected void swap(int i, int j) {
            ModelNode tmp = modelNodes[i];
            modelNodes[i] = modelNodes[j];
            modelNodes[j] = tmp;
            float tmpW = weights[i];
            weights[i] = weights[j];
            weights[j] = tmpW;
        }

        @Override
        protected int compare(int i, int j) {
            return Float.compare(weights[i], weights[j]);
        }

        @Override
        protected void setPivot(int i) {
            pivotWeight = weights[i];
        }

        @Override
        protected int comparePivot(int j) {
            return Float.compare(pivotWeight, weights[j]);
        }
    }
}
