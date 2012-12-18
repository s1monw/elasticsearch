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

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;;



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
            BalancedShardsAllocator.this.weightFunction = new WeightFunction(indexBalance, replicaBalance, primaryBalance);
        }
    }
    
    private volatile WeightFunction weightFunction;
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
    public void applyStartedShards(StartedRerouteAllocation allocation) { /* ONLY FOR GATEWAYS */ }

    @Override
    public void applyFailedShards(FailedRerouteAllocation allocation) { /* ONLY FOR GATEWAYS */ }

    @Override
    public boolean allocateUnassigned(RoutingAllocation allocation) {
        return rebalance(allocation);
    }

    @Override
    public boolean rebalance(RoutingAllocation allocation) {
        final Balancer balancer = new Balancer(logger, allocation, weightFunction, treshold);
        return balancer.balance();
    }

    @Override
    public boolean move(MutableShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        final Balancer balancer = new Balancer(logger, allocation, weightFunction, treshold);
        return balancer.move(shardRouting, node);
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
     */
    public static class WeightFunction  {

        private final float indexBalance;
        private final float replicaBalance;
        private final float primaryBalance;

        public WeightFunction(float indexBalance, float replicaBalance, float primaryBalance) {
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
            final float weightIndex = indexBalance * (node.numReplicas(index) - balancer.avgReplicasPerNode(index));
            final float weightPrimary = primaryBalance * (node.numPrimaries() - balancer.avgPrimariesPerNode());
            return weightReplica + weightIndex + weightPrimary;
        }

    }

    /**
     * A {@link Balancer}
     */
    public static class Balancer {

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

        /**
         * Returns an array view on the nodes in the balancer. Nodes should not be removed from this list.
         */
        private ModelNode[] nodesArray() {
            if (nodesArray == null) {
                return nodesArray = nodes.values().toArray(new ModelNode[nodes.size()]);
            }
            return nodesArray;
        }

        /**
         * Returns the average of replicas per node for the given index
         */
        public float avgReplicasPerNode(String index) {
            return ((float) metaData.index(index).totalNumberOfShards()) / nodes.size();
        }

        /**
         * Returns the global average of replicas per node
         */
        public float avgReplicasPerNode() {
            return ((float) metaData.totalNumberOfShards()) / nodes.size();
        }
        
        /**
         * Returns the global average of primaries per node
         */
        public float avgPrimariesPerNode() {
            return ((float) metaData.numberOfShards()) / nodes.size();
        }
        
        /**
         * Returns the average of primaries per node for the given index
         */
        public float avgPrimariesPerNode(String index) {
            return ((float) metaData.index(index).numberOfShards()) / nodes.size();
        }

        /**
         * Returns a new {@link NodeSorter} that sorts the nodes based on their
         * current weight with respect to the index passed to the sorter. The
         * returned sorter is already sorted with min weight first.
         */
        private NodeSorter newNodeSorter(String index) {
            final NodeSorter sorter = new NodeSorter(nodesArray(), weight, index, this);
            sorter.quickSort(0, sorter.modelNodes.length - 1);
            return sorter;
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
            if (this.nodes.isEmpty()) {
                return false;
            }
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
                            relocated = tryRelocateReplica(minNode, maxNode, index);
                        }
                        if (relocated) {
                            /*
                             * TODO we could be a bit smarter here, we don't need to fully sort necessarily
                             * we could just find the place to insert linearly but the win might be minor
                             * compared to the added complexity
                             */
                            weights[lowIdx] = sorter.weight(modelNodes[lowIdx]);
                            weights[highIdx] = sorter.weight(modelNodes[highIdx]);
                            sorter.quickSort(0, weights.length - 1);
                            lowIdx = 0;
                            highIdx = weights.length - 1;
                            changed = true;
                        } else if (lowIdx < highIdx - 1) {
                            /* we can't move from any shard from the min node lets move on to the next node
                             * and see if the threshold still holds. We either don't have any shard of this
                             * index on this node of allocation deciders prevent any relocation.*/
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

        /**
         * This function executes a move operation moving the given replica from
         * the given node to the minimal eligable node with respect to the
         * weight function. Iff the replica is moved the replica will be set to
         * {@link ShardRoutingState#RELOCATING} and a shadow instance of this
         * replica is created with an incremented version in the state
         * {@link ShardRoutingState#INITIALIZING}.
         * 
         * @return <code>true</code> iff the shard has successfully been moved.
         */
        public boolean move(MutableShardRouting replica, RoutingNode node) {
            if (nodes.isEmpty() || !replica.started()) {
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
            /*
             * the sorter holds the minimum weight node first for the shards index.
             * We now walk through the nodes until we find a node to allocate the shard.
             * This is not guaranteed to be balanced after this operation we still try best effort to 
             * allocate on the minimal eligable node.
             */
            for (ModelNode currentNode : nodes) {
                if (currentNode.getNodeId().equals(node.nodeId())) {
                    continue;
                }
                RoutingNode target = allocation.routingNodes().node(currentNode.getNodeId());
                Decision decision = allocation.deciders().canAllocate(replica, target, allocation);
                if (decision.type() == Type.YES) { // TODO maybe we can respect throtteling here too?
                    sourceNode.removeReplica(replica);
                    final MutableShardRouting initializingShard = new MutableShardRouting(replica.index(), replica.id(), currentNode.getNodeId(),
                            replica.currentNodeId(), replica.primary(), INITIALIZING, replica.version() + 1);
                    currentNode.addReplica(initializingShard, decision);
                    target.add(initializingShard);
                    replica.relocate(target.nodeId()); // set the node to relocate after we added the initializing shard
                    if (logger.isDebugEnabled()) {
                        logger.debug("Moved Replica [{}] to node [{}]", replica, currentNode.getNodeId());
                    }
                    return true;
                }
            }

            return changed;
        }

        /**
         * Builds the internal model from all shards in the given
         * {@link Iterable}. All shards in the {@link Iterable} must be assigned
         * to a node. This method will skip replicas in the state
         * {@link ShardRoutingState#RELOCATING} since each relocating shard has
         * a shadow replica in the state {@link ShardRoutingState#INITIALIZING}
         * on the target node which we respect during the allocation / balancing
         * process. In short, this method recreates the status-quo in the cluster.
         */
        private void buildModelFromAssigned(Iterable<MutableShardRouting> replicas) {
            for (MutableShardRouting replica : replicas) {
                assert replica.assignedToNode();
                /* we skip relocating shards here since we expect an initializing shard with the same id coming in */
                if (replica.state() == RELOCATING) {
                    continue; 
                }
                ModelNode node = nodes.get(replica.currentNodeId());
                assert node != null;
                if (node.addReplica(replica, Decision.single(Type.YES, "Already allocated on node", node.getNodeId()))) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Assigned Replica [{}] to node [{}]", replica, node.getNodeId());
                    }
                }
            }
        }

        /**
         *  Allocates all given replicas on the minimal eligable node for the replicas index
         *  with respect to the weight function. Allo given replicas must be unassigned.
         */
        private boolean allocateUnassigned(Iterable<MutableShardRouting> replicas) {
            assert !nodes.isEmpty();
            boolean changed = false;
            if (logger.isDebugEnabled()) {
                logger.debug("Start allocating unassigned replicas");
            }

            /*
             * TODO: We could be smarter here and group the replicas by index and then
             * use the sorter to save some iterations. 
             */
            final RoutingNodes routingNodes = allocation.routingNodes();
            final AllocationDeciders deciders = allocation.deciders();
            for (MutableShardRouting replica : replicas) {
                assert !replica.assignedToNode();
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

        /**
         *  Tries to find a relocation from the max node to the min node for an arbitrary shard of the given index on the
         *  balance model. Iff this method returns a <code>true</code> the relocation has already been executed on the
         *  simulation model as well as on the cluster.
         */
        private boolean tryRelocateReplica(ModelNode minNode, ModelNode maxNode, String idx) {
            final ModelIndex index = maxNode.getIndex(idx);
            if (index != null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Try relocating replica for index index [{}] from node [{}] to node [{}]", idx, maxNode.getNodeId(),
                            minNode.getNodeId());
                }
                final RoutingNode node = allocation.routingNodes().node(minNode.getNodeId());
                float minCost = Float.POSITIVE_INFINITY;
                MutableShardRouting candidate = null;
                Decision decision = null;
                final AllocationDeciders deciders = allocation.deciders();
                /* make a copy since we modify this list in the loop */
                final ArrayList<MutableShardRouting> replicas = new ArrayList<MutableShardRouting>(index.getAllReplicas());
                for (MutableShardRouting replica : replicas) {
                    if (replica.initializing() || replica.relocating()) {
                        // skip initializing and relocating shards we can't relocate them anyway
                        continue;
                    }
                    Decision allocationDecision = deciders.canAllocate(replica, node, allocation);
                    Decision rebalanceDecission = deciders.canRebalance(replica, allocation);

                    if (((allocationDecision.type() == Type.YES) || (allocationDecision.type() == Type.THROTTLE))
                            && ((rebalanceDecission.type() == Type.YES) || (rebalanceDecission.type() == Type.THROTTLE))) {
                        Decision srcDecision;
                        if ((srcDecision = maxNode.removeReplica(replica)) != null) {
                            if (minNode.addReplica(replica, srcDecision)) {
                                final float delta = weight.weight(this, minNode, idx) - weight.weight(this, maxNode, idx);
                                if (delta < minCost) {
                                    minCost = delta;
                                    candidate = replica;
                                    decision = new Decision.Multi().add(allocationDecision).add(rebalanceDecission);
                                }
                                minNode.removeReplica(replica);
                            }
                            maxNode.addReplica(replica, srcDecision);
                        }
                    }
                }

                if (candidate != null) {
                  
                    /* allocate on the model even if not throttled */
                    maxNode.removeReplica(candidate);
                    minNode.addReplica(candidate, decision);
                    if (decision.type() == Type.YES) { /* only allocate on the cluster if we are not throttled */
                        if (logger.isDebugEnabled()) {
                            logger.debug("Relocate replica [{}] from node [{}] to node [{}]", candidate, maxNode.getNodeId(),
                                    minNode.getNodeId());
                        }
                        /* now allocate on the cluster - if we are started we need to relocate the shard */
                        if (candidate.started()) {
                            RoutingNode lowRoutingNode = allocation.routingNodes().node(minNode.getNodeId());
                            lowRoutingNode.add(new MutableShardRouting(candidate.index(), candidate.id(), lowRoutingNode.nodeId(), candidate
                                    .currentNodeId(), candidate.primary(), INITIALIZING, candidate.version() + 1));
                            candidate.relocate(lowRoutingNode.nodeId());
                            
                        } else {
                            assert candidate.unassigned();
                            allocation.routingNodes().node(minNode.getNodeId()).add(candidate);
                        }
                    }
                    return true;
                }
            }
            return false;
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

        public ModelIndex getIndex(String indexId) {
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

        public boolean addReplica(MutableShardRouting replica, Decision decision) {
            numPrimaries = numReplicas = -1;
            ModelIndex index = indices.get(replica.index());
            if (index == null) {
                index = new ModelIndex(replica.index());
                indices.put(index.getIndexId(), index);
            }
            return index.addReplica(replica, decision);
        }

        public Decision removeReplica(MutableShardRouting replica) {
            numPrimaries = numReplicas = -1;
            ModelIndex index = indices.get(replica.index());
            Decision removed = null;
            if (index != null) {
                removed = index.removeReplica(replica);
                if (removed != null && index.numReplicas() == 0) {
                    indices.remove(replica.index());
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
            ModelIndex index = getIndex(replica.getIndex());
            return index == null ? false : index.containsReplica(replica);
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

        public Decision getDecicion(MutableShardRouting replica) {
            return replicas.get(replica);
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
                for (MutableShardRouting replica : replicas.keySet()) {
                    if (replica.primary()) {
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
            final ModelNode tmpNode = modelNodes[i];
            modelNodes[i] = modelNodes[j];
            modelNodes[j] = tmpNode;
            final float tmpWeight = weights[i];
            weights[i] = weights[j];
            weights[j] = tmpWeight;
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
