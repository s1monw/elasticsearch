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
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

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

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

/**
 * 1) Maximize the number of nodes that keep a primary
 * 2) Minimize the number of primaries per node
 * 3) Minimize the distance of the maximal and the minimal
 *    number of shards per node
 * 4) Maximize the Number of Indices per Node
 * 5) Keep care of replicas of the same shard on the same Node
 * 6) Minimize the Number of Move-Operations
 * 
 * 
 * @author schilling
 * 
 */
public class BalancedShardsAllocator extends AbstractComponent implements ShardsAllocator {
/*
 * if (logger.isTraceEnabled()) {
            logger.trace("merge [{}] starting..., merging [{}] segments, [{}] docs, [{}] size, into [{}] estimated_size", merge.info == null ? "_na_" : merge.info.info.name, merge.segments.size(), totalNumDocs, new ByteSizeValue(totalSizeInBytes), new ByteSizeValue(merge.estimatedMergeBytes));
        }
 */
    WeightFunction balance;
    float treshold;
    private final ESLogger logger = Loggers.getLogger(getClass());

    @Inject
    public BalancedShardsAllocator(Settings settings) {
        super(settings);
        float indexBalance = settings.getAsFloat("cluster.routing.allocation.balance.index", 0.0f);
        float replicaBalance  = settings.getAsFloat("cluster.routing.allocation.balance.replica", 1.0f);
        float primaryBalance = settings.getAsFloat("cluster.routing.allocation.balance.primary", 0.0f);

        this.treshold = settings.getAsFloat("cluster.routing.allocation.balance.treshold", 1.1f);
        this.balance = new BasicBalance(indexBalance, replicaBalance, primaryBalance);
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
        if (!shardRouting.started()) {
            return false;
        }
        return new Balancer(logger, allocation, balance, treshold).move(shardRouting, node);
    }

}

/**
 * Definition of a weight function for {@link NodeInfo} according to
 * a given <code>index</code>. 
 */
interface WeightFunction {
    public float weight(Balancer allocation, NodeInfo node, String index);
}

/**
 * Simple implementation of a {@link WeightFunction}. It is based on three
 * values:
 * <ul><li><code>index balance</code></li>
 *     <li><code>replica balance</code></li>
 *     <li><code>primary balance</code></li></ul>
 * <p>The <code>index balance</code> defines the factor of the distribution of 
 * shards per index on nodes is weighted. The factor <code>replica balance</code>
 * defines the weight of the current number of replicas allocated on a node
 * compared to the average number of replicas per node. Analogically the
 * <code>primary balance</code> factor defines the number of allocated primaries
 * per node according to the average number of primaries per node.</p>
 * <ul>
 * <li><code>weight<sub>index</sub>(node, index) = indexBalance * (node.numReplicas(index) - avgReplicasPerNode(index))</code></li>    
 * <li><code>weight<sub>node</sub>(node, index) = replicaBalance * (node.numReplicas() - avgReplicasPerNode)</code></li>    
 * <li><code>weight<sub>primary</sub>(node, index) = primaryBalance * (node.numPrimaries() - avgPrimariesPerNode)</code></li>
 * </ul>
 * <code>weight(node, index) = weight<sub>index</sub>(node, index) + weight<sub>node</sub>(node, index) + weight<sub>primary</sub>(node, index)</code>    
 * @author schilling
 */
class BasicBalance implements WeightFunction {

    private final float indexBalance;
    private final float replicaBalance;
    private final float primaryBalance;

    protected BasicBalance(float indexBalance, float replicaBalance, float primaryBalance) {
        super();
        
        float commonDenominator = indexBalance + replicaBalance + primaryBalance;
        
        this.indexBalance = indexBalance/commonDenominator;
        this.replicaBalance = replicaBalance/commonDenominator;
        this.primaryBalance = primaryBalance/commonDenominator;
    }

    @Override
    public float weight(Balancer balancer, NodeInfo node, String index) {
        final float weightReplica = replicaBalance * (node.numReplicas() - balancer.stats.avgReplicasPerNode());
        final float weightIndex = indexBalance * (node.numReplicas(index) - balancer.stats.avgReplicasOfIndexPerNode(index));
        final float weightPrimary = primaryBalance * (node.numPrimaries() - balancer.stats.avgPrimariesPerNode());
        return weightReplica + weightIndex + weightPrimary;
    }


}

/**
 * A {@link Comparator} used to order nodes according to
 * a given {@link WeightFunction}
 */
class WeightOrder implements Comparator<NodeInfo> {
    final Balancer balancer;
    final WeightFunction function;
    final String index;
    
    public WeightOrder(Balancer balancer, WeightFunction function, String index) {
        super();
        this.balancer = balancer;
        this.function = function;
        this.index = index;
    }

    public float weight(NodeInfo node) {
        return function.weight(balancer, node, index);
    }
    
    @Override
    public int compare(NodeInfo left, NodeInfo right) {
        final int cmp;
        if((cmp = Float.compare(weight(left), weight(right))) != 0) {
            return cmp;
        } else {
            return left.id.compareTo(right.id);
        }
    }
}

/**
 * A {@link Balancer} 
 * 
 * 
 */
class Balancer {
    
    private final ESLogger logger;
    
    private final Map<String, NodeInfo> nodes;
    private final HashSet<String> indices;
    private final RoutingAllocation allocation;
    private final WeightFunction weight;
    
    private float treshold = 0;
    
    public final Stats stats = new Stats();
    
    public Balancer(ESLogger logger, RoutingAllocation allocation, WeightFunction weight, float treshold) {
        this.logger = logger;
        this.allocation = allocation;
        this.nodes = new HashMap<String, NodeInfo>();
        this.indices = new HashSet<String>();
        this.weight = weight;
        this.treshold = treshold;
        
        for(RoutingNode node : allocation.routingNodes()) {
            nodes.put(node.nodeId(), new NodeInfo(node.nodeId()));
        }

    }

    /**
     * Statistics of the balancer. This class defines some common functions
     * for the distributions used by the {@link Balancer}
     */
    class Stats {

        /**
         * @return Number of nodes defined by the Balancer
         */
        public int numNodes() {
            assert nodes.size() > 0;
            return nodes.size();
        }    

        /**
         * @return Number of Replicas handled by the {@link Balancer}
         */
        public int numReplicas() {
            int sum = 0;
            for(NodeInfo node : nodes.values()) {
                sum += node.numReplicas();
            }
            return sum;
        }
        
        /**
         * @return Number of primaries shards of all indices 
         */
        public int numPrimaries() {
            int sum = 0;
            for(NodeInfo node : nodes.values()) {
                sum += node.numPrimaries();
            }
            return sum;
        }
        
        /**
         * @param index Index to use
         * @return number of replicas for the given index 
         */
        public int numReplicas(String index) {
            int sum = 0;
            for(NodeInfo node : nodes.values()) {
                sum += node.numReplicas(index);
            }
            return sum;
        }
        
        /**
         * @param index Index to use
         * @return number of primary shards of the given index
         */
        public int numPrimaries(String index) {
            int sum = 0;
            for(NodeInfo node : nodes.values()) {
                sum += node.numPrimaries(index);
            }
            return sum;
        }
        
        public float avgReplicasOfIndexPerNode(String index) {
            return ((float)numReplicas(index)) / numNodes();
        }
        
        public float avgReplicasPerNode() {
            return ((float)numReplicas()) / numNodes();
        }
        
        public float avgPrimariesPerNode() {
            return ((float)numPrimaries()) / numNodes();
        }

        public float avgPrimariesOfIndexPerNode(String index) {
            return ((float)numPrimaries(index)) / numNodes();
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Stats:");
            sb.append(" numNodes=").append(numNodes());
            sb.append(" numReplicas=").append(numReplicas());
            sb.append(" numPrimaries=").append(numPrimaries());
            sb.append(" avgReplicas=").append(avgReplicasPerNode());
            sb.append(" avgPrimaries=").append(avgPrimariesPerNode());
            return sb.toString();
        }
    }
    
    protected static String toString(MutableShardRouting replica) {
        return replica.getIndex()+":"+replica.id()+(replica.primary()?"P":"R") + " ["+replica.state()+(replica.relocating()?(" "+replica.currentNodeId() + "->"+replica.relocatingNodeId()):"")+"]";
    }
    
    public boolean move(MutableShardRouting replica, RoutingNode node) {
        logger.info("-------- MOVING --------");
        logger.info("\t"+toString(replica) + " from " + node.nodeId());
        printState("\t");
        boolean moved = moveShard(replica, node);
        printState("\t", moved);
        return moved;
    }

    private boolean moveShard(MutableShardRouting replica, RoutingNode node) {
        boolean changed = initateReplicas(allocation.routingNodes());
        
        NodeInfo source = null;
        NodeInfo[] nodes = this.nodes.values().toArray(new NodeInfo[this.nodes.size()]);
        WeightOrder order = new WeightOrder(this, weight, replica.index());
        Arrays.sort(nodes, order);
        source = this.nodes.get(node.nodeId());
        if(source == null) {
            return false;
        }
        logger.info("\tNode found " + source.id);

        if(!source.containsReplica(replica)) {
            return false;
        }

        for (int i=0; i<nodes.length; i++) {
            if(nodes[i].id.equals(node.nodeId())) {
                continue;
            }
            RoutingNode target = allocation.routingNodes().node(nodes[i].id);
            Decision decision = allocation.deciders().canAllocate(replica, target, allocation);
            if(decision.type() == Type.YES) {
                source.removeReplica(replica);
                MutableShardRouting initializingShard = new MutableShardRouting(replica.index(), replica.id(),
                        nodes[i].id, replica.currentNodeId(),
                        replica.primary(), ShardRoutingState.INITIALIZING, replica.version() + 1);
                nodes[i].addReplica(initializingShard, decision);
                target.add(initializingShard);
                replica.relocate(target.nodeId());
                if (logger.isDebugEnabled()) {
                    logger.debug("Moved Replica [{}] to node [{}]", toString(replica), nodes[i].id);
                }
                return true;
            }
        }
        

        return changed;
    }
    
    
    private boolean distributeReplicas(Collection<MutableShardRouting> replicas) {
        ArrayList<MutableShardRouting> others = new ArrayList<MutableShardRouting>();
        boolean changed = false;
        List<MutableShardRouting> added = new ArrayList<MutableShardRouting>();
        Predicate<MutableShardRouting> assignedFilter = new Predicate<MutableShardRouting>() {
            @Override
            public boolean apply(MutableShardRouting input) {
                return input.assignedToNode();
            }
        }; 
        others.addAll(replicas);
        buildModelFromAssigned(Iterables.filter(replicas, assignedFilter), added);
        changed |= allocateUnassigned(Iterables.filter(replicas, Predicates.not(assignedFilter)), added);
        replicas.removeAll(added);
        
        return changed;
    }
    
    private void buildModelFromAssigned(Iterable<MutableShardRouting> replicas, List<MutableShardRouting> added) {
        for(MutableShardRouting replica : replicas) {
            assert replica.assignedToNode();
            if (replica.state() == ShardRoutingState.RELOCATING) {
                continue; // we skip relocating shards here since we expect an initializing shard with the same id coming in
            }
            NodeInfo node = nodes.get(replica.currentNodeId());
            assert node != null;
            if (node.addReplica(replica, Decision.single(Type.YES, "Already allocated on node", node.id))) {
                if (logger.isDebugEnabled()) {
                    logger.info("Assigned Replica [{}] to node [{}]", toString(replica), node.id);
                }
                added.add(replica);    
            }
        }
    }
    
    private boolean allocateUnassigned(Iterable<MutableShardRouting> replicas, List<MutableShardRouting> added) {
        boolean changed = false;
        if (logger.isDebugEnabled()) {
            logger.debug("Start allocating unassigned replicas");
        }
        for(MutableShardRouting replica : replicas) {
            assert !replica.assignedToNode();
            assert !nodes.isEmpty();
            // find an allocatable node with minimal weight
            float minWeight = Float.POSITIVE_INFINITY;
            NodeInfo minNode = null;
            Decision decision = null;
            final AllocationDeciders deciders = allocation.deciders();
            for (NodeInfo node : nodes.values()) {
                if(node.addReplica(replica, Decision.ALWAYS)) { // we delete this replica anyways just add Decision.ALWAYS
                    float currentWeight = weight.weight(this, node, replica.index());
                    if (currentWeight < minWeight) { // only check deciders if this node is eligable
                        RoutingNode routing = allocation.routingNodes().node(node.id);
                        Decision currentDecision = deciders.canAllocate(replica, routing, allocation);
                        if (currentDecision.type() == Type.YES) {
                            minNode = node;
                            minWeight = currentWeight;
                            decision = currentDecision;
                        }
                    }
                    Decision removed = node.removeReplica(replica); // remove the temp node 
                    assert removed != null;
                }
            }
            assert decision != null && minNode != null || decision == null && minNode == null;
            if (minNode != null) {
                minNode.addReplica(replica, decision);
                if (logger.isDebugEnabled()) {
                    logger.debug("Assigned Replica [{}] to [{}]",  toString(replica), minNode.id);
                }
                allocation.routingNodes().node(minNode.id).add(replica);
                changed |= true;
                added.add(replica);
            } else if (logger.isDebugEnabled()) {
                    logger.debug("No Node found to assign replica [{}]", toString(replica));
            }
        }
        return changed;
    }
    
    
    protected MutableShardRouting relocateSomeReplica(NodeInfo src, NodeInfo dst, String idx) {
        ModelIndex index = src.getIndexInfo(idx);
        if(index == null) {
            return null;
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Try relocating replica for index index [{}] from node [{}] to node [{}]", idx, src.id, dst.id);
            }
            RoutingNode node = allocation.routingNodes().node(dst.id);
            float minCost = Float.POSITIVE_INFINITY;
            MutableShardRouting candidate = null;
            Decision decision = null;
                        
            Collection<MutableShardRouting> allReplicas = new ArrayList<MutableShardRouting>(index.getAllReplicas());
            
            for(MutableShardRouting replica : allReplicas) {
                
                Decision allocationDecision = allocation.deciders().canAllocate(replica, node, allocation);
                Decision rebalanceDecission = allocation.deciders().canRebalance(replica, allocation);
                
                if((allocationDecision.type() == Type.YES) || (allocationDecision.type() == Type.THROTTLE)) {
                    if((rebalanceDecission.type() == Type.YES) || (rebalanceDecission.type() == Type.THROTTLE)) {
                        Decision srcDecision;
                        if((srcDecision = src.removeReplica(replica)) != null) {
                            if(dst.addReplica(replica, srcDecision)) {
                                final float weightDiff = weight.weight(this, dst, idx) - weight.weight(this, src, idx); 
                               
                                //logger.info("\t" + toString(info) + " canAllocate=" + allocationDecision.type() + " canRebalance="+rebalanceDecission.type()+" optimize="+weightDiff);
                                
                                if(weightDiff<minCost) {
                                    minCost = weightDiff;
                                    candidate = replica;
                                    decision = new Decision.Multi().add(allocationDecision).add(rebalanceDecission);
                                }
                                dst.removeReplica(replica);
                            }
                            src.addReplica(replica, srcDecision);
                        }
                    }
                }
            }
            
            if(candidate != null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Relocate replica [{}] from node [{}] to node [{}]", candidate, src.id, dst.id);
                }
                src.removeReplica(candidate);
                dst.addReplica(candidate, decision);
                return candidate;
            } else {
                return null;
            }
        }
    }
    
    private boolean initateReplicas(RoutingNodes routing) {
        Collection<MutableShardRouting> replicas = new ArrayList<MutableShardRouting>();
        if (logger.isDebugEnabled()) {
            logger.debug("Start distributing Replicas");
        }

        for(IndexRoutingTable index : allocation.routingTable().indicesRouting().values()) {
            indices.add(index.index());
            for(IndexShardRoutingTable shard : index.getShards().values()) {
                replicas.addAll(routing.shardsRoutingFor(index.index(), shard.shardId().id()));
            }
        }

        boolean changed = distributeReplicas(replicas);
        if (logger.isDebugEnabled()) {
            printState("\t", changed); // nocommit too verbose
        }
        return changed;
    }
    
    public boolean balance() {
        return balance(treshold);
    }
    
    /**
     * balance the shards allocated on the nodes according to
     * a given <code>treshold</code>. Operations below this
     * value will not be handled.  
     * 
     * @param treshold operations treshold
     * 
     * @return <code>true</code> if the current configuration
     *  has been changed
     */
    public boolean balance(float treshold) {
        if (logger.isDebugEnabled()) {
            logger.debug("Start balancing cluster");
        }
        boolean changed = initateReplicas(allocation.routingNodes());
        
        if (logger.isDebugEnabled()) {
            logger.debug("Balance State after initializing balancer: [{}] ", stats.toString());   
        }
        
        if(nodes.size()<=1) {
            return changed;
        } else {
            boolean rebalanced = false;
            for(String index : indices) {
                final WeightOrder order = new WeightOrder(this, weight, index);
                rebalanced |= balanceIndex(index, order);
            }
            if (logger.isDebugEnabled()) {
                printState(changed | rebalanced); // nocommit too verbose
            }
            return changed | rebalanced;
        }
    }
    
    private boolean balanceIndex(String index, WeightOrder order) {
        
        boolean changed = false;
        
        TreeSet<NodeInfo> nodes = new TreeSet<NodeInfo>(order);
        Collection<NodeInfo> ignoredMin = new ArrayList<NodeInfo>(nodes.size());
        Collection<NodeInfo> ignoredMax = new ArrayList<NodeInfo>(nodes.size());

        nodes.addAll(this.nodes.values());
        
        while(true) {

            NodeInfo minNode = nodes.pollFirst();
            NodeInfo maxNode = nodes.pollLast();
            boolean moved = false;
            if(maxNode.numReplicas(index) > 0){
                moved = balanceRelocation(index, minNode, maxNode, order);
            }
            
            if(moved) {
                
                nodes.add(minNode);
                nodes.add(maxNode);
                
                nodes.addAll(ignoredMax);
                nodes.addAll(ignoredMin);
                
                ignoredMin.clear();
                ignoredMax.clear();
                
                changed = true;
            } else if(nodes.size()>=1) {
                nodes.add(maxNode);
                ignoredMin.add(minNode);
            } else if(ignoredMin.size()>=1){
                nodes.add(minNode);
                nodes.addAll(ignoredMin);
                ignoredMin.clear();
                ignoredMax.add(maxNode);
            } else {
                break;
            }
        }
        
        return changed;
    }
    
    private boolean balanceRelocation(String index, NodeInfo minNode, NodeInfo maxNode, WeightOrder order) {
        final float minWeight = order.weight(minNode);
        final float maxWeight = order.weight(maxNode);
        final float diff = (maxWeight - minWeight);

        logger.debug("\tBalancing: " + "from " + maxNode.id +"("+maxWeight+")"+ " to " + minNode.id +"("+minWeight+")"+ " " + diff);

        boolean changed = false;
        
        if(diff>=treshold) {
            MutableShardRouting replica = relocateSomeReplica(maxNode, minNode, index);
            if(replica != null) {
                logger.debug("\t\tReplica: " + toString(replica));
                //Move in Simulation
                //Move on Cluster
                if(replica.started()) {
                    String rId = replica.relocating() ? replica.relocatingNodeId() : replica.currentNodeId();
                    
                    if(!minNode.id.equals(rId)) {
                        RoutingNode lowRoutingNode = allocation.routingNodes().node(minNode.id);
                        
                        lowRoutingNode.add(new MutableShardRouting(replica.index(), replica.id(),
                                lowRoutingNode.nodeId(), replica.currentNodeId(),
                                replica.primary(), INITIALIZING, replica.version() + 1));
                        
                        replica.relocate(lowRoutingNode.nodeId());
                        logger.info("\tMoved " + toString(replica) + " v: " + replica.version());
                        changed = true;
                    }
                } else if(replica.unassigned()) {
                    logger.info("\tAssigned " + toString(replica));
                    allocation.routingNodes().node(minNode.id).add(replica);
                    changed = true;
                }
            } else {
                logger.debug("\t\tNo replica found");
            }
        } else {
            logger.debug("\t\tIgnored: diff=" + diff + "<" + "treshold=" + treshold);
        }

        return changed;
    }

    
    private void printState(String prefix) {
        printState("\t", null);
    }
    
    private void printState(Boolean changed) {
        printState("", changed);
    }
    private void printState(String prefix, Boolean changed) {
        logger.info(prefix + "-------------- STATE"+(changed!=null?("(changed="+changed+")"):"")+" -----------");
        for(RoutingNode node : allocation.routingNodes()) {
    
            StringBuilder sb = new StringBuilder(prefix+"\tNode Configured("+node.shards().size()+"): " + node.nodeId()+":\t");
            for(MutableShardRouting shardRouting : allocation.routingNodes().node(node.nodeId()).shards()) {
                sb.append(" " + toString(shardRouting));
            }
            logger.info(sb.toString());
        }
    }
    

    
}

class NodeInfo implements Iterable<ModelIndex> {
    final String id;
    private final Map<String, ModelIndex> indices = new HashMap<String, ModelIndex>();
    
    public NodeInfo(String id) {
        this.id = id;
    }
    
    public ModelIndex getIndexInfo(String indexId) {
      return indices.get(indexId);
    }
    
    
    public int numReplicas() {
        int sum = 0;
        for(ModelIndex index : indices.values()) {
            sum += index.numReplicas();
        }
        return sum;
    }
    
    public int numReplicas(String idx) {
        ModelIndex index = indices.get(idx);
        if(index == null) {
            return 0;
        } else {
            return index.numReplicas();
        }
    }

    public int numPrimaries(String idx) {
        ModelIndex index = indices.get(idx);
        return index == null ? 0 : index.numPrimaries();
    }

    public int numPrimaries() {
        int sum = 0;
        for(ModelIndex index : indices.values()) {
            sum += index.numPrimaries();
        }
        return sum;
    }
    

    public Collection<MutableShardRouting> replicas() {
        Collection<MutableShardRouting> result = new ArrayList<MutableShardRouting>();
        for(ModelIndex index : indices.values()) {
            result.addAll(index.getAllReplicas());
        }
        return result;
    }
    
    public boolean addReplica(MutableShardRouting info, Decision decision) {
        ModelIndex index = indices.get(info.index());
        if(index == null) {
            index = new ModelIndex(info.index());
            indices.put(index.id, index);
        }
        return index.addReplica(info, decision);
    }
    
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Node("+id+"):\n");
        for(ModelIndex index : indices.values()) {
            sb.append('\t').append("index("+index.id+"):");
            for(MutableShardRouting shard : index.getAllReplicas()) {
                sb.append(' ').append(Balancer.toString(shard));
            }
            sb.append('\n');
        }
        return sb.toString();
    }
    
    public Decision removeReplica(MutableShardRouting info) {
        ModelIndex index = indices.get(info.index());
        if(index==null){
            return null;
        } else {
            Decision removed = index.removeReplica(info);
            if(removed != null && index.numReplicas() == 0) {
                indices.remove(info.index());
            }
            return removed;
        }
    }

    @Override
    public Iterator<ModelIndex> iterator() {
        return indices.values().iterator();
    }
    
    public boolean containsReplica(MutableShardRouting replica) {
        ModelIndex info = getIndexInfo(replica.getIndex());
        if(info == null) {
            System.err.println("\tNo such index");
            return false;
        } else {
            return info.containsReplica(replica);
        }
    }
    
}

class ModelIndex {
    protected final String id;
    private final Map<MutableShardRouting, Decision> replicas = new HashMap<MutableShardRouting, Decision>();
    
    public ModelIndex(String id) {
        super();
        this.id = id;
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
        int num = 0;
        for (MutableShardRouting info : replicas.keySet()) {
            if (info.primary()) {
                num++;
            }
        }
        return num;
    }

    public Decision removeReplica(MutableShardRouting replica) {
        return replicas.remove(replica);
    }
    
    public boolean addReplica(MutableShardRouting replica, Decision decision) {
        assert decision != null;
        boolean isIn = containsReplica(replica);
        replicas.put(replica, decision);
        return !isIn;
    }
    
    public boolean containsReplica(MutableShardRouting replica) {
        return replicas.containsKey(replica);
    }
    
}

//class ModelReplica {
//    final MutableShardRouting replica;
//    private final Decision decision;
//    private final int replicaId;
//    private final ShardId shardId;
//    
//    public ModelReplica(MutableShardRouting replica, Decision decision) {
//        super();
//        this.replica = replica;
//        this.decision = decision;
//        this.replicaId = replica.getId();
//        this.shardId = replica.shardId();
//    }
//
//    @Override
//    public int hashCode() {
//        final int prime = 31;
//        int result = 1;
//        result = prime * result + replicaId;
//        result = prime * result + ((shardId == null) ? 0 : shardId.hashCode());
//        return result;
//    }
//
//
//
//    @Override
//    public boolean equals(Object obj) {
//        if (this == obj)
//            return true;
//        if (obj == null)
//            return false;
//        if (getClass() != obj.getClass())
//            return false;
//        ModelReplica other = (ModelReplica) obj;
//        if (replicaId != other.replicaId)
//            return false;
//        if (shardId == null) {
//            if (other.shardId != null)
//                return false;
//        } else if (!shardId.equals(other.shardId))
//            return false;
//        return true;
//    }
//
//
//
//    @Override
//    public String toString() {
//        return Balancer.toString(replica);
//    }
//}

