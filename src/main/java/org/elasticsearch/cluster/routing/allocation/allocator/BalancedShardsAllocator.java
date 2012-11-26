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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.FailedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.StartedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.Decision.Type;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import com.fasterxml.jackson.dataformat.yaml.snakeyaml.nodes.Node;

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
        
//        float weight = ((indexBalance * node.numReplicas(index)) - balancer.stats.avgReplicasOfIndexPerNode(index))
//                      + (replicaBalance * (node.numReplicas() - balancer.stats.avgReplicasPerNode()))
//                      + ((primaryBalance * node.numPrimaries()) - balancer.stats.avgPrimariesPerNode());

        float weightReplica = replicaBalance * (node.numReplicas() - balancer.stats.avgReplicasPerNode());
        float weightIndex = indexBalance * (node.numReplicas(index) - balancer.stats.avgReplicasOfIndexPerNode(index));
        float weightPrimary = primaryBalance * (node.numPrimaries() - balancer.stats.avgPrimariesPerNode());
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
    
    private final ArrayList<NodeInfo> nodes;
    private final HashSet<String> indices;
    private final RoutingAllocation allocation;
    private final WeightFunction weight;
    
    private float treshold = 0;
    
    public final Stats stats = new Stats();
    
    public Balancer(ESLogger logger, RoutingAllocation allocation, WeightFunction weight, float treshold) {
        this.logger = logger;
        this.allocation = allocation;
        this.nodes = new ArrayList<NodeInfo>();
        this.indices = new HashSet<String>();
        this.weight = weight;
        this.treshold = treshold;
        
        for(RoutingNode node : allocation.routingNodes()) {
            nodes.add(new NodeInfo(node.nodeId()));
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
            for(NodeInfo node : nodes) {
                sum += node.numReplicas();
            }
            return sum;
        }
        
        /**
         * @return Number of primaries shards of all indices 
         */
        public int numPrimaries() {
            int sum = 0;
            for(NodeInfo node : nodes) {
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
            for(NodeInfo node : nodes) {
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
            for(NodeInfo node : nodes) {
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
        NodeInfo[] nodes = this.nodes.toArray(new NodeInfo[this.nodes.size()]);
        WeightOrder order = new WeightOrder(this, weight, replica.index());
        Arrays.sort(nodes, order);
        
        for (int i=0; i<nodes.length; i++) {
            if(nodes[i].id.equals(node.nodeId())) {
                logger.info("\tNode found " + nodes[i].id);
                source = nodes[i];
                break;
            }
        }
        
        if(source == null) {
            return false;
        }
        
        ShardInfo info = source.getReplica(replica);
        
        if(info == null) {
            return false;
        }

        for (int i=0; i<nodes.length; i++) {
            if(nodes[i].id.equals(node.nodeId())) {
                continue;
            }
            RoutingNode target = allocation.routingNodes().node(nodes[i].id);
            Decision decision = allocation.deciders().canAllocate(replica, target, allocation);
            logger.info("\t" + nodes[i].id + " " + decision.type());
            if(decision.type() == Type.YES) {
                source.removeReplica(info);
                nodes[i].addReplica(new ShardInfo(replica, decision));
                
                allocation.routingNodes().node(nodes[i].id).add(new MutableShardRouting(replica.index(), replica.id(),
                        nodes[i].id, replica.currentNodeId(),
                        replica.primary(), ShardRoutingState.INITIALIZING, replica.version() + 1));
                
                info.replica.relocate(nodes[i].id);
                logger.info("\tReplica " + toString(info.replica) + " moved to " + nodes[i].id);
                return true;
            }
        }
        

        return changed;
    }
    
    class DistributionOrder implements Comparator<MutableShardRouting> {
        
        @Override
        public int compare(MutableShardRouting a, MutableShardRouting b) {
            if(a.assignedToNode()!=b.assignedToNode()) {
                return a.assignedToNode()?-1:1;
            } else if(a.primary()!=b.primary()) {
                return a.primary()?-1:1;
            } else {
                final int index = a.index().compareTo(b.index());
                if(index!=0) {
                    return index;
                } else {
                    final int shard = a.shardId().id()-b.shardId().id();
                    if(shard != 0) {
                        return shard;
                    } else {
                        final int id = a.id() - b.id();
                        if(id!=0) {
                            return id;
                        } else {
                            String aNode = a.assignedToNode()?(a.relocating()?a.relocatingNodeId():a.currentNodeId()):null;
                            String bNode = b.assignedToNode()?(b.relocating()?b.relocatingNodeId():b.currentNodeId()):null;
                            if(aNode==null && bNode!=null) {
                                return 1;
                            } else if(bNode==null && aNode!=null){
                                return -1;
                            } else if(aNode!=null && bNode!=null) {
                                return aNode.compareTo(bNode);
                            } else {
                                return a.hashCode()-b.hashCode();
                            }
                        }
                    }
                }
            }
        }
    }
    
    private boolean distributeReplicas(Collection<MutableShardRouting> replicas) {
        ArrayList<MutableShardRouting> others = new ArrayList<MutableShardRouting>();
        
        boolean failed = false;
        boolean changed = false;
        List<MutableShardRouting> added = new ArrayList<MutableShardRouting>();

        others.addAll(replicas);
        Collections.sort(others, new DistributionOrder());
        
        
        for(MutableShardRouting replica : others) {
            NodeInfo node = allocateReplica(replica);
            if(node==null) {
                failed = true;
                logger.info("\t\tFAILED TO ASSIGN REPLICA " + toString(replica) + " ");
            } else {
                logger.info("\t\tAssigned Replica " + toString(replica) + " to " + node.id);
                if(replica.unassigned()) {
                    allocation.routingNodes().node(node.id).add(replica);
                    changed=true;
                } else if(replica.initializing()) {
//                    replica.moveToStarted();
                }
                String replicanode = replica.relocating()?replica.relocatingNodeId():replica.currentNodeId();
                changed |= !node.id.equals(replicanode) ;

//                changed = true;
                added.add(replica);
            }
        }

        replicas.removeAll(added);
        
        return changed;
    }
    
    private NodeInfo allocateReplica(MutableShardRouting replica) {
        assert !nodes.isEmpty();
        // find an allocatable node which weight is minimal

        String nodeid = replica.relocating()?replica.relocatingNodeId():replica.currentNodeId();
        if(replica.assignedToNode()) {
            for (NodeInfo node : nodes) {
                if(node.id.equals(nodeid)) {
                    logger.debug("\t\t\tReassign replica "+ toString(replica) + " to " +node.id + ": "  + allocation.deciders().canAllocate(replica, allocation.routingNodes().node(node.id), allocation).toString());
                    if(node.addReplica(new ShardInfo(replica, Decision.single(Type.YES, "Already allocated on node", node.id)))) {
                        return node;
                    }
                }
            }
        }
        
        logger.info("\t\t\t"+"Search node for Replica");
        
        NodeInfo minNode = null;
        float min = Float.POSITIVE_INFINITY;
        Decision decision = null;

        for (NodeInfo node : nodes) {
            ShardInfo shard = new ShardInfo(replica, decision);
            if(node.addReplica(shard)) {
                float w = weight.weight(this, node, replica.index());
                if (w < min) {
                    RoutingNode routing = allocation.routingNodes().node(node.id);
                    Decision currentDecision = allocation.deciders().canAllocate(replica, routing, allocation);
                    logger.info("\t\t\t\t" + node.id + " " + currentDecision.toString());
                    if (currentDecision.type() == Type.YES) {
                        minNode = node;
                        min = w;
                        decision = currentDecision;
                    }
                }
                if(!node.removeReplica(shard)) {
                    throw new RuntimeException("Unable to reinsert shardinfo");
                }
            }
        }

        if (minNode != null) {
            if(minNode.addReplica(new ShardInfo(replica, decision))) {
                return minNode;
            }
        }
        return null;
    }
    
    protected ShardInfo relocateSomeReplica(NodeInfo src, NodeInfo dst, String idx) {
        IndexInfo index = src.getIndexInfo(idx);
        if(index == null) {
            return null;
        } else {

            logger.info("Relocate some replica of '"+idx+"' to move form '" + src.id + "' to '" + dst.id+"'");

            RoutingNode node = allocation.routingNodes().node(dst.id);
            float minCost = Float.POSITIVE_INFINITY;
            ShardInfo candidate = null;
            Decision decision = null;
                        
            Collection<ShardInfo> allReplicas = new ArrayList<ShardInfo>(index.replicas);
            
            for(ShardInfo info : allReplicas) {
                
                Decision allocationDecision = allocation.deciders().canAllocate(info.replica, node, allocation);
                Decision rebalanceDecission = allocation.deciders().canRebalance(info.replica, allocation);
                
                if((allocationDecision.type() == Type.YES) || (allocationDecision.type() == Type.THROTTLE)) {
                    if((rebalanceDecission.type() == Type.YES) || (rebalanceDecission.type() == Type.THROTTLE)) {
                        
                        if(src.removeReplica(info)) {
                            if(dst.addReplica(info)) {
                                
                                float srcWeight = weight.weight(this, src, idx);
                                float dstWeight = weight.weight(this, dst, idx);
                                float currentCost = dstWeight-srcWeight; 
    
                                logger.info("\t" + toString(info.replica) + " canAllocate=" + allocationDecision.type() + " canRebalance="+rebalanceDecission.type()+" optimize="+currentCost);
                                
                                if(currentCost<minCost) {
                                    minCost = currentCost;
                                    candidate = info;
                                    decision = new Decision.Multi().add(allocationDecision).add(rebalanceDecission);
                                }
                                dst.removeReplica(info);
                            }
                            src.addReplica(info);
                        }
                    }
                }
            }
            
            if(candidate != null) {
                src.removeReplica(candidate);
                dst.addReplica(candidate);
                candidate.decision = decision;
                return candidate;
            } else {
                return null;
            }
        }
    }
    
    private boolean initateReplicas(RoutingNodes routing) {
        Collection<MutableShardRouting> replicas = new ArrayList<MutableShardRouting>();
        
        logger.info("\t-------- DISTRIBUTING REPLICAS --------");

        for(IndexRoutingTable index : allocation.routingTable().indicesRouting().values()) {
            indices.add(index.index());
            for(IndexShardRoutingTable shard : index.getShards().values()) {
                replicas.addAll(routing.shardsRoutingFor(index.index(), shard.shardId().id()));
            }
        }

        boolean changed = distributeReplicas(replicas);
        printState("\t", changed);
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
        
        logger.info("-------- INITIALIZE BALANCE --------");
        boolean changed = initateReplicas(allocation.routingNodes());
        
//        if(changed)
//            return true;
        
        logger.info("----------- REBALANCE --------------");
        logger.info(stats.toString());
        
        if(nodes.size()<=1) {
            return changed;
        } else {

            boolean rebalanced = false;
            for(String index : indices) {
                final WeightOrder order = new WeightOrder(this, weight, index);
                rebalanced |= balanceIndex(index, order);
            }

            printState(changed | rebalanced);
            return changed | rebalanced;
        }
    }
    
    private boolean balanceIndex(String index, WeightOrder order) {
        
        boolean changed = false;
        
        TreeSet<NodeInfo> nodes = new TreeSet<NodeInfo>(order);
        Collection<NodeInfo> ignoredMin = new ArrayList<NodeInfo>(nodes.size());
        Collection<NodeInfo> ignoredMax = new ArrayList<NodeInfo>(nodes.size());

        nodes.addAll(this.nodes);
        
        while(true) {

            NodeInfo minNode = nodes.pollFirst();
            NodeInfo maxNode = nodes.pollLast();

//            if(order.weight(maxNode)-order.weight(minNode)<treshold) {
//                break;
//            }
            
            boolean moved = false;
            if(maxNode.numReplicas(index) > 0){
                
//                if(index.equals("test13") || index.equals("test11")) {
//                
//                    StringBuilder sb = new StringBuilder("[");
//                    for (NodeInfo n : ignoredMin)
//                        sb.append(" ").append(n.id);
//                    sb.append(" ]");
//                    sb.append(" " + minNode.id);
//                    for (NodeInfo n : nodes)
//                        sb.append(" ").append(n.id);
//                    sb.append(" " + maxNode.id);
//                    sb.append(" [");
//                    for (NodeInfo n : ignoredMax)
//                        sb.append(" ").append(n.id);
//                    sb.append(" ]");
//                    logger.info(sb.toString());
//                }
                
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
            ShardInfo replica = relocateSomeReplica(maxNode, minNode, index);
            if(replica != null) {
                logger.debug("\t\tReplica: " + toString(replica.replica));
                //Move in Simulation
                maxNode.removeReplica(replica);
                minNode.addReplica(replica);

                //Move on Cluster
                if(replica.replica.started()) {
                    if(!minNode.id.equals(replica.replica.currentNodeId())) {
                        RoutingNode lowRoutingNode = allocation.routingNodes().node(minNode.id);
                        
                        lowRoutingNode.add(new MutableShardRouting(replica.replica.index(), replica.replica.id(),
                                lowRoutingNode.nodeId(), replica.replica.currentNodeId(),
                                replica.replica.primary(), INITIALIZING, replica.replica.version() + 1));
                        
                        replica.replica.relocate(minNode.id);
                        logger.info("\tMoved " + toString(replica.replica));
                        changed = true;
                    }
                } else if(replica.replica.unassigned()) {
                    logger.info("\tAssigned " + toString(replica.replica));
                    allocation.routingNodes().node(minNode.id).add(replica.replica);
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

    
    private boolean balanceIndex(String index, NodeInfo maxNode, TreeSet<NodeInfo> minNodes, WeightOrder order) {
        boolean changed = false;

        TreeSet<NodeInfo> nodes = new TreeSet<NodeInfo>(order);
        nodes.addAll(minNodes);
        
        while (nodes.size()>=1) {
            final NodeInfo minNode = nodes.pollFirst();
            boolean moved = balanceRelocation(index, minNode, maxNode, order);
            if(moved) {
                changed = true;
                minNodes.add(minNode);
            }
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

class NodeInfo implements Iterable<IndexInfo> {
    final String id;
    private final Map<String, IndexInfo> indices = new HashMap<String, IndexInfo>();
    
    public NodeInfo(String id) {
        super();
        this.id = id;
    }
    
    public IndexInfo getIndexInfo(String indexId) {
      return indices.get(indexId);
    }
    
    
    public int numReplicas() {
        int sum = 0;
        for(IndexInfo index : indices.values()) {
            sum += index.replicas.size();
        }
        return sum;
    }
    
    public int numReplicas(String idx) {
        IndexInfo index = indices.get(idx);
        if(index == null) {
            return 0;
        } else {
            return index.replicas.size();
        }
    }

    public int numPrimaries(String idx) {
        IndexInfo index = indices.get(idx);
        if(index == null) {
            return 0;
        } else {
            int sum = 0;
            for (ShardInfo info : index.replicas) {
                if(info.replica.primary()) {
                    sum++;
                }
            }
            return sum;
        }
    }

    public int numPrimaries() {
        int sum = 0;
        for(IndexInfo index : indices.values()) {
            for (ShardInfo info : index.replicas) {
                if(info.replica.primary()) {
                    sum++;
                }
            }
        }
        return sum;
    }
    

    public Collection<ShardInfo> replicas() {
        Collection<ShardInfo> result = new ArrayList<ShardInfo>();
        for(IndexInfo index : indices.values()) {
            result.addAll(index.replicas);
        }
        return result;
    }
    
    public boolean addReplica(ShardInfo info) {
        IndexInfo index = indices.get(info.replica.index());
        if(index == null) {
            index = new IndexInfo(info.replica.index());
            indices.put(index.id, index);
        }
        return index.replicas.add(info);
    }
    
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Node("+id+"):\n");
        for(IndexInfo index : indices.values()) {
            sb.append('\t').append("index("+index.id+"):");
            for(ShardInfo shard : index.replicas) {
                sb.append(' ').append(Balancer.toString(shard.replica));
            }
            sb.append('\n');
        }
        return sb.toString();
    }
    
    public boolean removeReplica(ShardInfo info) {
        IndexInfo index = indices.get(info.replica.index());
        if(index==null){
            return false;
        } else {
            boolean removed = index.removeReplica(info);
            if(removed && index.replicas.isEmpty()) {
                indices.remove(info.replica.index());
            }
            return removed;
        }
    }

    @Override
    public Iterator<IndexInfo> iterator() {
        return indices.values().iterator();
    }
    
    public ShardInfo getReplica(MutableShardRouting replica) {
        IndexInfo info = getIndexInfo(replica.getIndex());
        if(info == null) {
            System.err.println("\tNo such index");
            return null;
        } else {
            return info.getReplica(replica);
        }
    }


}

class IndexInfo implements Comparator<ShardInfo> {
    protected final String id;
    final Collection<ShardInfo> replicas = new TreeSet<ShardInfo>(this);
    
    public IndexInfo(String id) {
        super();
        this.id = id;
    }

    public boolean removeReplica(ShardInfo replica) {
        return replicas.remove(replica);
    }
    
    public boolean addReplica(ShardInfo replica) {
        return replicas.add(replica);
    }
    
    @Override
    public int compare(ShardInfo o1, ShardInfo o2) {
        return o1.replica.id()-o2.replica.id();
    }
    
    public ShardInfo getReplica(MutableShardRouting replica) {
        for (ShardInfo info : replicas) {
            if(info.replica.id() == replica.id())
                return info;
        }
        System.err.println("\tNo such replica");
        return null;
    }
    
}

class ShardInfo {
    final MutableShardRouting replica;
    Decision decision;
    
//    public ShardInfo(MutableShardRouting replica) {
//        this(replica, null);
//    }
    
    public ShardInfo(MutableShardRouting replica, Decision decision) {
        super();
        this.replica = replica;
        this.decision = decision;
    }
    
    @Override
    public String toString() {
        return Balancer.toString(replica);
    }
}

