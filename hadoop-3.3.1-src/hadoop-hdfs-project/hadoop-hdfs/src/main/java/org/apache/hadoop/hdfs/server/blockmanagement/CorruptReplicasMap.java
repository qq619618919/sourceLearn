/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.Server;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Stores information about all corrupt blocks in the File System.
 * A Block is considered corrupt only if all of its replicas are
 * corrupt. While reporting replicas of a Block, we hide any corrupt
 * copies. These copies are removed once Block is found to have 
 * expected number of good replicas.
 * Mapping: Block {@literal -> TreeSet<DatanodeDescriptor>}
 */

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： CorruptReplicasMap 类用于保存损坏的数据块副本（corruptReplica） 集合。
 *  客户端发现损坏的数据块时会通过 ClientProtocol.reportBadBlocks() 方法向 Namenode 汇报损坏的数据块副本，
 *  数据节点会通过DatanodeProtocol.reportBadBlocks() 方法汇报损坏的数据块副本，
 *  之后 BlockManager 会将损坏的副本加入这个数据结构中。
 */
@InterfaceAudience.Private
public class CorruptReplicasMap {

    // TODO_MA 马中华 注释： CorruptReplicasMap 保存的是损坏的数据块副本与保存这个副本的 Datanode 的
    // TODO_MA 马中华 注释： 对应关系（Block ->Datanode的映射关系）， 注意这里同时还保存了这个副本损坏的原因。
    // TODO_MA 马中华 注释： 总共有 6 种
    /** The corruption reason code */
    public enum Reason {
        NONE,                // not specified.
        ANY,                 // wildcard reason
        GENSTAMP_MISMATCH,   // mismatch in generation stamps // TODO_MA 马中华 注释： Datanode 上副本的时间戳与 Namenode 上数据块的时间戳不一致
        SIZE_MISMATCH,       // mismatch in sizes    // TODO_MA 马中华 注释： Datanode 上副本的大小与 Namenode 上数据块的大小不一致
        INVALID_STATE,       // invalid state    // TODO_MA 马中华 注释： 无效的状态
        CORRUPTION_REPORTED  // client or datanode reported the corruption    // TODO_MA 马中华 注释： client 或 datanode 报告了损坏
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 内部核心数据结构
     */
    private final Map<Block, Map<DatanodeDescriptor, Reason>> corruptReplicasMap = new HashMap<Block, Map<DatanodeDescriptor, Reason>>();

    private final LongAdder totalCorruptBlocks = new LongAdder();
    private final LongAdder totalCorruptECBlockGroups = new LongAdder();

    /**
     * Mark the block belonging to datanode as corrupt.
     *
     * @param blk Block to be added to CorruptReplicasMap
     * @param dn DatanodeDescriptor which holds the corrupt replica
     * @param reason a textual reason (for logging purposes)
     * @param reasonCode the enum representation of the reason
     */
    void addToCorruptReplicasMap(Block blk, DatanodeDescriptor dn, String reason, Reason reasonCode, boolean isStriped) {
        Map<DatanodeDescriptor, Reason> nodes = corruptReplicasMap.get(blk);
        if (nodes == null) {
            nodes = new HashMap<DatanodeDescriptor, Reason>();
            corruptReplicasMap.put(blk, nodes);
            incrementBlockStat(isStriped);
        }

        String reasonText;
        if (reason != null) {
            reasonText = " because " + reason;
        } else {
            reasonText = "";
        }

        if (!nodes.keySet().contains(dn)) {
            NameNode.blockStateChangeLog.debug("BLOCK NameSystem.addToCorruptReplicasMap: {} added as corrupt on " + "{} by {} {}", blk, dn,
                    Server.getRemoteIp(), reasonText);
        } else {
            NameNode.blockStateChangeLog.debug(
                    "BLOCK NameSystem.addToCorruptReplicasMap: duplicate requested for" + " {} to add as corrupt on {} by {} {}", blk, dn,
                    Server.getRemoteIp(), reasonText);
        }
        // Add the node or update the reason.
        nodes.put(dn, reasonCode);
    }

    /**
     * Remove Block from CorruptBlocksMap.
     * @param blk Block to be removed
     */
    void removeFromCorruptReplicasMap(BlockInfo blk) {
        if (corruptReplicasMap != null) {
            Map<DatanodeDescriptor, Reason> value = corruptReplicasMap.remove(blk);
            if (value != null) {
                decrementBlockStat(blk.isStriped());
            }
        }
    }

    /**
     * Remove the block at the given datanode from CorruptBlockMap
     * @param blk block to be removed
     * @param datanode datanode where the block is located
     * @return true if the removal is successful;
    false if the replica is not in the map
     */
    boolean removeFromCorruptReplicasMap(BlockInfo blk, DatanodeDescriptor datanode) {
        return removeFromCorruptReplicasMap(blk, datanode, Reason.ANY);
    }

    boolean removeFromCorruptReplicasMap(BlockInfo blk, DatanodeDescriptor datanode, Reason reason) {
        Map<DatanodeDescriptor, Reason> datanodes = corruptReplicasMap.get(blk);
        if (datanodes == null) {
            return false;
        }

        // if reasons can be compared but don't match, return false.
        Reason storedReason = datanodes.get(datanode);
        if (reason != Reason.ANY && storedReason != null && reason != storedReason) {
            return false;
        }

        if (datanodes.remove(datanode) != null) { // remove the replicas
            if (datanodes.isEmpty()) {
                // remove the block if there is no more corrupted replicas
                corruptReplicasMap.remove(blk);
                decrementBlockStat(blk.isStriped());
            }
            return true;
        }
        return false;
    }

    private void incrementBlockStat(boolean isStriped) {
        if (isStriped) {
            totalCorruptECBlockGroups.increment();
        } else {
            totalCorruptBlocks.increment();
        }
    }

    private void decrementBlockStat(boolean isStriped) {
        if (isStriped) {
            totalCorruptECBlockGroups.decrement();
        } else {
            totalCorruptBlocks.decrement();
        }
    }

    /**
     * Get Nodes which have corrupt replicas of Block
     *
     * @param blk Block for which nodes are requested
     * @return collection of nodes. Null if does not exists
     */
    Collection<DatanodeDescriptor> getNodes(Block blk) {
        Map<DatanodeDescriptor, Reason> nodes = corruptReplicasMap.get(blk);
        if (nodes == null) return null;
        return nodes.keySet();
    }

    /**
     * Check if replica belonging to Datanode is corrupt
     *
     * @param blk Block to check
     * @param node DatanodeDescriptor which holds the replica
     * @return true if replica is corrupt, false if does not exists in this map
     */
    boolean isReplicaCorrupt(Block blk, DatanodeDescriptor node) {
        Collection<DatanodeDescriptor> nodes = getNodes(blk);
        return ((nodes != null) && (nodes.contains(node)));
    }

    int numCorruptReplicas(Block blk) {
        Collection<DatanodeDescriptor> nodes = getNodes(blk);
        return (nodes == null) ? 0 : nodes.size();
    }

    int size() {
        return corruptReplicasMap.size();
    }

    /**
     * Return a range of corrupt replica block ids. Up to numExpectedBlocks
     * blocks starting at the next block after startingBlockId are returned
     * (fewer if numExpectedBlocks blocks are unavailable). If startingBlockId
     * is null, up to numExpectedBlocks blocks are returned from the beginning.
     * If startingBlockId cannot be found, null is returned.
     *
     * @param bim BlockIdManager to determine the block type.
     * @param blockType desired block type to return.
     * @param numExpectedBlocks Number of block ids to return.
     *  0 <= numExpectedBlocks <= 100
     * @param startingBlockId Block id from which to start. If null, start at
     *  beginning.
     * @return Up to numExpectedBlocks blocks from startingBlockId if it exists
     */
    @VisibleForTesting
    long[] getCorruptBlockIdsForTesting(BlockIdManager bim, BlockType blockType, int numExpectedBlocks, Long startingBlockId) {
        if (numExpectedBlocks < 0 || numExpectedBlocks > 100) {
            return null;
        }
        long cursorBlockId = startingBlockId != null ? startingBlockId : Long.MIN_VALUE;
        return corruptReplicasMap.keySet().stream().filter(r -> {
            if (blockType == BlockType.STRIPED) {
                return bim.isStripedBlock(r) && r.getBlockId() >= cursorBlockId;
            } else {
                return !bim.isStripedBlock(r) && r.getBlockId() >= cursorBlockId;
            }
        }).sorted().limit(numExpectedBlocks).mapToLong(Block::getBlockId).toArray();
    }

    /**
     * method to get the set of corrupt blocks in corruptReplicasMap.
     * @return Set of Block objects
     */
    Set<Block> getCorruptBlocksSet() {
        Set<Block> corruptBlocks = new HashSet<Block>();
        corruptBlocks.addAll(corruptReplicasMap.keySet());
        return corruptBlocks;
    }

    /**
     * return the reason about corrupted replica for a given block
     * on a given dn
     * @param block block that has corrupted replica
     * @param node datanode that contains this corrupted replica
     * @return reason
     */
    String getCorruptReason(Block block, DatanodeDescriptor node) {
        Reason reason = null;
        if (corruptReplicasMap.containsKey(block)) {
            if (corruptReplicasMap.get(block).containsKey(node)) {
                reason = corruptReplicasMap.get(block).get(node);
            }
        }
        if (reason != null) {
            return reason.toString();
        } else {
            return null;
        }
    }

    long getCorruptBlocks() {
        return totalCorruptBlocks.longValue();
    }

    long getCorruptECBlockGroups() {
        return totalCorruptECBlockGroups.longValue();
    }
}
