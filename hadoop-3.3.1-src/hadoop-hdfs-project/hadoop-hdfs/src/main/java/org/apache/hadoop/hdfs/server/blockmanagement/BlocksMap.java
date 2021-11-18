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

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.LightWeightGSet;

/**
 * // TODO_MA 马中华 注释： 这个类，用来维护 数据块 元数据
 * This class maintains the map from a block to its metadata.
 * block's metadata currently includes blockCollection it belongs to and
 * the datanodes that store the block.
 */

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： BlocksMap 是 Namenode 上与数据块相关的最重要的类， 它管理着 Namenode 上数据块的元数据，
 *  包括当前数据块属于哪个 HDFS 文件， 以及当前数据块保存在哪些 Datanode 上。
 *  当 Datanode 启动时， 会对 Datanode 的本地磁盘进行扫描， 并将当前 Datanode 上保存的数据块信息汇报到 Namenode。
 *  Namenode 收到 Datanode 的汇报信息后， 会建立数据块与保存这个数据块的数据节点的对应关系，
 *  并将这个信息保存在 BlocksMap 中。
 *  所以无论是获取某个数据块对应的 HDFS 文件， 还是获取数据块保存在哪些数据节点上， 都需要通 BlocksMap 对象.
 */
class BlocksMap {

    /** Constant {@link LightWeightGSet} capacity. */
    // TODO_MA 马中华 注释： 容量
    private final int capacity;

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 数据块 集合
     */
    private GSet<Block, BlockInfo> blocks;

    private final LongAdder totalReplicatedBlocks = new LongAdder();
    private final LongAdder totalECBlockGroups = new LongAdder();

    BlocksMap(int capacity) {
        // Use 2% of total memory to size the GSet capacity
        this.capacity = capacity;
        // TODO_MA 马中华 注释： 容器初始化
        this.blocks = new LightWeightGSet<Block, BlockInfo>(capacity) {
            @Override
            public Iterator<BlockInfo> iterator() {
                SetIterator iterator = new SetIterator();
                /*
                 * Not tracking any modifications to set. As this set will be used
                 * always under FSNameSystem lock, modifications will not cause any
                 * ConcurrentModificationExceptions. But there is a chance of missing
                 * newly added elements during iteration.
                 */
                iterator.setTrackModification(false);
                return iterator;
            }
        };
    }


    void close() {
        clear();
        blocks = null;
    }

    void clear() {
        if (blocks != null) {
            blocks.clear();
            totalReplicatedBlocks.reset();
            totalECBlockGroups.reset();
        }
    }

    /**
     * Add block b belonging to the specified block collection to the map.
     */
    BlockInfo addBlockCollection(BlockInfo b, BlockCollection bc) {
        BlockInfo info = blocks.get(b);
        if (info != b) {
            info = b;
            blocks.put(info);
            incrementBlockStat(info);
        }
        info.setBlockCollectionId(bc.getId());
        return info;
    }

    /**
     * Remove the block from the block map;
     * remove it from all data-node lists it belongs to;
     * and remove all data-node locations associated with the block.
     */
    void removeBlock(BlockInfo block) {
        BlockInfo blockInfo = blocks.remove(block);
        if (blockInfo == null) {
            return;
        }
        decrementBlockStat(block);

        assert blockInfo.getBlockCollectionId() == INodeId.INVALID_INODE_ID;
        final int size = blockInfo.isStriped() ? blockInfo.getCapacity() : blockInfo.numNodes();
        for (int idx = size - 1; idx >= 0; idx--) {
            DatanodeDescriptor dn = blockInfo.getDatanode(idx);
            if (dn != null) {
                removeBlock(dn, blockInfo); // remove from the list and wipe the location
            }
        }
    }

    /**
     * Check if BlocksMap contains the block.
     *
     * @param b Block to check
     * @return true if block is in the map, otherwise false
     */
    boolean containsBlock(Block b) {
        return blocks.contains(b);
    }

    /** Returns the block object if it exists in the map. */
    BlockInfo getStoredBlock(Block b) {
        return blocks.get(b);
    }

    /**
     * Searches for the block in the BlocksMap and
     * returns {@link Iterable} of the storages the block belongs to.
     */
    Iterable<DatanodeStorageInfo> getStorages(Block b) {
        BlockInfo block = blocks.get(b);
        return block != null ? getStorages(block) : Collections.<DatanodeStorageInfo>emptyList();
    }

    /**
     * For a block that has already been retrieved from the BlocksMap
     * returns {@link Iterable} of the storages the block belongs to.
     */
    Iterable<DatanodeStorageInfo> getStorages(final BlockInfo storedBlock) {
        if (storedBlock == null) {
            return Collections.emptyList();
        } else {
            return new Iterable<DatanodeStorageInfo>() {
                @Override
                public Iterator<DatanodeStorageInfo> iterator() {
                    return storedBlock.getStorageInfos();
                }
            };
        }
    }

    /** counts number of containing nodes. Better than using iterator. */
    int numNodes(Block b) {
        BlockInfo info = blocks.get(b);
        return info == null ? 0 : info.numNodes();
    }

    /**
     * Remove data-node reference from the block.
     * Remove the block from the block map
     * only if it does not belong to any file and data-nodes.
     */
    boolean removeNode(Block b, DatanodeDescriptor node) {
        BlockInfo info = blocks.get(b);
        if (info == null) return false;

        // remove block from the data-node set and the node from the block info
        boolean removed = removeBlock(node, info);

        if (info.hasNoStorage()    // no datanodes left
                && info.isDeleted()) { // does not belong to a file
            blocks.remove(b);  // remove block from the map
            decrementBlockStat(info);
        }
        return removed;
    }

    /**
     * Remove block from the set of blocks belonging to the data-node. Remove
     * data-node from the block.
     */
    static boolean removeBlock(DatanodeDescriptor dn, BlockInfo b) {
        final DatanodeStorageInfo s = b.findStorageInfo(dn);
        // if block exists on this datanode
        return s != null && s.removeBlock(b);
    }

    int size() {
        if (blocks != null) {
            return blocks.size();
        } else {
            return 0;
        }
    }

    Iterable<BlockInfo> getBlocks() {
        return blocks;
    }

    /** Get the capacity of the HashMap that stores blocks */
    int getCapacity() {
        return capacity;
    }

    private void incrementBlockStat(BlockInfo block) {
        if (block.isStriped()) {
            totalECBlockGroups.increment();
        } else {
            totalReplicatedBlocks.increment();
        }
    }

    private void decrementBlockStat(BlockInfo block) {
        if (block.isStriped()) {
            totalECBlockGroups.decrement();
            assert totalECBlockGroups.longValue() >= 0 : "Total number of ec block groups should be non-negative";
        } else {
            totalReplicatedBlocks.decrement();
            assert totalReplicatedBlocks.longValue() >= 0 : "Total number of replicated blocks should be non-negative";
        }
    }

    long getReplicatedBlocks() {
        return totalReplicatedBlocks.longValue();
    }

    long getECBlockGroups() {
        return totalECBlockGroups.longValue();
    }
}
