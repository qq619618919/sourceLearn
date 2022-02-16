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
package org.apache.hadoop.hdfs.server.datanode;

import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo.BlockStatus;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;

/**
 * Manage Incremental Block Reports (IBRs).
 */
@InterfaceAudience.Private
class IncrementalBlockReportManager {
    private static final Logger LOG = LoggerFactory.getLogger(IncrementalBlockReportManager.class);

    // TODO_MA 马中华 注释：  每个 Storage 维护一个 Map
    private static class PerStorageIBR {
        /** The blocks in this IBR. */
        final Map<Block, ReceivedDeletedBlockInfo> blocks = Maps.newHashMap();

        private DataNodeMetrics dnMetrics;

        PerStorageIBR(final DataNodeMetrics dnMetrics) {
            this.dnMetrics = dnMetrics;
        }

        /**
         * Remove the given block from this IBR
         * @return true if the block was removed; otherwise, return false.
         */
        ReceivedDeletedBlockInfo remove(Block block) {
            return blocks.remove(block);
        }

        /** @return all the blocks removed from this IBR. */
        ReceivedDeletedBlockInfo[] removeAll() {
            final int size = blocks.size();
            if (size == 0) {
                return null;
            }

            final ReceivedDeletedBlockInfo[] rdbis = blocks.values().toArray(new ReceivedDeletedBlockInfo[size]);
            blocks.clear();
            return rdbis;
        }

        /** Put the block to this IBR. */
        void put(ReceivedDeletedBlockInfo rdbi) {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 每个 datanode 的里面都有一个集合 blocks，去管理 变动的 block
             *  datanode 有一个数据块汇报机制
             *  1、增量汇报： 其实就是把这个 blocks 集合中的 block 信息汇报给 namenode
             *      实时处理，但是老板不相信
             *  2、全量汇报： datanode 扫描所有存储目录中的所有 block 然后执行汇报！
             *      批处理：校验
             *  namenode 管理者， datanode 汇报数据块的状态给 namenode
             */
            blocks.put(rdbi.getBlock(), rdbi);
            increaseBlocksCounter(rdbi);
        }

        private void increaseBlocksCounter(final ReceivedDeletedBlockInfo receivedDeletedBlockInfo) {
            switch (receivedDeletedBlockInfo.getStatus()) {
                case RECEIVING_BLOCK:
                    dnMetrics.incrBlocksReceivingInPendingIBR();
                    break;
                case RECEIVED_BLOCK:
                    dnMetrics.incrBlocksReceivedInPendingIBR();
                    break;
                case DELETED_BLOCK:
                    dnMetrics.incrBlocksDeletedInPendingIBR();
                    break;
                default:
                    break;
            }
            dnMetrics.incrBlocksInPendingIBR();
        }

        /**
         * Put the all blocks to this IBR unless the block already exists.
         * @param rdbis list of blocks to add.
         * @return the number of missing blocks added.
         */
        int putMissing(ReceivedDeletedBlockInfo[] rdbis) {
            int count = 0;
            for (ReceivedDeletedBlockInfo rdbi : rdbis) {
                if (!blocks.containsKey(rdbi.getBlock())) {
                    put(rdbi);
                    count++;
                }
            }
            return count;
        }
    }

    /**
     * // TODO_MA 马中华 注释： 在块报告之间（大约每小时发生一次），DN 报告每个存储的块列表的较小增量更改。
     * // TODO_MA 马中华 注释： 该 map 包含尚未报告给 NN 的待定更改。
     * // TODO_MA 马中华 注释： 通俗理解： 待汇报的新增数据块
     * Between block reports (which happen on the order of once an hour) the
     * DN reports smaller incremental changes to its block list for each storage.
     * This map contains the pending changes not yet to be reported to the NN.
     */
    private final Map<DatanodeStorage, PerStorageIBR> pendingIBRs = Maps.newHashMap();

    /**
     * If this flag is set then an IBR will be sent immediately by the actor
     * thread without waiting for the IBR timer to elapse.
     */
    private volatile boolean readyToSend = false;

    /** The time interval between two IBRs. */
    private final long ibrInterval;

    /** The timestamp of the last IBR. */
    private volatile long lastIBR;
    private DataNodeMetrics dnMetrics;

    IncrementalBlockReportManager(final long ibrInterval, final DataNodeMetrics dnMetrics) {
        this.ibrInterval = ibrInterval;
        this.lastIBR = monotonicNow() - ibrInterval;
        this.dnMetrics = dnMetrics;
    }

    boolean sendImmediately() {
        return readyToSend && monotonicNow() - ibrInterval >= lastIBR;
    }

    synchronized void waitTillNextIBR(long waitTime) {
        if (waitTime > 0 && !sendImmediately()) {
            try {
                wait(ibrInterval > 0 && ibrInterval < waitTime ? ibrInterval : waitTime);
            } catch (InterruptedException ie) {
                LOG.warn(getClass().getSimpleName() + " interrupted");
            }
        }
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 最重要的是，就是这个数据结构： pendingIBRs， 存储的就是最近一段时间内新增的数据块
     *  新增： 代表是变化！
     *  1、增加了数据块
     *  2、减少了数据块
     */
    private synchronized StorageReceivedDeletedBlocks[] generateIBRs() {

        // TODO_MA 马中华 注释： 结果容器
        final List<StorageReceivedDeletedBlocks> reports = new ArrayList<>(pendingIBRs.size());

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 遍历 pendingIBRs 中的每个 PerStorageIBR 生成一个 StorageReceivedDeletedBlocks
         *  当我们完成一个数据块上传的时候，必然会有 3 个 datanode 新增一个数据块。
         *  那么这 3 个datanode 的 pendingIBRs 就会增加一个元素！
         */
        for (Map.Entry<DatanodeStorage, PerStorageIBR> entry : pendingIBRs.entrySet()) {
            final PerStorageIBR perStorage = entry.getValue();

            // Send newly-received and deleted blockids to namenode
            final ReceivedDeletedBlockInfo[] rdbi = perStorage.removeAll();
            if (rdbi != null) {

                // TODO_MA 马中华 注释： 构建 StorageReceivedDeletedBlocks 加入 List 容器中
                // TODO_MA 马中华 注释： Received 其他datanode 发送过的：  负载均衡的， 数据上传过程中
                // TODO_MA 马中华 注释： Deleted 负载均衡移动， 可能某些数据块的副本个数多了
                reports.add(new StorageReceivedDeletedBlocks(entry.getKey(), rdbi));
            }
        }

        /* set blocks to zero */
        this.dnMetrics.resetBlocksInPendingIBR();

        readyToSend = false;

        // TODO_MA 马中华 注释： 返回结果， 数组类型
        return reports.toArray(new StorageReceivedDeletedBlocks[reports.size()]);
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 元数据有三种类型：
     *  1、目录数据结构
     *  2、File 和 Blick 的映射关系
     *  3、Block 和 副本的映射关系
     *  -
     *  结论：
     *  1 和 2 两种元数据，在磁盘里面有，在内存里面也有
     *  3 这种类型的元数据，只在内存中，磁盘中么有
     *  -
     *  问题：既然磁盘没有，那么 anmenode 怎么知道每个数据块的分布呢
     *  冷启动恢复：
     *  1、加载磁盘元数据，恢复 元数据种类 1  和 2
     *  2、namenode 等待 datanode 上线然后做 数据块汇报，恢复元数据3，
     *  元数据种类3 就是 临时构建的
     *  -
     *  关于心跳和数据块汇报这个事儿，是每个datanode 需要向每个 namenode 做的事儿
     *  -
     *  这两件事就是一个 BPServiceActor干的事情：
     *  connectToNNandHandeShake()  建立连接和握手、注册
     *  offserSerice()    心跳和数据块汇报！
     */

    private synchronized void putMissing(StorageReceivedDeletedBlocks[] reports) {
        for (StorageReceivedDeletedBlocks r : reports) {
            pendingIBRs.get(r.getStorage()).putMissing(r.getBlocks());
        }
        if (reports.length > 0) {
            readyToSend = true;
        }
    }

    /** Send IBRs to namenode. */
    void sendIBRs(DatanodeProtocol namenode, DatanodeRegistration registration, String bpid,
                  String nnRpcLatencySuffix) throws IOException {

        // TODO_MA 马中华 注释： 生成 StorageReceivedDeletedBlocks
        // Generate a list of the pending reports for each storage under the lock
        final StorageReceivedDeletedBlocks[] reports = generateIBRs();

        // TODO_MA 马中华 注释： 如果没有需要汇报的，则直接返回
        if (reports.length == 0) {
            // Nothing new to report.
            return;
        }

        // Send incremental block reports to the Namenode outside the lock
        if (LOG.isDebugEnabled()) {
            LOG.debug("call blockReceivedAndDeleted: " + Arrays.toString(reports));
        }
        boolean success = false;
        final long startTime = monotonicNow();
        try {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 处理 Block 新增或者删除 的数据块 汇报
             *  发送 增量块汇报 的 RPC 请求给 NameNode
             */
            namenode.blockReceivedAndDeleted(registration, bpid, reports);
            success = true;
        } finally {

            if (success) {
                dnMetrics.addIncrementalBlockReport(monotonicNow() - startTime, nnRpcLatencySuffix);
                lastIBR = startTime;
            }
            // TODO_MA 马中华 注释： 如果当前这次汇报没成功，则把这些 BlockInfo 继续加入到队列中等待下一次汇报到来
            else {
                // If we didn't succeed in sending the report, put all of the
                // blocks back onto our queue, but only in the case where we
                // didn't put something newer in the meantime.
                putMissing(reports);
                LOG.warn("Failed to call blockReceivedAndDeleted: {}, nnId: {}" + ", duration(ms): {}",
                        Arrays.toString(reports), nnRpcLatencySuffix, monotonicNow() - startTime
                );
            }
        }
    }

    /** @return the pending IBR for the given {@code storage} */
    // TODO_MA 马中华 注释： 做个标记！
    private PerStorageIBR getPerStorageIBR(DatanodeStorage storage) {
        PerStorageIBR perStorage = pendingIBRs.get(storage);
        if (perStorage == null) {
            // This is the first time we are adding incremental BR state for
            // this storage so create a new map. This is required once per
            // storage, per service actor.
            perStorage = new PerStorageIBR(dnMetrics);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 如果有变化数据块，则加入到 pendingIBRs 集合
             */
            pendingIBRs.put(storage, perStorage);
        }
        return perStorage;
    }

    /**
     * Add a block for notification to NameNode.
     * If another entry exists for the same block it is removed.
     */
    @VisibleForTesting
    synchronized void addRDBI(ReceivedDeletedBlockInfo rdbi, DatanodeStorage storage) {
        // Make sure another entry for the same block is first removed.
        // There may only be one such entry.
        for (PerStorageIBR perStorage : pendingIBRs.values()) {
            if (perStorage.remove(rdbi.getBlock()) != null) {
                break;
            }
        }
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 将新增数据块 加入 Map<Block, ReceivedDeletedBlockInfo> blocks 增量数据块中
         */
        getPerStorageIBR(storage).put(rdbi);
    }

    synchronized void notifyNamenodeBlock(ReceivedDeletedBlockInfo rdbi, DatanodeStorage storage,
                                          boolean isOnTransientStorage) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 增加一个block ，到时候增量汇报
         */
        addRDBI(rdbi, storage);

        final BlockStatus status = rdbi.getStatus();
        if (status == BlockStatus.RECEIVING_BLOCK) {
            // the report will be sent out in the next heartbeat.
            readyToSend = true;
        } else if (status == BlockStatus.RECEIVED_BLOCK) {
            // the report is sent right away.
            triggerIBR(isOnTransientStorage);
        }
    }

    synchronized void triggerIBR(boolean force) {
        readyToSend = true;
        if (force) {
            lastIBR = monotonicNow() - ibrInterval;
        }
        if (sendImmediately()) {
            notifyAll();
        }
    }

    @VisibleForTesting
    synchronized void triggerDeletionReportForTests() {
        triggerIBR(true);

        while (sendImmediately()) {
            try {
                wait(100);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    void clearIBRs() {
        pendingIBRs.clear();
    }

    @VisibleForTesting
    int getPendingIBRSize() {
        return pendingIBRs.size();
    }
}