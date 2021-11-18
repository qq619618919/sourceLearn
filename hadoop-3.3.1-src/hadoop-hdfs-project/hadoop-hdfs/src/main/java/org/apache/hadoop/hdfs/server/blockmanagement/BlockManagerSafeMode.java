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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs.BlockReportReplica;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.RollingUpgradeStartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress.Counter;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Status;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.util.Daemon;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REPL_QUEUE_THRESHOLD_PCT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_EXTENSION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_EXTENSION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_MIN_DATANODES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY;
import static org.apache.hadoop.util.Time.monotonicNow;

/**
 * Block manager safe mode info.
 *
 * During name node startup, counts the number of <em>safe blocks</em>, those
 * that have at least the minimal number of replicas, and calculates the ratio
 * of safe blocks to the total number of blocks in the system, which is the size
 * of blocks. When the ratio reaches the {@link #threshold} and enough live data
 * nodes have registered, it needs to wait for the safe mode {@link #extension}
 * interval. After the extension period has passed, it will not leave safe mode
 * until the safe blocks ratio reaches the {@link #threshold} and enough live
 * data node registered.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class BlockManagerSafeMode {

    enum BMSafeModeStatus {
        PENDING_THRESHOLD,
        /** Pending on more safe blocks or live datanode. */
        EXTENSION,
        /** In extension period. */
        OFF                /** Safe mode is off. */
    }

    static final Logger LOG = LoggerFactory.getLogger(BlockManagerSafeMode.class);
    static final Step STEP_AWAITING_REPORTED_BLOCKS = new Step(StepType.AWAITING_REPORTED_BLOCKS);

    private final BlockManager blockManager;
    private final Namesystem namesystem;
    private final boolean haEnabled;
    private volatile BMSafeModeStatus status = BMSafeModeStatus.OFF;

    /** Safe mode threshold condition %.*/
    private final float threshold;
    /** Number of blocks needed to satisfy safe mode threshold condition. */
    private long blockThreshold;
    /** Total number of blocks. */
    private long blockTotal;
    /** Number of safe blocks. */
    private long blockSafe;
    /** Safe mode minimum number of datanodes alive. */
    private final int datanodeThreshold;
    /** Min replication required by safe mode. */
    private final int safeReplication;
    /** Threshold for populating needed replication queues. */
    private final float replQueueThreshold;
    /** Number of blocks needed before populating replication queues. */
    private long blockReplQueueThreshold;

    /** How long (in ms) is the extension period. */
    @VisibleForTesting
    final long extension;
    /** Timestamp of the first time when thresholds are met. */
    private final AtomicLong reachedTime = new AtomicLong();
    /** Timestamp of the safe mode initialized. */
    private long startTime;

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 监控安全模式的一个监控类
     */
    /** the safe mode monitor thread. */
    private final Daemon smmthread = new Daemon(new SafeModeMonitor());

    /** time of the last status printout */
    private long lastStatusReport;
    /** Counter for tracking startup progress of reported blocks. */
    private Counter awaitingReportedBlocksCounter;

    /** Keeps track of how many bytes are in Future Generation blocks. */
    private final LongAdder bytesInFutureBlocks = new LongAdder();
    private final LongAdder bytesInFutureECBlockGroups = new LongAdder();

    /** Reports if Name node was started with Rollback option. */
    private final boolean inRollBack;

    BlockManagerSafeMode(BlockManager blockManager, Namesystem namesystem, boolean haEnabled, Configuration conf) {
        this.blockManager = blockManager;
        this.namesystem = namesystem;
        this.haEnabled = haEnabled;

        // TODO_MA 马中华 注释： dfs.namenode.safemode.threshold-pct = 0.999f
        this.threshold = conf.getFloat(DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT);
        if (this.threshold > 1.0) {
            LOG.warn("The threshold value shouldn't be greater than 1, " + "threshold: {}", threshold);
        }
        // TODO_MA 马中华 注释： dfs.namenode.safemode.min.datanodes = 0
        this.datanodeThreshold = conf.getInt(DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY,
                DFS_NAMENODE_SAFEMODE_MIN_DATANODES_DEFAULT
        );
        int minReplication = conf.getInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY,
                DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_DEFAULT
        );
        // DFS_NAMENODE_SAFEMODE_REPLICATION_MIN_KEY is an expert level setting,
        // setting this lower than the min replication is not recommended
        // and/or dangerous for production setups.
        // When it's unset, safeReplication will use dfs.namenode.replication.min
        // TODO_MA 马中华 注释： dfs.namenode.safemode.replication.min = 1
        this.safeReplication = conf.getInt(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_REPLICATION_MIN_KEY, minReplication);
        // default to safe mode threshold (i.e., don't populate queues before
        // leaving safe mode)
        this.replQueueThreshold = conf.getFloat(DFS_NAMENODE_REPL_QUEUE_THRESHOLD_PCT_KEY, threshold);
        this.extension = conf.getTimeDuration(DFS_NAMENODE_SAFEMODE_EXTENSION_KEY, DFS_NAMENODE_SAFEMODE_EXTENSION_DEFAULT,
                MILLISECONDS
        );

        this.inRollBack = isInRollBackMode(NameNode.getStartupOption(conf));

        LOG.info("{} = {}", DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, threshold);
        LOG.info("{} = {}", DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY, datanodeThreshold);
        LOG.info("{} = {}", DFS_NAMENODE_SAFEMODE_EXTENSION_KEY, extension);
    }

    /**
     * Initialize the safe mode information.
     * @param total initial total blocks
     *              start  activate  begin  setup
     */
    void activate(long total) {
        assert namesystem.hasWriteLock();
        assert status == BMSafeModeStatus.OFF;

        startTime = monotonicNow();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 初始化得到总数据块 和 阈值数据块
         *  举个例子： 关于 ZK 的 配置文件解析那个地方，写的非常不好！ 如果有100个配置， if else 就有 100 个分支
         *  原来甚至连 myid 的解析，就是夹在在  zoo.cfg 的解析代码中。 setupMyID()
         */
        setBlockTotal(total);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 检查是否满足阈值要求
         *  刚启动的时候，也会执行一次判断，如果满足条件，则直接退出安全模式
         *  将来 namenode 接收到 datanode 的块汇报的时候，也都会调用 areThresholdsMet() 来执行判断
         */
        if (areThresholdsMet()) {

            // TODO_MA 马中华 注释： 如果满足阈值要求，则退出安全模式
            boolean exitResult = leaveSafeMode(false);
            Preconditions.checkState(exitResult, "Failed to leave safe mode.");
        }
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 进入安全模式，然后通过异步的方式，去计算 数据块 的统计信息
         */
        else {
            // enter safe mode
            // TODO_MA 马中华 注释： 进入安全模式
            status = BMSafeModeStatus.PENDING_THRESHOLD;
            initializeReplQueuesIfNecessary();
            reportStatus("STATE* Safe mode ON.", true);
            lastStatusReport = monotonicNow();
        }
    }

    /**
     * @return true if it stays in start up safe mode else false.
     */
    boolean isInSafeMode() {
        if (status != BMSafeModeStatus.OFF) {
            doConsistencyCheck();
            return true;
        } else {
            return false;
        }
    }

    /**
     * The transition of the safe mode state machine.
     * If safe mode is not currently on, this is a no-op.
     */
    void checkSafeMode() {
        assert namesystem.hasWriteLock();
        if (namesystem.inTransitionToActive()) {
            return;
        }

        switch (status) {

            // TODO_MA 马中华 注释： PENDING_THRESHOLD 的处理
            case PENDING_THRESHOLD:

                if (areThresholdsMet()) {
                    // TODO_MA 马中华 注释： PENDING_THRESHOLD -> EXTENSION
                    if (blockTotal > 0 && extension > 0) {
                        // PENDING_THRESHOLD -> EXTENSION
                        status = BMSafeModeStatus.EXTENSION;
                        reachedTime.set(monotonicNow());
                        /*************************************************
                         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                         *  注释： 监控线程启动
                         */
                        smmthread.start();
                        initializeReplQueuesIfNecessary();
                        reportStatus("STATE* Safe mode extension entered.", true);
                    }
                    // TODO_MA 马中华 注释： PENDING_THRESHOLD -> OFF
                    else {
                        // PENDING_THRESHOLD -> OFF
                        leaveSafeMode(false);
                    }
                } else {
                    initializeReplQueuesIfNecessary();
                    reportStatus("STATE* Safe mode ON.", false);
                }
                break;
            case EXTENSION:
                reportStatus("STATE* Safe mode ON.", false);
                break;
            case OFF:
                break;
            default:
                assert false : "Non-recognized block manager safe mode status: " + status;
        }
    }

    /**
     * Adjust the total number of blocks safe and expected during safe mode.
     * If safe mode is not currently on, this is a no-op.
     * @param deltaSafe  the change in number of safe blocks
     * @param deltaTotal the change in number of total blocks expected
     */
    void adjustBlockTotals(int deltaSafe, int deltaTotal) {
        assert namesystem.hasWriteLock();
        if (!isSafeModeTrackingBlocks()) {
            return;
        }

        long newBlockTotal;
        synchronized (this) {
            LOG.debug("Adjusting block totals from {}/{} to {}/{}", blockSafe, blockTotal, blockSafe + deltaSafe,
                    blockTotal + deltaTotal
            );
            assert blockSafe + deltaSafe >= 0 : "Can't reduce blockSafe " + blockSafe + " by " + deltaSafe + ": would be negative";
            assert blockTotal + deltaTotal >= 0 : "Can't reduce blockTotal " + blockTotal + " by " + deltaTotal + ": would be negative";

            blockSafe += deltaSafe;
            newBlockTotal = blockTotal + deltaTotal;
        }
        setBlockTotal(newBlockTotal);
        checkSafeMode();
    }

    /**
     * Should we track blocks in safe mode.
     * <p/>
     * Never track blocks incrementally in non-HA code.
     * <p/>
     * In the HA case, the StandbyNode can be in safemode while the namespace
     * is modified by the edit log tailer. In this case, the number of total
     * blocks changes as edits are processed (eg blocks are added and deleted).
     * However, we don't want to do the incremental tracking during the
     * startup-time loading process -- only once the initial total has been
     * set after the image has been loaded.
     */
    boolean isSafeModeTrackingBlocks() {
        assert namesystem.hasWriteLock();
        return haEnabled && status != BMSafeModeStatus.OFF;
    }

    /**
     * Set total number of blocks.
     */
    void setBlockTotal(long total) {

        assert namesystem.hasWriteLock();

        // TODO_MA 马中华 注释： 保存 blockTotal
        synchronized (this) {
            this.blockTotal = total;
            // TODO_MA 马中华 注释： 数据块阈值： dfs.namenode.safemode.threshold-pct = 0.999f
            this.blockThreshold = (long) (total * threshold);
        }
        // TODO_MA 马中华 注释： replQueueThreshold 默认和 threshold 一样
        this.blockReplQueueThreshold = (long) (total * replQueueThreshold);
    }

    String getSafeModeTip() {
        String msg = "";

        synchronized (this) {
            if (blockSafe < blockThreshold) {
                msg += String.format(
                        "The reported blocks %d needs additional %d" + " blocks to reach the threshold %.4f of total blocks %d.%n",
                        blockSafe, (blockThreshold - blockSafe), threshold, blockTotal
                );
            } else {
                msg += String.format("The reported blocks %d has reached the threshold" + " %.4f of total blocks %d. ",
                        blockSafe, threshold, blockTotal
                );
            }
        }

        if (datanodeThreshold > 0) {
            int numLive = blockManager.getDatanodeManager().getNumLiveDataNodes();
            if (numLive < datanodeThreshold) {
                msg += String.format(
                        "The number of live datanodes %d needs an additional %d live " + "datanodes to reach the minimum number %d.%n",
                        numLive, (datanodeThreshold - numLive), datanodeThreshold
                );
            } else {
                msg += String.format("The number of live datanodes %d has reached " + "the minimum number %d. ", numLive,
                        datanodeThreshold
                );
            }
        } else {
            msg += "The minimum number of live datanodes is not required. ";
        }

        if (getBytesInFuture() > 0) {
            msg += "Name node detected blocks with generation stamps " + "in future. This means that Name node metadata is inconsistent. " + "This can happen if Name node metadata files have been manually " + "replaced. Exiting safe mode will cause loss of " + getBytesInFuture() + " byte(s). Please restart name node with " + "right metadata or use \"hdfs dfsadmin -safemode forceExit\" " + "if you are certain that the NameNode was started with the " + "correct FsImage and edit logs. If you encountered this during " + "a rollback, it is safe to exit with -safemode forceExit.";
            return msg;
        }

        final String turnOffTip = "Safe mode will be turned off automatically ";
        switch (status) {
            case PENDING_THRESHOLD:
                msg += turnOffTip + "once the thresholds have been reached.";
                break;
            case EXTENSION:
                msg += "In safe mode extension. " + turnOffTip + "in " + timeToLeaveExtension() / 1000 + " seconds.";
                break;
            case OFF:
                msg += turnOffTip + "soon.";
                break;
            default:
                assert false : "Non-recognized block manager safe mode status: " + status;
        }
        return msg;
    }

    /**
     * Leave start up safe mode.
     *
     * @param force - true to force exit
     * @return true if it leaves safe mode successfully else false
     */
    boolean leaveSafeMode(boolean force) {
        assert namesystem.hasWriteLock() : "Leaving safe mode needs write lock!";

        final long bytesInFuture = getBytesInFuture();
        if (bytesInFuture > 0) {
            if (force) {
                LOG.warn("Leaving safe mode due to forceExit. This will cause a data " + "loss of {} byte(s).",
                        bytesInFuture
                );
                bytesInFutureBlocks.reset();
                bytesInFutureECBlockGroups.reset();
            } else {
                LOG.error(
                        "Refusing to leave safe mode without a force flag. " + "Exiting safe mode will cause a deletion of {} byte(s). Please " + "use -forceExit flag to exit safe mode forcefully if data loss is" + " acceptable.",
                        bytesInFuture
                );
                return false;
            }
        } else if (force) {
            LOG.warn("forceExit used when normal exist would suffice. Treating " + "force exit as normal safe mode exit.");
        }

        // if not done yet, initialize replication queues.
        // In the standby, do not populate repl queues
        if (!blockManager.isPopulatingReplQueues() && blockManager.shouldPopulateReplQueues()) {
            blockManager.initializeReplQueues();
        }

        if (status != BMSafeModeStatus.OFF) {
            NameNode.stateChangeLog.info("STATE* Safe mode is OFF");
        }

        // TODO_MA 马中华 注释：
        status = BMSafeModeStatus.OFF;

        final long timeInSafemode = monotonicNow() - startTime;

        // TODO_MA 马中华 注释： 安全模式的统计结果
        NameNode.stateChangeLog.info("STATE* Leaving safe mode after {} secs", timeInSafemode / 1000);
        NameNode.getNameNodeMetrics().setSafeModeTime(timeInSafemode);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        final NetworkTopology nt = blockManager.getDatanodeManager().getNetworkTopology();
        NameNode.stateChangeLog.info("STATE* Network topology has {} racks and {}" + " datanodes", nt.getNumOfRacks(),
                nt.getNumOfLeaves()
        );
        NameNode.stateChangeLog.info("STATE* UnderReplicatedBlocks has {} blocks",
                blockManager.numOfUnderReplicatedBlocks()
        );

        namesystem.startSecretManagerIfNecessary();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        // If startup has not yet completed, end safemode phase.
        StartupProgress prog = NameNode.getStartupProgress();
        if (prog.getStatus(Phase.SAFEMODE) != Status.COMPLETE) {
            prog.endStep(Phase.SAFEMODE, BlockManagerSafeMode.STEP_AWAITING_REPORTED_BLOCKS);
            prog.endPhase(Phase.SAFEMODE);
        }

        return true;
    }

    /**
     * Increment number of safe blocks if the current block is contiguous
     * and it has reached minimal replication or
     * if the current block is striped and the number of its actual data blocks
     * reaches the number of data units specified by the erasure coding policy.
     * If safe mode is not currently on, this is a no-op.
     * @param storageNum  current number of replicas or number of internal blocks
     *                    of a striped block group
     * @param storedBlock current storedBlock which is either a
     *                    BlockInfoContiguous or a BlockInfoStriped
     */
    synchronized void incrementSafeBlockCount(int storageNum, BlockInfo storedBlock) {
        assert namesystem.hasWriteLock();
        if (status == BMSafeModeStatus.OFF) {
            return;
        }

        final int safeNumberOfNodes = storedBlock.isStriped() ? ((BlockInfoStriped) storedBlock).getRealDataBlockNum() : safeReplication;
        if (storageNum == safeNumberOfNodes) {
            this.blockSafe++;

            // Report startup progress only if we haven't completed startup yet.
            StartupProgress prog = NameNode.getStartupProgress();
            if (prog.getStatus(Phase.SAFEMODE) != Status.COMPLETE) {
                if (this.awaitingReportedBlocksCounter == null) {
                    this.awaitingReportedBlocksCounter = prog.getCounter(Phase.SAFEMODE, STEP_AWAITING_REPORTED_BLOCKS);
                }
                this.awaitingReportedBlocksCounter.increment();
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            checkSafeMode();
        }
    }

    /**
     * Decrement number of safe blocks if the current block is contiguous
     * and it has just fallen below minimal replication or
     * if the current block is striped and its actual data blocks has just fallen
     * below the number of data units specified by erasure coding policy.
     * If safe mode is not currently on, this is a no-op.
     */
    synchronized void decrementSafeBlockCount(BlockInfo b) {
        assert namesystem.hasWriteLock();
        if (status == BMSafeModeStatus.OFF) {
            return;
        }

        final int safeNumberOfNodes = b.isStriped() ? ((BlockInfoStriped) b).getRealDataBlockNum() : safeReplication;
        BlockInfo storedBlock = blockManager.getStoredBlock(b);
        if (storedBlock.isComplete() && blockManager.countNodes(b).liveReplicas() == safeNumberOfNodes - 1) {
            this.blockSafe--;
            assert blockSafe >= 0;
            checkSafeMode();
        }
    }

    /**
     * Check if the block report replica has a generation stamp (GS) in future.
     * If safe mode is not currently on, this is a no-op.
     *
     * @param brr block report replica which belongs to no file in BlockManager
     */
    void checkBlocksWithFutureGS(BlockReportReplica brr) {
        assert namesystem.hasWriteLock();
        if (status == BMSafeModeStatus.OFF) {
            return;
        }
        if (!blockManager.getShouldPostponeBlocksFromFuture() && !inRollBack && blockManager.isGenStampInFuture(brr)) {
            if (blockManager.getBlockIdManager().isStripedBlock(brr)) {
                bytesInFutureECBlockGroups.add(brr.getBytesOnDisk());
            } else {
                bytesInFutureBlocks.add(brr.getBytesOnDisk());
            }
        }
    }

    /**
     * Returns the number of bytes that reside in blocks with Generation Stamps
     * greater than generation stamp known to Namenode.
     *
     * @return Bytes in future
     */
    long getBytesInFuture() {
        return getBytesInFutureBlocks() + getBytesInFutureECBlockGroups();
    }

    long getBytesInFutureBlocks() {
        return bytesInFutureBlocks.longValue();
    }

    long getBytesInFutureECBlockGroups() {
        return bytesInFutureECBlockGroups.longValue();
    }

    void close() {
        assert namesystem.hasWriteLock() : "Closing bmSafeMode needs write lock!";
        try {
            smmthread.interrupt();
            smmthread.join(3000);
        } catch (InterruptedException ignored) {
        }
    }

    /**
     * Get time (counting in milliseconds) left to leave extension period.
     * It should leave safemode at once if blockTotal = 0 rather than wait
     * extension time (30s by default).
     *
     * Negative value indicates the extension period has passed.
     */
    private long timeToLeaveExtension() {
        return blockTotal > 0 ? reachedTime.get() + extension - monotonicNow() : 0;
    }

    /**
     * Returns true if Namenode was started with a RollBack option.
     *
     * @param option - StartupOption
     * @return boolean
     */
    private static boolean isInRollBackMode(StartupOption option) {
        return (option == StartupOption.ROLLBACK) || (option == StartupOption.ROLLINGUPGRADE && option.getRollingUpgradeStartupOption() == RollingUpgradeStartupOption.ROLLBACK);
    }

    /** Check if we are ready to initialize replication queues. */
    private void initializeReplQueuesIfNecessary() {
        assert namesystem.hasWriteLock();

        // Whether it has reached the threshold for initializing replication queues.
        boolean canInitializeReplQueues = blockManager.shouldPopulateReplQueues() && blockSafe >= blockReplQueueThreshold;

        if (canInitializeReplQueues && !blockManager.isPopulatingReplQueues() && !haEnabled) {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            blockManager.initializeReplQueues();
        }
    }

    /**
     * @return true if both block and datanode threshold are met else false.
     */
    private boolean areThresholdsMet() {
        assert namesystem.hasWriteLock();
        // Calculating the number of live datanodes is time-consuming
        // in large clusters. Skip it when datanodeThreshold is zero.
        // We need to evaluate getNumLiveDataNodes only when
        // (blockSafe >= blockThreshold) is true and hence moving evaluation
        // of datanodeNum conditional to isBlockThresholdMet as well
        synchronized (this) {
            // TODO_MA 马中华 注释： 检查数据块满足要求
            boolean isBlockThresholdMet = (blockSafe >= blockThreshold);
            boolean isDatanodeThresholdMet = true;

            // TODO_MA 马中华 注释： 检查 datanode 满足要求
            // TODO_MA 马中华 注释： dfs.namenode.safemode.min.datanodes = 0
            if (isBlockThresholdMet && datanodeThreshold > 0) {
                int datanodeNum = blockManager.getDatanodeManager().getNumLiveDataNodes();
                isDatanodeThresholdMet = (datanodeNum >= datanodeThreshold);
            }

            // TODO_MA 马中华 注释： 数据块 和 datanode 都满足数量了。
            return isBlockThresholdMet && isDatanodeThresholdMet;
        }
    }

    /**
     * Checks consistency of the class state.
     * This is costly so only runs if asserts are enabled.
     */
    private void doConsistencyCheck() {
        boolean assertsOn = false;
        assert assertsOn = true; // set to true if asserts are on
        if (!assertsOn) {
            return;
        }

        int activeBlocks = blockManager.getActiveBlockCount();
        synchronized (this) {
            if (blockTotal != activeBlocks && !(blockSafe >= 0 && blockSafe <= blockTotal)) {
                LOG.warn(
                        "SafeMode is in inconsistent filesystem state. " + "BlockManagerSafeMode data: blockTotal={}, blockSafe={}; " + "BlockManager data: activeBlocks={}",
                        blockTotal, blockSafe, activeBlocks
                );
            }
        }
    }

    /**
     * Print status every 20 seconds.
     */
    private void reportStatus(String msg, boolean rightNow) {
        assert namesystem.hasWriteLock();
        long curTime = monotonicNow();
        if (!rightNow && (curTime - lastStatusReport < 20 * 1000)) {
            return;
        }
        NameNode.stateChangeLog.info(msg + " \n" + getSafeModeTip());
        lastStatusReport = curTime;
    }

    /**
     * Periodically check whether it is time to leave safe mode.
     * This thread starts when the threshold level is reached.
     */
    private class SafeModeMonitor implements Runnable {
        /** Interval in msec for checking safe mode. */
        private static final long RECHECK_INTERVAL = 1000;

        @Override
        public void run() {
            while (namesystem.isRunning()) {
                try {
                    namesystem.writeLock();
                    if (status == BMSafeModeStatus.OFF) { // Not in safe mode.
                        break;
                    }
                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 判断是否可以退出
                     */
                    if (canLeave()) {
                        // EXTENSION -> OFF
                        /*************************************************
                         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                         *  注释： 退出安全模式
                         */
                        leaveSafeMode(false);
                        break;
                    }
                } finally {
                    namesystem.writeUnlock();
                }

                // TODO_MA 马中华 注释： 每隔 1s 钟，执行一次检查，是否可以退出安全模式
                try {
                    Thread.sleep(RECHECK_INTERVAL);
                } catch (InterruptedException ignored) {
                }
            }

            if (!namesystem.isRunning()) {
                LOG.info("NameNode is being shutdown, exit SafeModeMonitor thread");
            }
        }

        /**
         * // TODO_MA 马中华 注释： 检查此显示器是否可以关闭安全模式。
         * Check whether the safe mode can be turned off by this monitor.
         *
         * // TODO_MA 马中华 注释： 如果达到阈值并且延长时间已过，则可以关闭安全模式。
         * Safe mode can be turned off iff the threshold is reached, and the extension time has passed.
         */
        private boolean canLeave() {
            if (namesystem.inTransitionToActive()) {
                return false;
            } else if (timeToLeaveExtension() > 0) {
                reportStatus("STATE* Safe mode ON, in safe mode extension.", false);
                return false;
            } else if (!areThresholdsMet()) {
                reportStatus("STATE* Safe mode ON, thresholds not met.", false);
                return false;
            } else {
                return true;
            }
        }
    }

}
