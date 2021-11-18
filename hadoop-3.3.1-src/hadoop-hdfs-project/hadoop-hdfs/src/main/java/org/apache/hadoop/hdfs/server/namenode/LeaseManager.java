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
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedListEntries;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.OpenFileEntry;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.util.Daemon;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * // TODO_MA 马中华 注释： 租约是 Namenode 给予租约持有者（LeaseHolder， 一般是客户端） 在规定时间内拥有文件权限（写文件） 的合同
 * // TODO_MA 马中华 注释： 租约管理器保存了HDFS文件与租约、 租约与租约持有者的对应关系
 * // TODO_MA 马中华 注释： HDFS文件是write-once-read-many， 并且不支持客户端的并行写操作。 HDFS提供了租约（Lease） 机制保证对HDFS文件的互斥操作来实现这个功能
 * // TODO_MA 马中华 注释： 租约管理器还会定期检查它维护的所有租约是否过期
 * // TODO_MA 马中华 注释： 租约管理器会强制收回过期的租约， 所以租约持有者需要定期更新租约(renew)， 维护对该文件的独占锁定。
 * // TODO_MA 马中华 注释： 当客户端完成了对文件的写操作， 关闭文件时， 必须在租约管理器中释放租
 * LeaseManager does the lease housekeeping for writing on files.   
 * This class also provides useful static methods for lease recovery.
 *
 * Lease Recovery Algorithm
 * 1) Namenode retrieves lease information
 * 2) For each file f in the lease, consider the last block b of f
 * 2.1) Get the datanodes which contains b
 * 2.2) Assign one of the datanodes as the primary datanode p
 * 2.3) p obtains a new generation stamp from the namenode
 * 2.4) p gets the block info from each datanode
 * 2.5) p computes the minimum block length
 * 2.6) p updates the datanodes, which have a valid generation stamp, with the new generation stamp and the minimum block length
 * 2.7) p acknowledges the namenode the update results
 * 2.8) Namenode updates the BlockInfo
 * 2.9) Namenode removes f from the lease and removes the lease once all files have been removed
 * 2.10) Namenode commit changes to edit log
 */

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： LeaseManager是Namenode中维护所有租约操作的类，它不仅仅保存了HDFS中所有租约的信息，
 *  提供租约的增、 删、 改、 查方法， 同时还维护了一个Monitor线程定期检查租约是否超时，
 *  对于长时间没有更新租约的文件（超过硬限制时间）， LeaseManager会触发约恢复机制， 然后关闭文件。
 */
@InterfaceAudience.Private
public class LeaseManager {
    public static final Logger LOG = LoggerFactory.getLogger(LeaseManager.class.getName());
    private final FSNamesystem fsnamesystem;

    // TODO_MA 马中华 注释： 使用softLimit字段保存软限制时间（默认是60秒， 不可以配置）
    private long softLimit = HdfsConstants.LEASE_SOFTLIMIT_PERIOD;

    // TODO_MA 马中华 注释： 使用hadrLimit字段保存硬限制时间（默认是60分钟， 不可以配置） 。
    private long hardLimit;

    static final int INODE_FILTER_WORKER_COUNT_MAX = 4;
    static final int INODE_FILTER_WORKER_TASK_MIN = 512;
    private long lastHolderUpdateTime;
    private String internalLeaseHolder;

    // Used for handling lock-leases
    // Mapping: leaseHolder -> Lease
    // TODO_MA 马中华 注释： 保存了 租约持有者 与 租约 的对应关系。
    private final HashMap<String, Lease> leases = new HashMap<>();

    // INodeID -> Lease
    // TODO_MA 马中华 注释： 保存了 INodeID -> Lease 的对应关系
    private final TreeMap<Long, Lease> leasesById = new TreeMap<>();

    // TODO_MA 马中华 注释： 租约检查线程
    private Daemon lmthread;

    private volatile boolean shouldRunMonitor;

    LeaseManager(FSNamesystem fsnamesystem) {
        Configuration conf = new Configuration();
        this.fsnamesystem = fsnamesystem;
        this.hardLimit = conf.getLong(DFSConfigKeys.DFS_LEASE_HARDLIMIT_KEY, DFSConfigKeys.DFS_LEASE_HARDLIMIT_DEFAULT) * 1000;
        updateInternalLeaseHolder();
    }

    // Update the internal lease holder with the current time stamp.
    private void updateInternalLeaseHolder() {
        this.lastHolderUpdateTime = Time.monotonicNow();
        this.internalLeaseHolder = HdfsServerConstants.NAMENODE_LEASE_HOLDER + "-" + Time.formatTime(Time.now());
    }

    // Get the current internal lease holder name.
    String getInternalLeaseHolder() {
        long elapsed = Time.monotonicNow() - lastHolderUpdateTime;
        if (elapsed > hardLimit) {
            updateInternalLeaseHolder();
        }
        return internalLeaseHolder;
    }

    Lease getLease(String holder) {
        return leases.get(holder);
    }

    /**
     * This method iterates through all the leases and counts the number of blocks
     * which are not COMPLETE. The FSNamesystem read lock MUST be held before
     * calling this method.
     */
    synchronized long getNumUnderConstructionBlocks() {
        assert this.fsnamesystem.hasReadLock() : "The FSNamesystem read lock wasn't" + "acquired before counting under construction blocks";
        long numUCBlocks = 0;
        for (Long id : getINodeIdWithLeases()) {
            INode inode = fsnamesystem.getFSDirectory().getInode(id);
            if (inode == null) {
                // The inode could have been deleted after getINodeIdWithLeases() is
                // called, check here, and ignore it if so
                LOG.warn("Failed to find inode {} in getNumUnderConstructionBlocks().", id);
                continue;
            }
            final INodeFile cons = inode.asFile();
            if (!cons.isUnderConstruction()) {
                LOG.warn("The file {} is not under construction but has lease.", cons.getFullPathName());
                continue;
            }
            BlockInfo[] blocks = cons.getBlocks();
            if (blocks == null) {
                continue;
            }
            for (BlockInfo b : blocks) {
                if (!b.isComplete()) {
                    numUCBlocks++;
                }
            }
        }
        LOG.info("Number of blocks under construction: {}", numUCBlocks);
        return numUCBlocks;
    }

    Collection<Long> getINodeIdWithLeases() {
        return leasesById.keySet();
    }

    /**
     * Get {@link INodesInPath} for all {@link INode} in the system
     * which has a valid lease.
     *
     * @return Set<INodesInPath>
     */
    @VisibleForTesting
    Set<INodesInPath> getINodeWithLeases() throws IOException {
        return getINodeWithLeases(null);
    }

    private synchronized INode[] getINodesWithLease() {
        List<INode> inodes = new ArrayList<>(leasesById.size());
        INode currentINode;
        for (long inodeId : leasesById.keySet()) {
            currentINode = fsnamesystem.getFSDirectory().getInode(inodeId);
            // A file with an active lease could get deleted, or its
            // parent directories could get recursively deleted.
            if (currentINode != null && currentINode.isFile() && !fsnamesystem.isFileDeleted(currentINode.asFile())) {
                inodes.add(currentINode);
            }
        }
        return inodes.toArray(new INode[0]);
    }

    /**
     * Get {@link INodesInPath} for all files under the ancestor directory which
     * has valid lease. If the ancestor directory is null, then return all files
     * in the system with valid lease. Callers must hold {@link FSNamesystem}
     * read or write lock.
     *
     * @param ancestorDir the ancestor {@link INodeDirectory}
     * @return {@code Set<INodesInPath>}
     */
    public Set<INodesInPath> getINodeWithLeases(final INodeDirectory ancestorDir) throws IOException {
        assert fsnamesystem.hasReadLock();
        final long startTimeMs = Time.monotonicNow();
        Set<INodesInPath> iipSet = new HashSet<>();
        final INode[] inodes = getINodesWithLease();
        int inodeCount = inodes.length;
        if (inodeCount == 0) {
            return iipSet;
        }

        List<Future<List<INodesInPath>>> futureList = Lists.newArrayList();
        final int workerCount = Math.min(INODE_FILTER_WORKER_COUNT_MAX, (((inodeCount - 1) / INODE_FILTER_WORKER_TASK_MIN) + 1));
        ExecutorService inodeFilterService = Executors.newFixedThreadPool(workerCount);
        for (int workerIdx = 0; workerIdx < workerCount; workerIdx++) {
            final int startIdx = workerIdx;
            Callable<List<INodesInPath>> c = new Callable<List<INodesInPath>>() {
                @Override
                public List<INodesInPath> call() {
                    List<INodesInPath> iNodesInPaths = Lists.newArrayList();
                    for (int idx = startIdx; idx < inodeCount; idx += workerCount) {
                        INode inode = inodes[idx];
                        if (!inode.isFile()) {
                            continue;
                        }
                        INodesInPath inodesInPath = INodesInPath.fromINode(fsnamesystem.getFSDirectory().getRoot(), inode.asFile());
                        if (ancestorDir != null && !inodesInPath.isDescendant(ancestorDir)) {
                            continue;
                        }
                        iNodesInPaths.add(inodesInPath);
                    }
                    return iNodesInPaths;
                }
            };

            // Submit the inode filter task to the Executor Service
            futureList.add(inodeFilterService.submit(c));
        }
        inodeFilterService.shutdown();

        for (Future<List<INodesInPath>> f : futureList) {
            try {
                iipSet.addAll(f.get());
            } catch (Exception e) {
                throw new IOException("Failed to get files with active leases", e);
            }
        }
        final long endTimeMs = Time.monotonicNow();
        if ((endTimeMs - startTimeMs) > 1000) {
            LOG.info("Took {} ms to collect {} open files with leases {}", (endTimeMs - startTimeMs), iipSet.size(),
                    ((ancestorDir != null) ? " under " + ancestorDir.getFullPathName() : ".")
            );
        }
        return iipSet;
    }

    public BatchedListEntries<OpenFileEntry> getUnderConstructionFiles(final long prevId) throws IOException {
        return getUnderConstructionFiles(prevId, OpenFilesIterator.FILTER_PATH_DEFAULT);
    }

    /**
     * Get a batch of under construction files from the currently active leases.
     * File INodeID is the cursor used to fetch new batch of results and the
     * batch size is configurable using below config param. Since the list is
     * fetched in batches, it does not represent a consistent view of all
     * open files.
     *
     * @see org.apache.hadoop.hdfs.DFSConfigKeys#DFS_NAMENODE_LIST_OPENFILES_NUM_RESPONSES
     * @param prevId the INodeID cursor
     * @throws IOException
     */
    public BatchedListEntries<OpenFileEntry> getUnderConstructionFiles(final long prevId, final String path) throws IOException {
        assert fsnamesystem.hasReadLock();
        SortedMap<Long, Lease> remainingLeases;
        synchronized (this) {
            remainingLeases = leasesById.tailMap(prevId, false);
        }
        Collection<Long> inodeIds = remainingLeases.keySet();
        final int numResponses = Math.min(this.fsnamesystem.getMaxListOpenFilesResponses(), inodeIds.size());
        final List<OpenFileEntry> openFileEntries = Lists.newArrayListWithExpectedSize(numResponses);

        int count = 0;
        String fullPathName = null;
        Iterator<Long> inodeIdIterator = inodeIds.iterator();
        while (inodeIdIterator.hasNext()) {
            Long inodeId = inodeIdIterator.next();
            final INodeFile inodeFile = fsnamesystem.getFSDirectory().getInode(inodeId).asFile();
            if (!inodeFile.isUnderConstruction()) {
                LOG.warn("The file {} is not under construction but has lease.", inodeFile.getFullPathName());
                continue;
            }

            fullPathName = inodeFile.getFullPathName();
            if (StringUtils.isEmpty(path) || DFSUtil.isParentEntry(fullPathName, path)) {
                openFileEntries.add(new OpenFileEntry(inodeFile.getId(), fullPathName,
                        inodeFile.getFileUnderConstructionFeature().getClientName(),
                        inodeFile.getFileUnderConstructionFeature().getClientMachine()
                ));
                count++;
            }

            if (count >= numResponses) {
                break;
            }
        }
        // avoid rescanning all leases when we have checked all leases already
        boolean hasMore = inodeIdIterator.hasNext();
        return new BatchedListEntries<>(openFileEntries, hasMore);
    }

    /** @return the lease containing src */
    public synchronized Lease getLease(INodeFile src) {
        return leasesById.get(src.getId());
    }

    /** @return the number of leases currently in the system */
    @VisibleForTesting
    public synchronized int countLease() {
        return leases.size();
    }

    /** @return the number of paths contained in all leases */
    synchronized long countPath() {
        return leasesById.size();
    }

    /**
     * Adds (or re-adds) the lease for the specified file.
     * // TODO_MA 马中华 注释： 添加租约
     * // TODO_MA 马中华 注释： 当客户端创建文件和追加写文件时， FSNamesystem.startFileIntemal() 以及 appendFilelnternal()
     * // TODO_MA 马中华 注释： 方法都会调用 LeaseManager.addLease() 为该客户端在 HDFS 文件上添加一个租约
     */
    synchronized Lease addLease(String holder, long inodeId) {

        // TODO_MA 马中华 注释： 根据 client 的名字获取 Lease 信息
        Lease lease = getLease(holder);
        if (lease == null) {

            // TODO_MA 马中华 注释： 构造 Lease 对象
            lease = new Lease(holder);

            // TODO_MA 马中华 注释： 在 LeaseManager.leases 字段中添加 Lease 对象
            leases.put(holder, lease);
        } else {

            // TODO_MA 马中华 注释： 如果该租约存在，更新租约时间
            renewLease(lease);
        }

        // TODO_MA 马中华 注释： 保存 inodeId 信息
        leasesById.put(inodeId, lease);

        // TODO_MA 马中华 注释： 保存租约中的文件信息
        lease.files.add(inodeId);
        return lease;
    }

    synchronized void removeLease(long inodeId) {
        final Lease lease = leasesById.get(inodeId);
        if (lease != null) {
            removeLease(lease, inodeId);
        }
    }

    /**
     * Remove the specified lease and src.
     */
    private synchronized void removeLease(Lease lease, long inodeId) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 移除
         */
        leasesById.remove(inodeId);
        if (!lease.removeFile(inodeId)) {
            LOG.debug("inode {} not found in lease.files (={})", inodeId, lease);
        }

        if (!lease.hasFiles()) {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 移除
             */
            if (leases.remove(lease.holder) == null) {
                LOG.error("{} not found", lease);
            }
        }
    }

    /**
     * Remove the lease for the specified holder and src
     */
    synchronized void removeLease(String holder, INodeFile src) {
        Lease lease = getLease(holder);
        if (lease != null) {
            removeLease(lease, src.getId());
        } else {
            LOG.warn("Removing non-existent lease! holder={} src={}", holder, src.getFullPathName());
        }
    }

    synchronized void removeAllLeases() {
        leasesById.clear();
        leases.clear();
    }

    /**
     * Reassign lease for file src to the new holder.
     */
    synchronized Lease reassignLease(Lease lease, INodeFile src, String newHolder) {
        assert newHolder != null : "new lease holder is null";
        if (lease != null) {
            removeLease(lease, src.getId());
        }
        return addLease(newHolder, src.getId());
    }

    /**
     * Renew the lease(s) held by the given client
     */
    synchronized void renewLease(String holder) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        renewLease(getLease(holder));
    }

    synchronized void renewLease(Lease lease) {
        if (lease != null) {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            lease.renew();
        }
    }

    /**
     * Renew all of the currently open leases.
     */
    synchronized void renewAllLeases() {
        for (Lease l : leases.values()) {
            renewLease(l);
        }
    }

    /************************************************************
     * A Lease governs all the locks held by a single client.
     * For each client there's a corresponding lease, whose
     * timestamp is updated when the client periodically
     * checks in.  If the client dies and allows its lease to
     * expire, all the corresponding locks can be released.
     *************************************************************/
    // TODO_MA 马中华 注释： 一个 HDFS 客户端是可以同时打开多个 HDFS 文件进行读写操作的，
    // TODO_MA 马中华 注释： 为了便于管理， 在租约管理器中将一个客户端打开的所有文件组织在一起构成一条记录
    class Lease {

        // TODO_MA 马中华 注释： 租约持有者
        private final String holder;
        // TODO_MA 马中华 注释： 租约的最后更新时间
        private long lastUpdate;
        // TODO_MA 马中华 注释： 该租约对应的客户端打开的所有文件
        private final HashSet<Long> files = new HashSet<>();

        /** Only LeaseManager object can create a lease */
        private Lease(String h) {
            this.holder = h;
            renew();
        }

        /** Only LeaseManager object can renew a lease */
        private void renew() {
            // TODO_MA 马中华 注释： 更新时间
            this.lastUpdate = monotonicNow();
        }

        /** @return true if the Hard Limit Timer has expired */
        public boolean expiredHardLimit() {
            return monotonicNow() - lastUpdate > hardLimit;
        }

        // TODO_MA 马中华 注释： 用于判断当前租约是否超出了硬限制（hardLimit），硬限制是用于考虑文件关闭异常时，强制回收租约的时间，默认是60分钟，不可以配置
        public boolean expiredHardLimit(long now) {
            return now - lastUpdate > hardLimit;
        }

        /** @return true if the Soft Limit Timer has expired */
        // TODO_MA 马中华 注释： 用于判断当前租约是否超出了软限制（softLimit），软限制是写文件规定的租约超时时间，默认是60秒，不可以配置
        public boolean expiredSoftLimit() {
            return monotonicNow() - lastUpdate > softLimit;
        }

        /** Does this lease contain any path? */
        boolean hasFiles() {
            return !files.isEmpty();
        }

        boolean removeFile(long inodeId) {
            return files.remove(inodeId);
        }

        @Override
        public String toString() {
            return "[Lease.  Holder: " + holder + ", pending creates: " + files.size() + "]";
        }

        @Override
        public int hashCode() {
            return holder.hashCode();
        }

        private Collection<Long> getFiles() {
            return Collections.unmodifiableCollection(files);
        }

        String getHolder() {
            return holder;
        }

        @VisibleForTesting
        long getLastUpdate() {
            return lastUpdate;
        }
    }

    public void setLeasePeriod(long softLimit, long hardLimit) {
        this.softLimit = softLimit;
        this.hardLimit = hardLimit;
    }

    private synchronized Collection<Lease> getExpiredCandidateLeases() {
        final long now = Time.monotonicNow();
        Collection<Lease> expired = new HashSet<>();
        for (Lease lease : leases.values()) {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 超过 60 分钟
             */
            if (lease.expiredHardLimit(now)) {
                expired.add(lease);
            }
        }
        return expired;
    }

    /******************************************************
     * Monitor checks for leases that have expired, and disposes of them.
     ******************************************************/
    // TODO_MA 马中华 注释： Monitor 内部类用于定期检查租约的更新情况， 当超过硬限制时间时， 会触发租约恢复机制。
    // TODO_MA 马中华 注释： 对于长时间没有进行租约更新的文件， LeaseManager会对这个文件进行租约恢复操作， 然后关闭这个文件
    class Monitor implements Runnable {
        final String name = getClass().getSimpleName();

        /** Check leases periodically. */
        @Override
        public void run() {
            for (; shouldRunMonitor && fsnamesystem.isRunning(); ) {
                boolean needSync = false;
                try {

                    // TODO_MA 马中华 注释： 每隔 2s
                    // sleep now to avoid infinite loop if an exception was thrown.
                    Thread.sleep(fsnamesystem.getLeaseRecheckIntervalMs());

                    // pre-filter the leases w/o the fsn lock.
                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释：
                     */
                    Collection<Lease> candidates = getExpiredCandidateLeases();
                    if (candidates.isEmpty()) {
                        continue;
                    }

                    fsnamesystem.writeLockInterruptibly();
                    try {

                        // TODO_MA 马中华 注释： fsnamesystem 是否是安全模式
                        if (!fsnamesystem.isInSafeMode()) {

                            /*************************************************
                             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                             *  注释： 检查租约信息
                             */
                            needSync = checkLeases(candidates);
                        }
                    } finally {
                        fsnamesystem.writeUnlock("leaseManager");
                        // lease reassignments should to be sync'ed.
                        if (needSync) {
                            fsnamesystem.getEditLog().logSync();
                        }
                    }
                } catch (InterruptedException ie) {
                    LOG.debug("{} is interrupted", name, ie);
                } catch (Throwable e) {
                    LOG.warn("Unexpected throwable: ", e);
                }
            }
        }
    }

    /** Check the leases beginning from the oldest.
     *  @return true is sync is needed.
     */
    @VisibleForTesting
    synchronized boolean checkLeases() {
        return checkLeases(getExpiredCandidateLeases());
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 检查租约
     */
    private synchronized boolean checkLeases(Collection<Lease> leasesToCheck) {
        boolean needSync = false;
        assert fsnamesystem.hasWriteLock();

        long start = monotonicNow();

        // TODO_MA 马中华 注释： 遍历每个租约
        for (Lease leaseToCheck : leasesToCheck) {
            if (isMaxLockHoldToReleaseLease(start)) {
                break;
            }
            if (!leaseToCheck.expiredHardLimit(Time.monotonicNow())) {
                continue;
            }
            LOG.info("{} has expired hard limit", leaseToCheck);
            final List<Long> removing = new ArrayList<>();
            // need to create a copy of the oldest lease files, because
            // internalReleaseLease() removes files corresponding to empty files,
            // i.e. it needs to modify the collection being iterated over
            // causing ConcurrentModificationException
            // TODO_MA 马中华 注释： 获取租约打开的文件
            Collection<Long> files = leaseToCheck.getFiles();
            Long[] leaseINodeIds = files.toArray(new Long[files.size()]);
            FSDirectory fsd = fsnamesystem.getFSDirectory();
            String p = null;
            String newHolder = getInternalLeaseHolder();

            // TODO_MA 马中华 注释： 遍历租约打开的每个文件
            for (Long id : leaseINodeIds) {
                try {
                    INodesInPath iip = INodesInPath.fromINode(fsd.getInode(id));
                    p = iip.getPath();
                    // Sanity check to make sure the path is correct
                    if (!p.startsWith("/")) {
                        throw new IOException("Invalid path in the lease " + p);
                    }
                    final INodeFile lastINode = iip.getLastINode().asFile();

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释：
                     */
                    if (fsnamesystem.isFileDeleted(lastINode)) {
                        // INode referred by the lease could have been deleted.
                        // TODO_MA 马中华 注释： 释放租约
                        removeLease(lastINode.getId());
                        continue;
                    }
                    boolean completed = false;

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 调用 Fsnamesystem.internalReleaseLease() 对文件进行租约释放
                     */
                    try {
                        completed = fsnamesystem.internalReleaseLease(leaseToCheck, p, iip, newHolder);
                    } catch (IOException e) {
                        LOG.warn("Cannot release the path {} in the lease {}. It will be " + "retried.", p, leaseToCheck, e);
                        continue;
                    }
                    if (LOG.isDebugEnabled()) {
                        if (completed) {
                            LOG.debug("Lease recovery for inode {} is complete. File closed" + ".", id);
                        } else {
                            LOG.debug("Started block recovery {} lease {}", p, leaseToCheck);
                        }
                    }
                    // TODO_MA 马中华 注释： 由于进行了恢复操作， 需要在editlog中同步记录
                    // If a lease recovery happened, we need to sync later.
                    if (!needSync && !completed) {
                        needSync = true;
                    }
                } catch (IOException e) {
                    LOG.warn("Removing lease with an invalid path: {},{}", p, leaseToCheck, e);
                    removing.add(id);
                }
                if (isMaxLockHoldToReleaseLease(start)) {
                    LOG.debug("Breaking out of checkLeases after {} ms.", fsnamesystem.getMaxLockHoldToReleaseLeaseMs());
                    break;
                }
            }

            // TODO_MA 马中华 注释： 租约恢复异常， 则直接删除
            for (Long id : removing) {
                removeLease(leaseToCheck, id);
            }
        }
        return needSync;
    }


    /** @return true if max lock hold is reached */
    private boolean isMaxLockHoldToReleaseLease(long start) {
        return monotonicNow() - start > fsnamesystem.getMaxLockHoldToReleaseLeaseMs();
    }

    @Override
    public synchronized String toString() {
        return getClass().getSimpleName() + "= {" + "\n leases=" + leases + "\n leasesById=" + leasesById + "\n}";
    }

    void startMonitor() {
        Preconditions.checkState(lmthread == null, "Lease Monitor already running");
        shouldRunMonitor = true;

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 启动租约检查线程
         */
        lmthread = new Daemon(new Monitor());
        lmthread.start();
    }

    void stopMonitor() {
        if (lmthread != null) {
            shouldRunMonitor = false;
            try {
                lmthread.interrupt();
                lmthread.join(3000);
            } catch (InterruptedException ie) {
                LOG.warn("Encountered exception ", ie);
            }
            lmthread = null;
        }
    }

    /**
     * Trigger the currently-running Lease monitor to re-check
     * its leases immediately. This is for use by unit tests.
     */
    @VisibleForTesting
    public void triggerMonitorCheckNow() {
        Preconditions.checkState(lmthread != null, "Lease monitor is not running");
        lmthread.interrupt();
    }

    @VisibleForTesting
    public void runLeaseChecks() {
        checkLeases();
    }

}
