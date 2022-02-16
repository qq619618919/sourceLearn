/*
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

package org.apache.zookeeper.server.persistence;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.DataTree.ProcessTxnResult;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerMetrics;
import org.apache.zookeeper.server.ServerStats;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.server.persistence.TxnLog.TxnIterator;
import org.apache.zookeeper.txn.CreateSessionTxn;
import org.apache.zookeeper.txn.TxnDigest;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a helper class
 * above the implementations
 * of txnlog and snapshot
 * classes
 * // TODO_MA 马中华 注释： ZKDatabase  zk 数据库
 * // TODO_MA 马中华 注释： ZKDatabase 维护了两个东西
 * // TODO_MA 马中华 注释： 1、FileTxnSnapLog  提供相关动作的实现
 * // TODO_MA 马中华 注释： 2、DataTree 内存中的所有数据
 */
public class FileTxnSnapLog {

    //the directory containing the
    //the transaction logs
    final File dataDir;
    //the directory containing the
    //the snapshot directory
    final File snapDir;

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 负责 日志记录相关 工作
     */
    TxnLog txnLog;

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 负责 内存数据 和 磁盘数据 的转化 工作
     */
    SnapShot snapLog;

    private final boolean autoCreateDB;
    private final boolean trustEmptySnapshot;
    public static final int VERSION = 2;
    public static final String version = "version-";

    private static final Logger LOG = LoggerFactory.getLogger(FileTxnSnapLog.class);

    public static final String ZOOKEEPER_DATADIR_AUTOCREATE = "zookeeper.datadir.autocreate";

    public static final String ZOOKEEPER_DATADIR_AUTOCREATE_DEFAULT = "true";

    static final String ZOOKEEPER_DB_AUTOCREATE = "zookeeper.db.autocreate";

    private static final String ZOOKEEPER_DB_AUTOCREATE_DEFAULT = "true";

    public static final String ZOOKEEPER_SNAPSHOT_TRUST_EMPTY = "zookeeper.snapshot.trust.empty";

    private static final String EMPTY_SNAPSHOT_WARNING = "No snapshot found, but there are log entries. ";

    /**
     * This listener helps
     * the external apis calling
     * restore to gather information
     * while the data is being
     * restored.
     */
    public interface PlayBackListener {

        void onTxnLoaded(TxnHeader hdr, Record rec, TxnDigest digest);

    }

    /**
     * Finalizing restore of data tree through
     * a set of operations (replaying transaction logs,
     * calculating data tree digests, and so on.).
     */
    private interface RestoreFinalizer {
        /**
         * @return the highest zxid of restored data tree.
         */
        long run() throws IOException;
    }

    /**
     * the constructor which takes the datadir and
     * snapdir.
     *
     * @param dataDir the transaction directory
     * @param snapDir the snapshot directory
     */
    public FileTxnSnapLog(File dataDir, File snapDir) throws IOException {
        LOG.debug("Opening datadir:{} snapDir:{}", dataDir, snapDir);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        this.dataDir = new File(dataDir, version + VERSION);
        this.snapDir = new File(snapDir, version + VERSION);

        // by default create snap/log dirs, but otherwise complain instead
        // See ZOOKEEPER-1161 for more details
        boolean enableAutocreate = Boolean
                .parseBoolean(System.getProperty(ZOOKEEPER_DATADIR_AUTOCREATE, ZOOKEEPER_DATADIR_AUTOCREATE_DEFAULT));

        trustEmptySnapshot = Boolean.getBoolean(ZOOKEEPER_SNAPSHOT_TRUST_EMPTY);
        LOG.info("{} : {}", ZOOKEEPER_SNAPSHOT_TRUST_EMPTY, trustEmptySnapshot);

        // TODO_MA 注释： 检查数据文件夹是否存在
        if(!this.dataDir.exists()) {
            if(!enableAutocreate) {
                throw new DatadirException(String.format(
                        "Missing data directory %s, automatic data directory creation is disabled (%s is false)." + " Please create this directory manually.",
                        this.dataDir, ZOOKEEPER_DATADIR_AUTOCREATE));
            }

            if(!this.dataDir.mkdirs() && !this.dataDir.exists()) {
                throw new DatadirException("Unable to create data directory " + this.dataDir);
            }
        }
        if(!this.dataDir.canWrite()) {
            throw new DatadirException("Cannot write to data directory " + this.dataDir);
        }

        // TODO_MA 注释： 检查快照文件夹是否存在
        if(!this.snapDir.exists()) {
            // by default create this directory, but otherwise complain instead
            // See ZOOKEEPER-1161 for more details
            if(!enableAutocreate) {
                throw new DatadirException(String.format(
                        "Missing snap directory %s, automatic data directory creation is disabled (%s is false)." + "Please create this directory manually.",
                        this.snapDir, ZOOKEEPER_DATADIR_AUTOCREATE));
            }

            if(!this.snapDir.mkdirs() && !this.snapDir.exists()) {
                throw new DatadirException("Unable to create snap directory " + this.snapDir);
            }
        }
        if(!this.snapDir.canWrite()) {
            throw new DatadirException("Cannot write to snap directory " + this.snapDir);
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        // check content of transaction log and snapshot dirs if they are two different directories
        // See ZOOKEEPER-2967 for more details
        if(!this.dataDir.getPath().equals(this.snapDir.getPath())) {
            checkLogDir();
            checkSnapDir();
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        txnLog = new FileTxnLog(this.dataDir);
        snapLog = new FileSnap(this.snapDir);

        autoCreateDB = Boolean.parseBoolean(System.getProperty(ZOOKEEPER_DB_AUTOCREATE, ZOOKEEPER_DB_AUTOCREATE_DEFAULT));
    }

    public void setServerStats(ServerStats serverStats) {
        txnLog.setServerStats(serverStats);
    }

    private void checkLogDir() throws LogDirContentCheckException {
        File[] files = this.dataDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return Util.isSnapshotFileName(name);
            }
        });
        if(files != null && files.length > 0) {
            throw new LogDirContentCheckException(
                    "Log directory has snapshot files. Check if dataLogDir and dataDir configuration is correct.");
        }
    }

    private void checkSnapDir() throws SnapDirContentCheckException {
        File[] files = this.snapDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return Util.isLogFileName(name);
            }
        });
        if(files != null && files.length > 0) {
            throw new SnapDirContentCheckException(
                    "Snapshot directory has log files. Check if dataLogDir and dataDir configuration is correct.");
        }
    }

    /**
     * get the datadir used by this filetxn
     * snap log
     *
     * @return the data dir
     */
    public File getDataDir() {
        return this.dataDir;
    }

    /**
     * get the snap dir used by this
     * filetxn snap log
     *
     * @return the snap dir
     */
    public File getSnapDir() {
        return this.snapDir;
    }

    /**
     * get information of the last saved/restored snapshot
     *
     * @return info of last snapshot
     */
    public SnapshotInfo getLastSnapshotInfo() {
        return this.snapLog.getLastSnapshotInfo();
    }

    /**
     * this function restores the server
     * database after reading from the
     * snapshots and transaction logs
     *
     * // TODO_MA 注释： 假设当前 zookeeper 最大 事务 id  = 100
     *
     * @param dt       the datatree to be restored
     * @param sessions the sessions to be restored
     * @param listener the playback listener to run on the
     *                 database restoration
     * @return the highest zxid restored
     * @throws IOException
     */
    public long restore(DataTree dt, Map<Long, Integer> sessions, PlayBackListener listener) throws IOException {
        long snapLoadingStartTime = Time.currentElapsedTime();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 从快照恢复绝大部分数据
         *  快照文件恢复： 1-95
         *  里面又包含恢复 session 信息 和 datatree 数据
         *  dt.lastProcessID = 95
         *  snapLog = SnapShot 快照功能
         */
        long deserializeResult = snapLog.deserialize(dt, sessions);

        ServerMetrics.getMetrics().STARTUP_SNAP_LOAD_TIME.add(Time.currentElapsedTime() - snapLoadingStartTime);
        FileTxnLog txnLog = new FileTxnLog(dataDir);
        boolean trustEmptyDB;
        File initFile = new File(dataDir.getParent(), "initialize");
        if(Files.deleteIfExists(initFile.toPath())) {
            LOG.info("Initialize file found, an empty database will not block voting participation");
            trustEmptyDB = true;
        } else {
            trustEmptyDB = autoCreateDB;
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 从日志恢复一小部分最新数据
         *  96-100 这五条事务的数据，从日志中执行恢复
         *  扫描日志文件
         *  highestZxid = 100
         */
        RestoreFinalizer finalizer = () -> {
            long highestZxid = fastForwardFromEdits(dt, sessions, listener);
            // The snapshotZxidDigest will reset after replaying the txn of the
            // zxid in the snapshotZxidDigest, if it's not reset to null after
            // restoring, it means either there are not enough txns to cover that
            // zxid or that txn is missing
            DataTree.ZxidDigest snapshotZxidDigest = dt.getDigestFromLoadedSnapshot();
            if(snapshotZxidDigest != null) {
                LOG.warn(
                        "Highest txn zxid 0x{} is not covering the snapshot digest zxid 0x{}, " + "which might lead to inconsistent state",
                        Long.toHexString(highestZxid), Long.toHexString(snapshotZxidDigest.getZxid()));
            }

            // TODO_MA 注释： 返回 最大的 zxid
            return highestZxid;
        };

        // TODO_MA 注释： 如果还没有快照文件，则执行 ZKDatabase 初始化
        if(-1L == deserializeResult) {
            /* this means that we couldn't find any snapshot, so we need to
             * initialize an empty database (reported in ZOOKEEPER-2325) */
            if(txnLog.getLastLoggedZxid() != -1) {
                // ZOOKEEPER-3056: provides an escape hatch for users upgrading
                // from old versions of zookeeper (3.4.x, pre 3.5.3).
                if(!trustEmptySnapshot) {
                    throw new IOException(EMPTY_SNAPSHOT_WARNING + "Something is broken!");
                } else {
                    LOG.warn("{}This should only be allowed during upgrading.", EMPTY_SNAPSHOT_WARNING);
                    return finalizer.run();
                }
            }

            if(trustEmptyDB) {
                /* TODO: (br33d) we should either put a ConcurrentHashMap on restore() or use Map on save() */
                save(dt, (ConcurrentHashMap<Long, Integer>) sessions, false);

                /* return a zxid of 0, since we know the database is empty */
                return 0L;
            } else {
                /* return a zxid of -1, since we are possibly missing data */
                LOG.warn("Unexpected empty data tree, setting zxid to -1");
                dt.lastProcessedZxid = -1L;
                return -1L;
            }
        }

        return finalizer.run();
    }

    /**
     * This function will fast forward the server database to have the latest
     * transactions in it.  This is the same as restore, but only reads from
     * the transaction logs and not restores from a snapshot.
     *
     * @param dt       the datatree to write transactions to.
     * @param sessions the sessions to be restored.
     * @param listener the playback listener to run on the
     *                 database transactions.
     * @return the highest zxid restored.
     * @throws IOException
     * // TODO_MA 注释： snap.deserialize();
     * // TODO_MA 注释： txnLog.read(dt.lastProcessedZxid + 1)
     * dt.lastProcessedZxid = 95
     */
    public long fastForwardFromEdits(DataTree dt, Map<Long, Integer> sessions,
            PlayBackListener listener) throws IOException {

        // TODO_MA 注释： 注意此处： 从 dt.lastProcessedZxid + 1 处开始读取 日志文件中的 日志进行恢复
        // TODO_MA 注释： 从日志文件中，开始读取第 96 事务的数据
        TxnIterator itr = txnLog.read(dt.lastProcessedZxid + 1);
        // TODO_MA 注释： 等于举例的 95
        long highestZxid = dt.lastProcessedZxid;

        TxnHeader hdr;
        int txnLoaded = 0;
        long startTime = Time.currentElapsedTime();
        try {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            while(true) {

                // TODO_MA 注释： 先读取 日志消息的 header，这里面包含了 zxid
                // iterator points to the first valid txn when initialized
                hdr = itr.getHeader();

                // TODO_MA 注释： 如果为空，则直接返回
                if(hdr == null) {
                    //empty logs
                    return dt.lastProcessedZxid;
                }

                // TODO_MA 注释： 如果从日志文件中读取的 zxid 小于 datatree 中的，则忽略
                if(hdr.getZxid() < highestZxid && highestZxid != 0) {
                    LOG.error("{}(highestZxid) > {}(next log) for type {}", highestZxid, hdr.getZxid(), hdr.getType());
                } else {
                    // TODO_MA 注释： while每执行一次，其实这个 highestZxid 就 + 1
                    highestZxid = hdr.getZxid();
                }
                try {

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 恢复事务的执行，更新数据到内存 datatree 中
                     *  1、session
                     *  2、datatree的datanode节点
                     *  replay  =  复现 = 重复执行
                     */
                    processTransaction(hdr, dt, sessions, itr.getTxn());

                    dt.compareDigest(hdr, itr.getTxn(), itr.getDigest());

                    // TODO_MA 注释： 读取的日志 计数 +1
                    txnLoaded++;
                } catch(KeeperException.NoNodeException e) {
                    throw new IOException(
                            "Failed to process transaction type: " + hdr.getType() + " error: " + e.getMessage(), e);
                }
                listener.onTxnLoaded(hdr, itr.getTxn(), itr.getDigest());

                // TODO_MA 注释： 如果读取完毕，则退出循环
                if(!itr.next()) {
                    break;
                }
            }
        } finally {
            if(itr != null) {
                itr.close();
            }
        }

        // TODO_MA 注释： 计算从日志读取的时间消耗
        long loadTime = Time.currentElapsedTime() - startTime;
        LOG.info("{} txns loaded in {} ms", txnLoaded, loadTime);

        ServerMetrics.getMetrics().STARTUP_TXNS_LOADED.add(txnLoaded);
        ServerMetrics.getMetrics().STARTUP_TXNS_LOAD_TIME.add(loadTime);

        // TODO_MA 注释： 返回最大的 zxid
        return highestZxid;
    }

    /**
     * Get TxnIterator for iterating through txnlog starting at a given zxid
     *
     * @param zxid starting zxid
     * @return TxnIterator
     * @throws IOException
     */
    public TxnIterator readTxnLog(long zxid) throws IOException {
        return readTxnLog(zxid, true);
    }

    /**
     * Get TxnIterator for iterating through txnlog starting at a given zxid
     *
     * @param zxid        starting zxid
     * @param fastForward true if the iterator should be fast forwarded to point
     *                    to the txn of a given zxid, else the iterator will point to the
     *                    starting txn of a txnlog that may contain txn of a given zxid
     * @return TxnIterator
     * @throws IOException
     */
    public TxnIterator readTxnLog(long zxid, boolean fastForward) throws IOException {
        FileTxnLog txnLog = new FileTxnLog(dataDir);
        return txnLog.read(zxid, fastForward);
    }

    /**
     * process the transaction on the datatree
     *
     * @param hdr      the hdr of the transaction
     * @param dt       the datatree to apply transaction to
     * @param sessions the sessions to be restored
     * @param txn      the transaction to be applied
     */
    public void processTransaction(TxnHeader hdr, DataTree dt, Map<Long, Integer> sessions,
            Record txn) throws KeeperException.NoNodeException {
        ProcessTxnResult rc;

        // TODO_MA 注释： session 相关动作， znode 相关动作
        switch(hdr.getType()) {
            case OpCode.createSession:
                sessions.put(hdr.getClientId(), ((CreateSessionTxn) txn).getTimeOut());
                if(LOG.isTraceEnabled()) {
                    ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK,
                            "playLog --- create session in log: 0x" + Long
                                    .toHexString(hdr.getClientId()) + " with timeout: " + ((CreateSessionTxn) txn)
                                    .getTimeOut());
                }
                // give dataTree a chance to sync its lastProcessedZxid
                rc = dt.processTxn(hdr, txn);
                break;
            case OpCode.closeSession:
                sessions.remove(hdr.getClientId());
                if(LOG.isTraceEnabled()) {
                    ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK,
                            "playLog --- close session in log: 0x" + Long.toHexString(hdr.getClientId()));
                }
                rc = dt.processTxn(hdr, txn);
                break;

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 增删改 DataNode 节点的动作
             */
            default:
                rc = dt.processTxn(hdr, txn);
        }

        /*
         * Snapshots are lazily created. So when a snapshot is in progress,
         * there is a chance for later transactions to make into the
         * snapshot. Then when the snapshot is restored, NONODE/NODEEXISTS
         * errors could occur. It should be safe to ignore these.
         */
        if(rc.err != Code.OK.intValue()) {
            LOG.debug("Ignoring processTxn failure hdr: {}, error: {}, path: {}", hdr.getType(), rc.err, rc.path);
        }
    }

    /**
     * the last logged zxid on the transaction logs
     *
     * @return the last logged zxid
     */
    public long getLastLoggedZxid() {
        FileTxnLog txnLog = new FileTxnLog(dataDir);
        return txnLog.getLastLoggedZxid();
    }

    /**
     * save the datatree and the sessions into a snapshot
     *
     * @param dataTree             the datatree to be serialized onto disk
     * @param sessionsWithTimeouts the session timeouts to be
     *                             serialized onto disk
     * @param syncSnap             sync the snapshot immediately after write
     * @throws IOException
     */
    public void save(DataTree dataTree, ConcurrentHashMap<Long, Integer> sessionsWithTimeouts,
            boolean syncSnap) throws IOException {

        // TODO_MA 注释： 获取 zxid
        long lastZxid = dataTree.lastProcessedZxid;

        // TODO_MA 注释： 生成快照文件名
		// TODO_MA 注释： -rw-rw-r-- 1 bigdata bigdata    594 Feb 28 14:17 snapshot.0
		// TODO_MA 注释： -rw-rw-r-- 1 bigdata bigdata    594 Feb 28 14:17 snapshot.100000000
		// TODO_MA 注释： -rw-rw-r-- 1 bigdata bigdata 147203 Jul 14 17:10 snapshot.10000001c3
		// TODO_MA 注释： -rw-rw-r-- 1 bigdata bigdata  50221 Mar  1 10:26 snapshot.20000d77d
		// TODO_MA 注释： -rw-rw-r-- 1 bigdata bigdata 122839 Apr 26 15:55 snapshot.60000068e
		// TODO_MA 注释： -rw-rw-r-- 1 bigdata bigdata 118703 May 11 19:27 snapshot.70000003d
		// TODO_MA 注释： -rw-rw-r-- 1 bigdata bigdata 122408 Jun 19 17:34 snapshot.900000084
		// TODO_MA 注释： -rw-rw-r-- 1 bigdata bigdata 123100 Jun 22 17:43 snapshot.b00000002
		// TODO_MA 注释： -rw-rw-r-- 1 bigdata bigdata 142611 Jul  9 12:58 snapshot.c00000064
		// TODO_MA 注释： -rw-rw-r-- 1 bigdata bigdata 142793 Jul  9 12:59 snapshot.d00000010
        File snapshotFile = new File(snapDir, Util.makeSnapshotName(lastZxid));
        LOG.info("Snapshotting: 0x{} to {}", Long.toHexString(lastZxid), snapshotFile);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 拍摄快照
         */
        try {
            snapLog.serialize(dataTree, sessionsWithTimeouts, snapshotFile, syncSnap);
        } catch(IOException e) {
            if(snapshotFile.length() == 0) {
                /* This may be caused by a full disk. In such a case, the server
                 * will get stuck in a loop where it tries to write a snapshot
                 * out to disk, and ends up creating an empty file instead.
                 * Doing so will eventually result in valid snapshots being
                 * removed during cleanup. */
                if(snapshotFile.delete()) {
                    LOG.info("Deleted empty snapshot file: {}", snapshotFile.getAbsolutePath());
                } else {
                    LOG.warn("Could not delete empty snapshot file: {}", snapshotFile.getAbsolutePath());
                }
            } else {
                /* Something else went wrong when writing the snapshot out to
                 * disk. If this snapshot file is invalid, when restarting,
                 * ZooKeeper will skip it, and find the last known good snapshot
                 * instead. */
            }
            throw e;
        }
    }

    /**
     * truncate the transaction logs the zxid
     * specified
     *
     * @param zxid the zxid to truncate the logs to
     * @return true if able to truncate the log, false if not
     * @throws IOException
     */
    public boolean truncateLog(long zxid) {
        try {
            // close the existing txnLog and snapLog
            close();

            // truncate it
            try(FileTxnLog truncLog = new FileTxnLog(dataDir)) {

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释：
                 */
                boolean truncated = truncLog.truncate(zxid);

                // re-open the txnLog and snapLog
                // I'd rather just close/reopen this object itself, however that
                // would have a big impact outside ZKDatabase as there are other
                // objects holding a reference to this object.
                txnLog = new FileTxnLog(dataDir);
                snapLog = new FileSnap(snapDir);

                return truncated;
            }
        } catch(IOException e) {
            LOG.error("Unable to truncate Txn log", e);
            return false;
        }
    }

    /**
     * the most recent snapshot in the snapshot
     * directory
     *
     * @return the file that contains the most
     * recent snapshot
     * @throws IOException
     */
    public File findMostRecentSnapshot() throws IOException {
        FileSnap snaplog = new FileSnap(snapDir);
        return snaplog.findMostRecentSnapshot();
    }

    /**
     * the n most recent snapshots
     *
     * @param n the number of recent snapshots
     * @return the list of n most recent snapshots, with
     * the most recent in front
     * @throws IOException
     */
    public List<File> findNRecentSnapshots(int n) throws IOException {
        FileSnap snaplog = new FileSnap(snapDir);
        return snaplog.findNRecentSnapshots(n);
    }

    /**
     * the n recent valid snapshots
     *
     * @param n the number of recent valid snapshots
     * @return the list of n recent valid snapshots, with
     * the most recent in front
     * @throws IOException
     */
    public List<File> findNValidSnapshots(int n) throws IOException {
        FileSnap snaplog = new FileSnap(snapDir);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return snaplog.findNValidSnapshots(n);
    }

    /**
     * get the snapshot logs which may contain transactions newer than the given zxid.
     * This includes logs with starting zxid greater than given zxid, as well as the
     * newest transaction log with starting zxid less than given zxid.  The latter log
     * file may contain transactions beyond given zxid.
     *
     * @param zxid the zxid that contains logs greater than
     *             zxid
     * @return
     */
    public File[] getSnapshotLogs(long zxid) {
        return FileTxnLog.getLogFiles(dataDir.listFiles(), zxid);
    }

    /**
     * append the request to the transaction logs
     *
     * @param si the request to be appended
     * @return true iff something appended, otw false
     * @throws IOException
     */
    public boolean append(Request si) throws IOException {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 记录一条日志
         */
        return txnLog.append(si.getHdr(), si.getTxn(), si.getTxnDigest());
    }

    /**
     * commit the transaction of logs
     *
     * @throws IOException
     */
    public void commit() throws IOException {
        txnLog.commit();
    }

    /**
     * @return elapsed sync time of transaction log commit in milliseconds
     */
    public long getTxnLogElapsedSyncTime() {
        return txnLog.getTxnLogSyncElapsedTime();
    }

    /**
     * roll the transaction logs
     *
     * @throws IOException
     */
    public void rollLog() throws IOException {
        txnLog.rollLog();
    }

    /**
     * close the transaction log files
     *
     * @throws IOException
     */
    public void close() throws IOException {
        if(txnLog != null) {
            txnLog.close();
            txnLog = null;
        }
        if(snapLog != null) {
            snapLog.close();
            snapLog = null;
        }
    }

    @SuppressWarnings("serial")
    public static class DatadirException extends IOException {

        public DatadirException(String msg) {
            super(msg);
        }

        public DatadirException(String msg, Exception e) {
            super(msg, e);
        }

    }

    @SuppressWarnings("serial")
    public static class LogDirContentCheckException extends DatadirException {

        public LogDirContentCheckException(String msg) {
            super(msg);
        }

    }

    @SuppressWarnings("serial")
    public static class SnapDirContentCheckException extends DatadirException {

        public SnapDirContentCheckException(String msg) {
            super(msg);
        }

    }

    public void setTotalLogSize(long size) {
        txnLog.setTotalLogSize(size);
    }

    public long getTotalLogSize() {
        return txnLog.getTotalLogSize();
    }
}
