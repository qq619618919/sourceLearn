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

import static org.apache.hadoop.util.ExitUtil.terminate;
import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.Storage.FormatConfirmable;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AddBlockOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AddCacheDirectiveInfoOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AddCachePoolOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AddOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AllocateBlockIdOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AllowSnapshotOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AppendOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.CancelDelegationTokenOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.CloseOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.ConcatDeleteOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.CreateSnapshotOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.DeleteOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.DeleteSnapshotOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.DisallowSnapshotOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.GetDelegationTokenOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.LogSegmentOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.MkdirOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.ModifyCacheDirectiveInfoOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.ModifyCachePoolOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.OpInstanceCache;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.ReassignLeaseOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RemoveCacheDirectiveInfoOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RemoveCachePoolOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RemoveXAttrOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RenameOldOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RenameOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RenameSnapshotOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RenewDelegationTokenOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RollingUpgradeFinalizeOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RollingUpgradeOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RollingUpgradeStartOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetAclOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetGenstampV1Op;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetGenstampV2Op;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetOwnerOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetPermissionsOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetQuotaOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetQuotaByStorageTypeOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetReplicationOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetStoragePolicyOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetXAttrOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SymlinkOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.TimesOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.TruncateOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.UpdateBlocksOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.UpdateMasterKeyOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AddErasureCodingPolicyOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.EnableErasureCodingPolicyOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.DisableErasureCodingPolicyOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RemoveErasureCodingPolicyOp;
import org.apache.hadoop.hdfs.server.namenode.JournalSet.JournalAndStream;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.token.delegation.DelegationKey;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FSEditLog maintains a log of the namespace modifications.
 * // TODO_MA ????????? ????????? FSEditLog ????????????????????????????????????
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSEditLog implements LogsPurgeable {
    public static final Logger LOG = LoggerFactory.getLogger(FSEditLog.class);

    /**
     * // TODO_MA ????????? ????????? ??????????????????????????????
     * State machine for edit log.
     *
     * In a non-HA setup:
     *
     * The log starts in UNINITIALIZED state upon construction. Once it's
     * initialized, it is usually in IN_SEGMENT state, indicating that edits may
     * be written. In the middle of a roll, or while saving the namespace, it
     * briefly enters the BETWEEN_LOG_SEGMENTS state, indicating that the previous
     * segment has been closed, but the new one has not yet been opened.
     *
     * In an HA setup:
     *
     * The log starts in UNINITIALIZED state upon construction. Once it's
     * initialized, it sits in the OPEN_FOR_READING state the entire time that the
     * NN is in standby. Upon the NN transition to active, the log will be CLOSED,
     * and then move to being BETWEEN_LOG_SEGMENTS, much as if the NN had just
     * started up, and then will move to IN_SEGMENT so it can begin writing to the
     * log. The log states will then revert to behaving as they do in a non-HA setup.
     *
     * // TODO_MA ????????? ????????? FSEditLog ??????????????????
     */
    private enum State {
        UNINITIALIZED,              // TODO_MA ????????? ????????? ????????????
        BETWEEN_LOG_SEGMENTS,       // TODO_MA ????????? ????????? segment ??????
        IN_SEGMENT,                 // TODO_MA ????????? ????????? editlog ????????????
        OPEN_FOR_READING,           // TODO_MA ????????? ????????? editlog ????????????
        CLOSED;                     // TODO_MA ????????? ????????? editlog ????????????
    }

    private State state = State.UNINITIALIZED;

    //initialize
    private JournalSet journalSet = null;

    /*************************************************
     * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
     *  ????????? ?????????
     */
    @VisibleForTesting
    EditLogOutputStream editLogStream = null;

    // a monotonically increasing counter that represents transactionIds.
    // All of the threads which update/increment txid are synchronized,
    // so make txid volatile instead of AtomicLong.
    private volatile long txid = 0;

    // stores the last synced transactionId.
    private long synctxid = 0;

    // the first txid of the log that's currently open for writing.
    // If this value is N, we are currently writing to edits_inprogress_N
    private volatile long curSegmentTxId = HdfsServerConstants.INVALID_TXID;

    // the time of printing the statistics to the log file.
    private long lastPrintTime;

    // is a sync currently running?
    private volatile boolean isSyncRunning;

    // is an automatic sync scheduled?
    private volatile boolean isAutoSyncScheduled = false;

    // these are statistics counters.
    private long numTransactions;        // number of transactions
    private final LongAdder numTransactionsBatchedInSync = new LongAdder();
    private long totalTimeTransactions;  // total time for all transactions
    private NameNodeMetrics metrics;

    private final NNStorage storage;
    private final Configuration conf;

    private final List<URI> editsDirs;

    protected final OpInstanceCache cache = new OpInstanceCache();

    /**
     * The edit directories that are shared between primary and secondary.
     */
    private final List<URI> sharedEditsDirs;

    /**
     * Take this lock when adding journals to or closing the JournalSet. Allows
     * us to ensure that the JournalSet isn't closed or updated underneath us
     * in selectInputStreams().
     */
    private final Object journalSetLock = new Object();

    /*************************************************
     * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
     *  ????????? HDFS ???????????????id ?????????
     */
    private static class TransactionId {
        public long txid;

        TransactionId(long value) {
            this.txid = value;
        }
    }

    // stores the most current transactionId of this thread.
    private static final ThreadLocal<TransactionId> myTransactionId = new ThreadLocal<TransactionId>() {
        @Override
        protected synchronized TransactionId initialValue() {
            return new TransactionId(Long.MAX_VALUE);
        }
    };

    /*************************************************
     * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
     *  ????????? hadoop-3.x ????????????????????? ????????????????????? ????????????
     */
    static FSEditLog newInstance(Configuration conf, NNStorage storage, List<URI> editsDirs) {

        // TODO_MA ????????? ????????? dfs.namenode.edits.asynclogging = true
        boolean asyncEditLogging = conf.getBoolean(DFSConfigKeys.DFS_NAMENODE_EDITS_ASYNC_LOGGING,
                DFSConfigKeys.DFS_NAMENODE_EDITS_ASYNC_LOGGING_DEFAULT
        );

        LOG.info("Edit logging is async:" + asyncEditLogging);

        // TODO_MA ????????? ????????? ?????? FSEditLog ???????????????????????? FSEditLogAsync ??????
        return asyncEditLogging ? new FSEditLogAsync(conf, storage, editsDirs) : new FSEditLog(conf, storage, editsDirs);
    }

    /**
     * Constructor for FSEditLog. Underlying journals are constructed, but
     * no streams are opened until open() is called.
     *
     * @param conf The namenode configuration
     * @param storage Storage object used by namenode
     * @param editsDirs List of journals to use
     */
    FSEditLog(Configuration conf, NNStorage storage, List<URI> editsDirs) {
        isSyncRunning = false;
        this.conf = conf;
        this.storage = storage;
        metrics = NameNode.getNameNodeMetrics();
        lastPrintTime = monotonicNow();

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ????????? ?????????????????????
         */
        // If this list is empty, an error will be thrown on first use
        // of the editlog, as no journals will exist
        this.editsDirs = Lists.newArrayList(editsDirs);

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ????????? dfs.namenode.shared.edits.dir ??????
         */
        this.sharedEditsDirs = FSNamesystem.getSharedEditsDirs(conf);
    }

    /*************************************************
     * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
     *  ?????????
     *  UNINITIALIZED | CLOSED ==> BETWEEN_LOG_SEGMENTS
     */
    public synchronized void initJournalsForWrite() {
        Preconditions.checkState(state == State.UNINITIALIZED || state == State.CLOSED, "Unexpected state: %s", state);

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ?????????
         */
        initJournals(this.editsDirs);
        state = State.BETWEEN_LOG_SEGMENTS;
    }

    /*************************************************
     * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
     *  ????????? UNINITIALIZED ==> OPEN_FOR_READING
     */
    public synchronized void initSharedJournalsForRead() {
        if (state == State.OPEN_FOR_READING) {
            LOG.warn("Initializing shared journals for READ, already open for READ", new Exception());
            return;
        }
        // TODO_MA ????????? ????????? ????????????
        Preconditions.checkState(state == State.UNINITIALIZED || state == State.CLOSED);

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ????????? ????????? JournalManager
         */
        initJournals(this.sharedEditsDirs);

        // TODO_MA ????????? ????????? ????????? ??????
        state = State.OPEN_FOR_READING;
    }

    /*************************************************
     * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
     *  ????????? ????????? JournalManager
     *  ?????????NameNode ?????????
     *  FSNameSystem
     *  FSImage
     *  FSEditLog
     *  journalSet
     *  JournalManager
     */
    private synchronized void initJournals(List<URI> dirs) {

        // TODO_MA ????????? ????????? dfs.namenode.edits.dir.minimum = 1
        int minimumRedundantJournals = conf.getInt(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_MINIMUM_KEY,
                DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_MINIMUM_DEFAULT
        );

        synchronized (journalSetLock) {

            // TODO_MA ????????? ????????? ????????? JournalSet ???????????? JournalManager
            // TODO_MA ????????? ????????? JournalManager ??????????????????????????????????????? editlog
            journalSet = new JournalSet(minimumRedundantJournals);

            // TODO_MA ????????? ????????? ??????????????????????????????????????? JournalManager ????????? editlog
            // TODO_MA ????????? ????????? namenode ?????????????????????  file:///home/bigdata/hadoopdata/dfs/name/current
            // TODO_MA ????????? ????????? journalNode ?????????????????????   qjournal/...../journaldata/....
            for (URI u : dirs) {
                boolean required = FSNamesystem.getRequiredNamespaceEditsDirs(conf).contains(u);

                // TODO_MA ????????? ????????? ?????? URI????????? local ????????? FileJournalManager
                if (u.getScheme().equals(NNStorage.LOCAL_URI_SCHEME)) {
                    StorageDirectory sd = storage.getStorageDirectory(u);
                    if (sd != null) {
                        journalSet.add(new FileJournalManager(conf, sd, storage), required, sharedEditsDirs.contains(u));
                    }
                }

                // TODO_MA ????????? ????????? ?????? URI ?????? JournalManager ?????????????????? JournalAndStream ??????????????? JournalSet ???
                // TODO_MA ????????? ????????? ??? JournalNode ??????????????? QuorumJournalManager
                else {
                    journalSet.add(createJournal(u), required, sharedEditsDirs.contains(u));
                }
            }
        }

        if (journalSet.isEmpty()) {
            LOG.error("No edits directories configured!");
        }
    }

    /**
     * Get the list of URIs the editlog is using for storage
     * @return collection of URIs in use by the edit log
     */
    Collection<URI> getEditURIs() {
        return editsDirs;
    }

    /**
     * // TODO_MA ????????? ????????? ?????????????????????????????????????????? log segment
     * Initialize the output stream for logging, opening the first log segment.
     */
    synchronized void openForWrite(int layoutVersion) throws IOException {
        // TODO_MA ????????? ????????? ????????????
        Preconditions.checkState(state == State.BETWEEN_LOG_SEGMENTS, "Bad state: %s", state);

        // TODO_MA ????????? ????????? ??? log segment ??? txid
        long segmentTxId = getLastWrittenTxId() + 1;

        // Safety check: we should never start a segment if there are newer txids readable.
        List<EditLogInputStream> streams = new ArrayList<EditLogInputStream>();

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ?????????
         *  1????????? segmentTxId ?????? txid ????????????
         *  2???????????????????????????????????? ??? segmentTxId ????????? editlog ???????????????
         */
        journalSet.selectInputStreams(streams, segmentTxId, true, false);

        // TODO_MA ????????? ????????? ???????????? segmentTxId ??? editlog ????????????????????????
        if (!streams.isEmpty()) {
            String error = String.format("Cannot start writing at txid %s " + "when there is a stream available for read: %s",
                    segmentTxId, streams.get(0)
            );
            IOUtils.cleanupWithLogger(LOG, streams.toArray(new EditLogInputStream[0]));
            throw new IllegalStateException(error);
        }

        // TODO_MA ????????? ????????? ??????????????????
        startLogSegmentAndWriteHeaderTxn(segmentTxId, layoutVersion);

        // TODO_MA ????????? ????????? ????????? IN_SEGMENT
        assert state == State.IN_SEGMENT : "Bad state: " + state;
    }

    /**
     * @return true if the log is currently open in write mode, regardless
     * of whether it actually has an open segment.
     */
    synchronized boolean isOpenForWrite() {
        return state == State.IN_SEGMENT || state == State.BETWEEN_LOG_SEGMENTS;
    }

    /**
     * Return true if the log is currently open in write mode.
     * This method is not synchronized and must be used only for metrics.
     * @return true if the log is currently open in write mode, regardless
     * of whether it actually has an open segment.
     */
    boolean isOpenForWriteWithoutLock() {
        return state == State.IN_SEGMENT || state == State.BETWEEN_LOG_SEGMENTS;
    }

    /**
     * @return true if the log is open in write mode and has a segment open
     * ready to take edits.
     */
    synchronized boolean isSegmentOpen() {
        return state == State.IN_SEGMENT;
    }

    /**
     * Return true the state is IN_SEGMENT.
     * This method is not synchronized and must be used only for metrics.
     * @return true if the log is open in write mode and has a segment open
     * ready to take edits.
     */
    boolean isSegmentOpenWithoutLock() {
        return state == State.IN_SEGMENT;
    }

    /**
     * @return true if the log is open in read mode.
     */
    public synchronized boolean isOpenForRead() {
        return state == State.OPEN_FOR_READING;
    }

    /**
     * Shutdown the file store.
     */
    synchronized void close() {
        if (state == State.CLOSED) {
            LOG.debug("Closing log when already closed");
            return;
        }

        try {
            /*************************************************
             * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
             *  ????????? ???????????? IN_SEGMENT ??? CLOSED
             */
            if (state == State.IN_SEGMENT) {
                assert editLogStream != null;

                // TODO_MA ????????? ????????? ?????? sync ??????
                waitForSyncToFinish();

                /*************************************************
                 * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
                 *  ????????? ???????????? log segment
                 *  ????????? curSegmentTxid ??? lastTxId ??????????????????????????????
                 */
                endCurrentLogSegment(true);
            }
        } finally {

            // TODO_MA ????????? ????????? journalSet close
            if (journalSet != null && !journalSet.isEmpty()) {
                try {
                    synchronized (journalSetLock) {
                        journalSet.close();
                    }
                } catch (IOException ioe) {
                    LOG.warn("Error closing journalSet", ioe);
                }
            }

            // TODO_MA ????????? ????????? ????????????
            state = State.CLOSED;
        }
    }


    /**
     * Format all configured journals which are not file-based.
     *
     * File-based journals are skipped, since they are formatted by the
     * Storage format code.
     */
    synchronized void formatNonFileJournals(NamespaceInfo nsInfo, boolean force) throws IOException {
        Preconditions.checkState(state == State.BETWEEN_LOG_SEGMENTS, "Bad state: %s", state);

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ????????? ????????? Journal
         */
        for (JournalManager jm : journalSet.getJournalManagers()) {
            if (!(jm instanceof FileJournalManager)) {
                jm.format(nsInfo, force);
            }
        }
    }

    synchronized List<FormatConfirmable> getFormatConfirmables() {
        Preconditions.checkState(state == State.BETWEEN_LOG_SEGMENTS, "Bad state: %s", state);

        List<FormatConfirmable> ret = Lists.newArrayList();
        for (final JournalManager jm : journalSet.getJournalManagers()) {
            // The FJMs are confirmed separately since they are also
            // StorageDirectories
            if (!(jm instanceof FileJournalManager)) {
                ret.add(jm);
            }
        }
        return ret;
    }

    /**
     * Write an operation to the edit log.
     * <p/>
     * Additionally, this will sync the edit log if required by the underlying
     * edit stream's automatic sync policy (e.g. when the buffer is full, or
     * if a time interval has elapsed).
     */
    void logEdit(final FSEditLogOp op) {

        boolean needsSync = false;

        synchronized (this) {
            assert isOpenForWrite() : "bad state: " + state;

            // TODO_MA ????????? ????????? ???????????????????????????????????????????????????
            // TODO_MA ????????? ????????? ?????? bufReady ?????????flush??????????????? bufCurrent ??? bufReady ????????????????????????????????????
            // wait if an automatic sync is scheduled
            waitIfAutoSyncScheduled();

            // check if it is time to schedule an automatic sync
            /*************************************************
             * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
             *  ????????? ??????????????????
             *  1?????????????????? FSDirectory ???
             *  2?????????????????? ????????????
             */
            needsSync = doEditTransaction(op);

            // TODO_MA ????????? ????????? ?????? needsSync = true???????????? bufCurrent

            // TODO_MA ????????? ????????? ??????????????????????????????????????????
            if (needsSync) {
                isAutoSyncScheduled = true;
            }
        }

        // TODO_MA ????????? ????????? ???????????? flush
        // TODO_MA ????????? ????????? ?????? bufCurrent ????????? 512kb
        // Sync the log if an automatic sync is required.
        if (needsSync) {
            logSync();
        }
    }

    /*************************************************
     * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
     *  ????????? ??????????????????????????????????????? Txid
     *  ????????????
     *  1????????????????????? transaction
     *  2????????? editlog ????????????????????????????????????
     *  3????????? endTransaction() ?????????????????? transaction
     *  4????????? logSync() ??????????????????????????????????????????
     *  ????????? ????????????????????????????????? ????????????????????? txid ?????????
     */
    synchronized boolean doEditTransaction(final FSEditLogOp op) {

        // TODO_MA ????????? ????????? ????????????????????? txid
        long start = beginTransaction();
        op.setTransactionId(txid);

        try {
            /*************************************************
             * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
             *  ????????? ??????????????????????????? editLogStream ?????? Op ??????
             *  editLogStream = JournalSetOutputStream
             */
            editLogStream.write(op);
        } catch (IOException ex) {
            // All journals failed, it is handled in logSync.
        } finally {
            op.reset();
        }

        // TODO_MA ????????? ????????? ????????????
        endTransaction(start);

        // TODO_MA ????????? ????????? ??????????????????????????????
        // TODO_MA ????????? ????????? ??????????????? ???????????????????????????????????? 512kb
        return shouldForceSync();
    }

    /**
     * Wait if an automatic sync is scheduled
     */
    synchronized void waitIfAutoSyncScheduled() {
        try {
            while (isAutoSyncScheduled) {
                this.wait(1000);
            }
        } catch (InterruptedException e) {
        }
    }

    /**
     * Signal that an automatic sync scheduling is done if it is scheduled
     */
    synchronized void doneWithAutoSyncScheduling() {
        if (isAutoSyncScheduled) {
            isAutoSyncScheduled = false;
            notifyAll();
        }
    }

    /**
     * Check if should automatically sync buffered edits to
     * persistent store
     *
     * @return true if any of the edit stream says that it should sync
     */
    private boolean shouldForceSync() {

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ????????? ???????????????????????????????????????????????? 512kb
         */
        return editLogStream.shouldForceSync();
    }

    /*************************************************
     * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
     *  ?????????  ???????????????txid + 1
     */
    private long beginTransaction() {
        assert Thread.holdsLock(this);

        // TODO_MA ????????? ????????? txid +1
        // get a new transactionId
        txid++;

        // TODO_MA ????????? ????????? ?????? ThreadLocal ?????? txid ?????????
        // record the transactionId when new data was written to the edits log
        TransactionId id = myTransactionId.get();

        id.txid = txid;
        // TODO_MA ????????? ????????? ???????????????????????????
        return monotonicNow();
    }

    /*************************************************
     * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
     *  ????????? ????????????
     */
    private void endTransaction(long start) {
        assert Thread.holdsLock(this);

        // update statistics
        long end = monotonicNow();
        // TODO_MA ????????? ????????? ????????????
        numTransactions++;

        // TODO_MA ????????? ????????? ??????????????????
        totalTimeTransactions += (end - start);
        if (metrics != null) // Metrics is non-null only when used inside name node
            metrics.addTransaction(end - start);
    }

    /**
     * Return the transaction ID of the last transaction written to the log.
     */
    public synchronized long getLastWrittenTxId() {
        return txid;
    }

    /**
     * Return the transaction ID of the last transaction written to the log.
     * This method is not synchronized and must be used only for metrics.
     * @return The transaction ID of the last transaction written to the log
     */
    long getLastWrittenTxIdWithoutLock() {
        return txid;
    }

    /**
     * @return the first transaction ID in the current log segment
     */
    @VisibleForTesting
    public synchronized long getCurSegmentTxId() {
        Preconditions.checkState(isSegmentOpen(), "Bad state: %s", state);
        return curSegmentTxId;
    }

    /**
     * Return the first transaction ID in the current log segment.
     * This method is not synchronized and must be used only for metrics.
     * @return The first transaction ID in the current log segment
     */
    long getCurSegmentTxIdWithoutLock() {
        return curSegmentTxId;
    }

    /**
     * Set the transaction ID to use for the next transaction written.
     */
    synchronized void setNextTxId(long nextTxId) {
        Preconditions.checkArgument(synctxid <= txid && nextTxId >= txid,
                "May not decrease txid." + " synctxid=%s txid=%s nextTxId=%s", synctxid, txid, nextTxId
        );

        txid = nextTxId - 1;
    }

    /**
     * Blocks until all ongoing edits have been synced to disk.
     * This differs from logSync in that it waits for edits that have been
     * written by other threads, not just edits from the calling thread.
     *
     * NOTE: this should be done while holding the FSNamesystem lock, or
     * else more operations can start writing while this is in progress.
     */
    void logSyncAll() {
        // Make sure we're synced up to the most recent transaction ID.
        long lastWrittenTxId = getLastWrittenTxId();
        LOG.info("logSyncAll toSyncToTxId=" + lastWrittenTxId + " lastSyncedTxid=" + synctxid + " mostRecentTxid=" + txid);

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ?????????
         */
        logSync(lastWrittenTxId);

        lastWrittenTxId = getLastWrittenTxId();
        LOG.info("Done logSyncAll lastWrittenTxId=" + lastWrittenTxId + " lastSyncedTxid=" + synctxid + " mostRecentTxid=" + txid);
    }

    /**
     * Sync all modifications done by this thread.
     *
     * The internal concurrency design of this class is as follows:
     *   - Log items are written synchronized into an in-memory buffer,
     *     and each assigned a transaction ID.
     *   - When a thread (client) would like to sync all of its edits, logSync()
     *     uses a ThreadLocal transaction ID to determine what edit number must
     *     be synced to.
     *   - The isSyncRunning volatile boolean tracks whether a sync is currently
     *     under progress.
     *
     * The data is double-buffered within each edit log implementation so that
     * in-memory writing can occur in parallel with the on-disk writing.
     *
     * Each sync occurs in three steps:
     *   1. synchronized, it swaps the double buffer and sets the isSyncRunning
     *      flag.
     *   2. unsynchronized, it flushes the data to storage
     *   3. synchronized, it resets the flag and notifies anyone waiting on the
     *      sync.
     *
     * The lack of synchronization on step 2 allows other threads to continue
     * to write into the memory buffer while the sync is in progress.
     * Because this step is unsynchronized, actions that need to avoid
     * concurrency with sync() should be synchronized and also call
     * waitForSyncToFinish() before assuming they are running alone.
     */
    public void logSync() {
        // Fetch the transactionId of this thread.
        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ?????????
         */
        logSync(myTransactionId.get().txid);
    }

    /*************************************************
     * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
     *  ?????????
     */
    protected void logSync(long mytxid) {
        long syncStart = 0;
        boolean sync = false;
        long editsBatchedInSync = 0;
        try {
            EditLogOutputStream logStream = null;

            // TODO_MA ????????? ????????? ???
            synchronized (this) {
                try {
                    // TODO_MA ????????? ????????? ??????????????????
                    printStatistics(false);

                    // TODO_MA ????????? ????????? ??? mytxid ?????? synctxid ?????? isSyncRunning ??? true
                    // TODO_MA ????????? ????????? ??? ??????????????????????????? ?????? flush ??????????????????
                    // if somebody is already syncing, then wait
                    while (mytxid > synctxid && isSyncRunning) {
                        try {
                            wait(1000);
                        } catch (InterruptedException ie) {
                        }
                    }

//                    // ?????????????????????????????????????????????????????????????????????
//                    // Hashmap ?????????????????????????????????
//                    // hbase ??? spark ?????????
//                    if( bufcurrent.size() < bufferMaxSize && isSyncRunning){
//                        // ?????????????????? buffer
//                        TxnBuffer old = bufCurrent;
//                        // ??????????????????????????????
//                        bufCurrent = new TxnBuffer(bufcurrent.size() * 2);
//                        // ???????????????
//                        System.copy(old, bufCurrent);
//                        // ???????????????????????????????????????????????????????????????????????????????????????????????????????????????
//                        return;
//                    } else {
//                        try {
//                            wait(1000);
//                        } catch (InterruptedException ie) {
//                        }
//                    }


                    /*************************************************
                     * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
                     *  ????????? ?????? mytxid ?????? synctxid ????????????????????????
                     */
                    // If this transaction was already flushed, then nothing to do
                    if (mytxid <= synctxid) {
                        return;
                    }

                    // TODO_MA ????????? ????????? ???????????? flush ???
                    // now, this thread will do the sync.  track if other edits were
                    // included in the sync - ie. batched.  if this is the only edit
                    // synced then the batched count is 0
                    editsBatchedInSync = txid - synctxid - 1;
                    syncStart = txid;

                    // TODO_MA ????????? ????????? ????????? true ?????????????????? sync
                    isSyncRunning = true;
                    sync = true;

                    // swap buffers
                    try {
                        if (journalSet.isEmpty()) {
                            throw new IOException("No journals available to flush");
                        }
                        /*************************************************
                         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
                         *  ????????? ??????????????????????????????????????????????????????????????????????????????
                         */
                        editLogStream.setReadyToFlush();
                    } catch (IOException e) {
                        final String msg = "Could not sync enough journals to persistent storage " + "due to " + e.getMessage() + ". " + "Unsynced transactions: " + (txid - synctxid);
                        LOG.error(msg, new Exception());
                        synchronized (journalSetLock) {
                            IOUtils.cleanupWithLogger(LOG, journalSet);
                        }
                        terminate(1, msg);
                    }
                } finally {
                    // TODO_MA ????????? ????????? ??????????????????????????????????????????????????????????????????????????????????????????
                    // Prevent RuntimeException from blocking other log edit write
                    doneWithAutoSyncScheduling();
                }
                //editLogStream may become null,
                //so store a local variable for flush.
                logStream = editLogStream;
            }

            // do the sync
            long start = monotonicNow();
            try {
                /*************************************************
                 * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
                 *  ????????? ???????????? flsuh ?????????????????? editlog ???
                 *  ??????????????????????????????????????????????????????????????????
                 */
                if (logStream != null) {
                    logStream.flush();
                }

                /*************************************************
                 * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
                 *  ????????? ???????????? IO ????????????  QuorumCall ????????????????????????????????? 20s ??????????????? ?????????????????????????????? ?????????
                 */
            } catch (IOException ex) {
                /*************************************************
                 * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
                 *  ????????? ?????? JournalNode ???????????????
                 */
                synchronized (this) {
                    final String msg = "Could not sync enough journals to persistent storage. " + "Unsynced transactions: " + (txid - synctxid);
                    // TODO_MA ????????? ????????? error ????????? journal ???????????????????????????
                    LOG.error(msg, new Exception());

                    synchronized (journalSetLock) {
                        IOUtils.cleanupWithLogger(LOG, journalSet);
                    }
                    /*************************************************
                     * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
                     *  ????????? ?????? flush ??????????????????????????????????????????????????? JVM ????????????
                     */
                    terminate(1, msg);
                }
            }
            long elapsed = monotonicNow() - start;

            if (metrics != null) { // Metrics non-null only when used inside name node
                metrics.addSync(elapsed);
                metrics.incrTransactionsBatchedInSync(editsBatchedInSync);
                numTransactionsBatchedInSync.add(editsBatchedInSync);
            }

        } finally {

            // TODO_MA ????????? ????????? ?????? ?????? isSyncRunning ????????? false
            // TODO_MA ????????? ????????? ??????????????? ?????????
            // Prevent RuntimeException from blocking other log edit sync
            synchronized (this) {
                if (sync) {
                    synctxid = syncStart;
                    for (JournalManager jm : journalSet.getJournalManagers()) {
                        /**
                         * {@link FileJournalManager#lastReadableTxId} is only meaningful
                         * for file-based journals. Therefore the interface is not added to
                         * other types of {@link JournalManager}.
                         */
                        if (jm instanceof FileJournalManager) {
                            ((FileJournalManager) jm).setLastReadableTxId(syncStart);
                        }
                    }
                    // TODO_MA ????????? ????????? flush ??????
                    isSyncRunning = false;
                }
                // TODO_MA ????????? ????????? ???????????????????????????
                this.notifyAll();
            }
        }
    }

    //
    // print statistics every 1 minute.
    //
    private void printStatistics(boolean force) {
        long now = monotonicNow();
        if (lastPrintTime + 60000 > now && !force) {
            return;
        }
        lastPrintTime = now;
        StringBuilder buf = new StringBuilder();
        buf.append("Number of transactions: ").append(numTransactions).append(" Total time for transactions(ms): ")
                .append(totalTimeTransactions).append(" Number of transactions batched in Syncs: ")
                .append(numTransactionsBatchedInSync.longValue()).append(" Number of syncs: ").append(editLogStream.getNumSync())
                .append(" SyncTimes(ms): ").append(journalSet.getSyncTimes());
        LOG.info(buf.toString());
    }

    /** Record the RPC IDs if necessary */
    private void logRpcIds(FSEditLogOp op, boolean toLogRpcIds) {
        if (toLogRpcIds) {
            op.setRpcClientId(Server.getClientId());
            op.setRpcCallId(Server.getCallId());
        }
    }

    public void logAppendFile(String path, INodeFile file, boolean newBlock, boolean toLogRpcIds) {
        FileUnderConstructionFeature uc = file.getFileUnderConstructionFeature();
        assert uc != null;
        AppendOp op = AppendOp.getInstance(cache.get()).setPath(path).setClientName(uc.getClientName())
                .setClientMachine(uc.getClientMachine()).setNewBlock(newBlock);
        logRpcIds(op, toLogRpcIds);
        logEdit(op);
    }

    /**
     * Add open lease record to edit log.
     * Records the block locations of the last block.
     */
    public void logOpenFile(String path, INodeFile newNode, boolean overwrite, boolean toLogRpcIds) {
        Preconditions.checkArgument(newNode.isUnderConstruction());
        PermissionStatus permissions = newNode.getPermissionStatus();

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ?????????
         */
        AddOp op = AddOp.getInstance(cache.get()).setInodeId(newNode.getId()).setPath(path)
                .setReplication(newNode.getFileReplication()).setModificationTime(newNode.getModificationTime())
                .setAccessTime(newNode.getAccessTime()).setBlockSize(newNode.getPreferredBlockSize()).setBlocks(newNode.getBlocks())
                .setPermissionStatus(permissions).setClientName(newNode.getFileUnderConstructionFeature().getClientName())
                .setClientMachine(newNode.getFileUnderConstructionFeature().getClientMachine()).setOverwrite(overwrite)
                .setStoragePolicyId(newNode.getLocalStoragePolicyID()).setErasureCodingPolicyId(newNode.getErasureCodingPolicyID());

        AclFeature f = newNode.getAclFeature();
        if (f != null) {
            op.setAclEntries(AclStorage.readINodeLogicalAcl(newNode));
        }

        XAttrFeature x = newNode.getXAttrFeature();
        if (x != null) {
            op.setXAttrs(x.getXAttrs());
        }

        logRpcIds(op, toLogRpcIds);

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ????????? ???????????????????????????????????????
         */
        logEdit(op);
    }

    /**
     * Add close lease record to edit log.
     */
    public void logCloseFile(String path, INodeFile newNode) {
        CloseOp op = CloseOp.getInstance(cache.get()).setPath(path).setReplication(newNode.getFileReplication())
                .setModificationTime(newNode.getModificationTime()).setAccessTime(newNode.getAccessTime())
                .setBlockSize(newNode.getPreferredBlockSize()).setBlocks(newNode.getBlocks())
                .setPermissionStatus(newNode.getPermissionStatus());

        logEdit(op);
    }

    public void logAddBlock(String path, INodeFile file) {
        Preconditions.checkArgument(file.isUnderConstruction());
        BlockInfo[] blocks = file.getBlocks();
        Preconditions.checkState(blocks != null && blocks.length > 0);
        BlockInfo pBlock = blocks.length > 1 ? blocks[blocks.length - 2] : null;
        BlockInfo lastBlock = blocks[blocks.length - 1];

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ?????????
         */
        AddBlockOp op = AddBlockOp.getInstance(cache.get()).setPath(path).setPenultimateBlock(pBlock).setLastBlock(lastBlock);

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ?????????
         */
        logEdit(op);
    }

    public void logUpdateBlocks(String path, INodeFile file, boolean toLogRpcIds) {
        Preconditions.checkArgument(file.isUnderConstruction());
        UpdateBlocksOp op = UpdateBlocksOp.getInstance(cache.get()).setPath(path).setBlocks(file.getBlocks());
        logRpcIds(op, toLogRpcIds);
        logEdit(op);
    }

    /**
     * Add create directory record to edit log
     */
    public void logMkDir(String path, INode newNode) {
        PermissionStatus permissions = newNode.getPermissionStatus();

        // TODO_MA ????????? ????????? ?????? MkdirOp
        MkdirOp op = MkdirOp.getInstance(cache.get()).setInodeId(newNode.getId()).setPath(path)
                .setTimestamp(newNode.getModificationTime()).setPermissionStatus(permissions);

        AclFeature f = newNode.getAclFeature();
        if (f != null) {
            op.setAclEntries(AclStorage.readINodeLogicalAcl(newNode));
        }

        XAttrFeature x = newNode.getXAttrFeature();
        if (x != null) {
            op.setXAttrs(x.getXAttrs());
        }

        // TODO_MA ????????? ????????? ????????????
        logEdit(op);
    }

    /**
     * Add rename record to edit log.
     *
     * The destination should be the file name, not the destination directory.
     * TODO: use String parameters until just before writing to disk
     */
    void logRename(String src, String dst, long timestamp, boolean toLogRpcIds) {
        RenameOldOp op = RenameOldOp.getInstance(cache.get()).setSource(src).setDestination(dst).setTimestamp(timestamp);
        logRpcIds(op, toLogRpcIds);

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ?????????
         */
        logEdit(op);
    }

    /**
     * Add rename record to edit log.
     *
     * The destination should be the file name, not the destination directory.
     */
    void logRename(String src, String dst, long timestamp, boolean toLogRpcIds, Options.Rename... options) {
        RenameOp op = RenameOp.getInstance(cache.get()).setSource(src).setDestination(dst).setTimestamp(timestamp)
                .setOptions(options);
        logRpcIds(op, toLogRpcIds);
        logEdit(op);
    }

    /**
     * Add set replication record to edit log
     */
    void logSetReplication(String src, short replication) {
        SetReplicationOp op = SetReplicationOp.getInstance(cache.get()).setPath(src).setReplication(replication);
        logEdit(op);
    }

    /**
     * Add set storage policy id record to edit log
     */
    void logSetStoragePolicy(String src, byte policyId) {
        SetStoragePolicyOp op = SetStoragePolicyOp.getInstance(cache.get()).setPath(src).setPolicyId(policyId);
        logEdit(op);
    }

    /** Add set namespace quota record to edit log
     *
     * @param src the string representation of the path to a directory
     * @param nsQuota namespace quota
     * @param dsQuota diskspace quota
     */
    void logSetQuota(String src, long nsQuota, long dsQuota) {
        SetQuotaOp op = SetQuotaOp.getInstance(cache.get()).setSource(src).setNSQuota(nsQuota).setDSQuota(dsQuota);
        logEdit(op);
    }

    /** Add set quota by storage type record to edit log */
    void logSetQuotaByStorageType(String src, long dsQuota, StorageType type) {
        SetQuotaByStorageTypeOp op = SetQuotaByStorageTypeOp.getInstance(cache.get()).setSource(src)
                .setQuotaByStorageType(dsQuota, type);
        logEdit(op);
    }

    /**  Add set permissions record to edit log */
    void logSetPermissions(String src, FsPermission permissions) {
        SetPermissionsOp op = SetPermissionsOp.getInstance(cache.get()).setSource(src).setPermissions(permissions);
        logEdit(op);
    }

    /**  Add set owner record to edit log */
    void logSetOwner(String src, String username, String groupname) {
        SetOwnerOp op = SetOwnerOp.getInstance(cache.get()).setSource(src).setUser(username).setGroup(groupname);
        logEdit(op);
    }

    /**
     * concat(trg,src..) log
     */
    void logConcat(String trg, String[] srcs, long timestamp, boolean toLogRpcIds) {
        ConcatDeleteOp op = ConcatDeleteOp.getInstance(cache.get()).setTarget(trg).setSources(srcs).setTimestamp(timestamp);
        logRpcIds(op, toLogRpcIds);
        logEdit(op);
    }

    /**
     * // TODO_MA ????????? ????????? ???????????????????????????????????? edit log
     * Add delete file record to edit log
     */
    void logDelete(String src, long timestamp, boolean toLogRpcIds) {

        // TODO_MA ????????? ????????? ??????
        DeleteOp op = DeleteOp.getInstance(cache.get()).setPath(src).setTimestamp(timestamp);
        logRpcIds(op, toLogRpcIds);

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ????????? ????????????
         */
        logEdit(op);
    }

    /**
     * Add truncate file record to edit log
     */
    void logTruncate(String src, String clientName, String clientMachine, long size, long timestamp, Block truncateBlock) {
        TruncateOp op = TruncateOp.getInstance(cache.get()).setPath(src).setClientName(clientName).setClientMachine(clientMachine)
                .setNewLength(size).setTimestamp(timestamp).setTruncateBlock(truncateBlock);
        logEdit(op);
    }

    /**
     * Add legacy block generation stamp record to edit log
     */
    void logLegacyGenerationStamp(long genstamp) {
        SetGenstampV1Op op = SetGenstampV1Op.getInstance(cache.get()).setGenerationStamp(genstamp);
        logEdit(op);
    }

    /**
     * Add generation stamp record to edit log
     */
    void logGenerationStamp(long genstamp) {
        SetGenstampV2Op op = SetGenstampV2Op.getInstance(cache.get()).setGenerationStamp(genstamp);
        logEdit(op);
    }

    /**
     * Record a newly allocated block ID in the edit log
     */
    void logAllocateBlockId(long blockId) {
        AllocateBlockIdOp op = AllocateBlockIdOp.getInstance(cache.get()).setBlockId(blockId);
        logEdit(op);
    }

    /**
     * Add access time record to edit log
     */
    void logTimes(String src, long mtime, long atime) {
        TimesOp op = TimesOp.getInstance(cache.get()).setPath(src).setModificationTime(mtime).setAccessTime(atime);
        logEdit(op);
    }

    /**
     * Add a create symlink record.
     */
    void logSymlink(String path, String value, long mtime, long atime, INodeSymlink node, boolean toLogRpcIds) {
        SymlinkOp op = SymlinkOp.getInstance(cache.get()).setId(node.getId()).setPath(path).setValue(value)
                .setModificationTime(mtime).setAccessTime(atime).setPermissionStatus(node.getPermissionStatus());
        logRpcIds(op, toLogRpcIds);
        logEdit(op);
    }

    /**
     * log delegation token to edit log
     * @param id DelegationTokenIdentifier
     * @param expiryTime of the token
     */
    void logGetDelegationToken(DelegationTokenIdentifier id, long expiryTime) {
        GetDelegationTokenOp op = GetDelegationTokenOp.getInstance(cache.get()).setDelegationTokenIdentifier(id)
                .setExpiryTime(expiryTime);
        logEdit(op);
    }

    void logRenewDelegationToken(DelegationTokenIdentifier id, long expiryTime) {
        RenewDelegationTokenOp op = RenewDelegationTokenOp.getInstance(cache.get()).setDelegationTokenIdentifier(id)
                .setExpiryTime(expiryTime);
        logEdit(op);
    }

    void logCancelDelegationToken(DelegationTokenIdentifier id) {
        CancelDelegationTokenOp op = CancelDelegationTokenOp.getInstance(cache.get()).setDelegationTokenIdentifier(id);
        logEdit(op);
    }

    void logUpdateMasterKey(DelegationKey key) {
        UpdateMasterKeyOp op = UpdateMasterKeyOp.getInstance(cache.get()).setDelegationKey(key);
        logEdit(op);
    }

    void logReassignLease(String leaseHolder, String src, String newHolder) {
        ReassignLeaseOp op = ReassignLeaseOp.getInstance(cache.get()).setLeaseHolder(leaseHolder).setPath(src)
                .setNewHolder(newHolder);
        logEdit(op);
    }

    /**
     * Log that a snapshot is created.
     * @param snapRoot Root of the snapshot.
     * @param snapName Name of the snapshot.
     * @param toLogRpcIds If it is logging RPC ids.
     * @param mtime The snapshot creation time set by Time.now().
     */
    void logCreateSnapshot(String snapRoot, String snapName, boolean toLogRpcIds, long mtime) {
        CreateSnapshotOp op = CreateSnapshotOp.getInstance(cache.get()).setSnapshotRoot(snapRoot).setSnapshotName(snapName)
                .setSnapshotMTime(mtime);
        logRpcIds(op, toLogRpcIds);
        logEdit(op);
    }

    /**
     * Log that a snapshot is deleted.
     * @param snapRoot Root of the snapshot.
     * @param snapName Name of the snapshot.
     * @param toLogRpcIds If it is logging RPC ids.
     * @param mtime The snapshot deletion time set by Time.now().
     */
    void logDeleteSnapshot(String snapRoot, String snapName, boolean toLogRpcIds, long mtime) {
        DeleteSnapshotOp op = DeleteSnapshotOp.getInstance(cache.get()).setSnapshotRoot(snapRoot).setSnapshotName(snapName)
                .setSnapshotMTime(mtime);
        logRpcIds(op, toLogRpcIds);
        logEdit(op);
    }

    /**
     * Log that a snapshot is renamed.
     * @param path Root of the snapshot.
     * @param snapOldName Old name of the snapshot.
     * @param snapNewName New name the snapshot will be renamed to.
     * @param toLogRpcIds If it is logging RPC ids.
     * @param mtime The snapshot modification time set by Time.now().
     */
    void logRenameSnapshot(String path, String snapOldName, String snapNewName, boolean toLogRpcIds, long mtime) {
        RenameSnapshotOp op = RenameSnapshotOp.getInstance(cache.get()).setSnapshotRoot(path).setSnapshotOldName(snapOldName)
                .setSnapshotNewName(snapNewName).setSnapshotMTime(mtime);
        logRpcIds(op, toLogRpcIds);
        logEdit(op);
    }

    void logAllowSnapshot(String path) {
        AllowSnapshotOp op = AllowSnapshotOp.getInstance(cache.get()).setSnapshotRoot(path);
        logEdit(op);
    }

    void logDisallowSnapshot(String path) {
        DisallowSnapshotOp op = DisallowSnapshotOp.getInstance(cache.get()).setSnapshotRoot(path);
        logEdit(op);
    }

    /**
     * Log a CacheDirectiveInfo returned from
     * {@link CacheManager#addDirective(CacheDirectiveInfo, FSPermissionChecker)}
     */
    void logAddCacheDirectiveInfo(CacheDirectiveInfo directive, boolean toLogRpcIds) {
        AddCacheDirectiveInfoOp op = AddCacheDirectiveInfoOp.getInstance(cache.get()).setDirective(directive);
        logRpcIds(op, toLogRpcIds);
        logEdit(op);
    }

    void logModifyCacheDirectiveInfo(CacheDirectiveInfo directive, boolean toLogRpcIds) {
        ModifyCacheDirectiveInfoOp op = ModifyCacheDirectiveInfoOp.getInstance(cache.get()).setDirective(directive);
        logRpcIds(op, toLogRpcIds);
        logEdit(op);
    }

    void logRemoveCacheDirectiveInfo(Long id, boolean toLogRpcIds) {
        RemoveCacheDirectiveInfoOp op = RemoveCacheDirectiveInfoOp.getInstance(cache.get()).setId(id);
        logRpcIds(op, toLogRpcIds);
        logEdit(op);
    }

    void logAddCachePool(CachePoolInfo pool, boolean toLogRpcIds) {
        AddCachePoolOp op = AddCachePoolOp.getInstance(cache.get()).setPool(pool);
        logRpcIds(op, toLogRpcIds);
        logEdit(op);
    }

    void logModifyCachePool(CachePoolInfo info, boolean toLogRpcIds) {
        ModifyCachePoolOp op = ModifyCachePoolOp.getInstance(cache.get()).setInfo(info);
        logRpcIds(op, toLogRpcIds);
        logEdit(op);
    }

    void logRemoveCachePool(String poolName, boolean toLogRpcIds) {
        RemoveCachePoolOp op = RemoveCachePoolOp.getInstance(cache.get()).setPoolName(poolName);
        logRpcIds(op, toLogRpcIds);
        logEdit(op);
    }

    void logStartRollingUpgrade(long startTime) {
        RollingUpgradeStartOp op = RollingUpgradeStartOp.getInstance(cache.get());
        op.setTime(startTime);
        logEdit(op);
    }

    void logFinalizeRollingUpgrade(long finalizeTime) {
        RollingUpgradeOp op = RollingUpgradeFinalizeOp.getInstance(cache.get());
        op.setTime(finalizeTime);
        logEdit(op);
    }

    void logSetAcl(String src, List<AclEntry> entries) {
        final SetAclOp op = SetAclOp.getInstance(cache.get());
        op.src = src;
        op.aclEntries = entries;
        logEdit(op);
    }

    void logSetXAttrs(String src, List<XAttr> xAttrs, boolean toLogRpcIds) {
        final SetXAttrOp op = SetXAttrOp.getInstance(cache.get());
        op.src = src;
        op.xAttrs = xAttrs;
        logRpcIds(op, toLogRpcIds);
        logEdit(op);
    }

    void logRemoveXAttrs(String src, List<XAttr> xAttrs, boolean toLogRpcIds) {
        final RemoveXAttrOp op = RemoveXAttrOp.getInstance(cache.get());
        op.src = src;
        op.xAttrs = xAttrs;
        logRpcIds(op, toLogRpcIds);
        logEdit(op);
    }

    void logAddErasureCodingPolicy(ErasureCodingPolicy ecPolicy, boolean toLogRpcIds) {
        AddErasureCodingPolicyOp op = AddErasureCodingPolicyOp.getInstance(cache.get());
        op.setErasureCodingPolicy(ecPolicy);
        logRpcIds(op, toLogRpcIds);
        logEdit(op);
    }

    void logEnableErasureCodingPolicy(String ecPolicyName, boolean toLogRpcIds) {
        EnableErasureCodingPolicyOp op = EnableErasureCodingPolicyOp.getInstance(cache.get());
        op.setErasureCodingPolicy(ecPolicyName);
        logRpcIds(op, toLogRpcIds);
        logEdit(op);
    }

    void logDisableErasureCodingPolicy(String ecPolicyName, boolean toLogRpcIds) {
        DisableErasureCodingPolicyOp op = DisableErasureCodingPolicyOp.getInstance(cache.get());
        op.setErasureCodingPolicy(ecPolicyName);
        logRpcIds(op, toLogRpcIds);
        logEdit(op);
    }

    void logRemoveErasureCodingPolicy(String ecPolicyName, boolean toLogRpcIds) {
        RemoveErasureCodingPolicyOp op = RemoveErasureCodingPolicyOp.getInstance(cache.get());
        op.setErasureCodingPolicy(ecPolicyName);
        logRpcIds(op, toLogRpcIds);
        logEdit(op);
    }

    /**
     * Get all the journals this edit log is currently operating on.
     */
    List<JournalAndStream> getJournals() {
        // The list implementation is CopyOnWriteArrayList,
        // so we don't need to synchronize this method.
        return journalSet.getAllJournalStreams();
    }

    /**
     * Used only by tests.
     */
    @VisibleForTesting
    public JournalSet getJournalSet() {
        return journalSet;
    }

    @VisibleForTesting
    synchronized void setJournalSetForTesting(JournalSet js) {
        this.journalSet = js;
    }

    /**
     * Used only by tests.
     */
    @VisibleForTesting
    void setMetricsForTests(NameNodeMetrics metrics) {
        this.metrics = metrics;
    }

    /**
     * Return a manifest of what finalized edit logs are available
     */
    public synchronized RemoteEditLogManifest getEditLogManifest(long fromTxId) throws IOException {
        return journalSet.getEditLogManifest(fromTxId);
    }

    /**
     * Finalizes the current edit log and opens a new log segment.
     *
     * @param layoutVersion The layout version of the new edit log segment.
     * @return the transaction id of the BEGIN_LOG_SEGMENT transaction in the new log.
     */
    synchronized long rollEditLog(int layoutVersion) throws IOException {
        LOG.info("Rolling edit logs");

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ????????? ?????? edits_inprogress_starttxid ??? edits_firsttxid_lasttxid ??????
         */
        endCurrentLogSegment(true);

        long nextTxId = getLastWrittenTxId() + 1;

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ????????? ?????????????????? logsegment: edits_inprogress_starttxid
         */
        startLogSegmentAndWriteHeaderTxn(nextTxId, layoutVersion);

        assert curSegmentTxId == nextTxId;
        return nextTxId;
    }

    /**
     * Remote namenode just has started a log segment, start log segment locally.
     */
    public synchronized void startLogSegment(long txid, boolean abortCurrentLogSegment, int layoutVersion) throws IOException {
        LOG.info("Started a new log segment at txid " + txid);
        if (isSegmentOpen()) {
            if (getLastWrittenTxId() == txid - 1) {
                //In sync with the NN, so end and finalize the current segment`
                endCurrentLogSegment(false);
            } else {
                //Missed some transactions: probably lost contact with NN temporarily.
                final String mess = "Cannot start a new log segment at txid " + txid + " since only up to txid " + getLastWrittenTxId() + " have been written in the log segment starting at " + getCurSegmentTxId() + ".";
                if (abortCurrentLogSegment) {
                    //Mark the current segment as aborted.
                    LOG.warn(mess);
                    abortCurrentLogSegment();
                } else {
                    throw new IOException(mess);
                }
            }
        }
        setNextTxId(txid);
        startLogSegment(txid, layoutVersion);
    }

    /**
     * Start writing to the log segment with the given txid.
     * Transitions from BETWEEN_LOG_SEGMENTS state to IN_LOG_SEGMENT state.
     */
    private void startLogSegment(final long segmentTxId, int layoutVersion) throws IOException {
        assert Thread.holdsLock(this);

        LOG.info("Starting log segment at " + segmentTxId);
        Preconditions.checkArgument(segmentTxId > 0, "Bad txid: %s", segmentTxId);
        Preconditions.checkState(state == State.BETWEEN_LOG_SEGMENTS, "Bad state: %s", state);
        Preconditions.checkState(segmentTxId > curSegmentTxId,
                "Cannot start writing to log segment " + segmentTxId + " when previous log segment started at " + curSegmentTxId
        );
        Preconditions.checkArgument(segmentTxId == txid + 1,
                "Cannot start log segment at txid %s when next expected " + "txid is %s", segmentTxId, txid + 1
        );

        numTransactions = 0;
        totalTimeTransactions = 0;
        numTransactionsBatchedInSync.reset();

        // TODO no need to link this back to storage anymore! See HDFS-2174.
        storage.attemptRestoreRemovedStorage();

        try {
            /*************************************************
             * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
             *  ????????? ????????? editLogStream
             *  editLogStream = JournalSetOutputStream
             */
            editLogStream = journalSet.startLogSegment(segmentTxId, layoutVersion);
        } catch (IOException ex) {
            final String msg = "Unable to start log segment " + segmentTxId + ": too few journals successfully started.";
            LOG.error(msg, ex);
            synchronized (journalSetLock) {
                IOUtils.cleanupWithLogger(LOG, journalSet);
            }
            terminate(1, msg);
        }

        // TODO_MA ????????? ????????? ?????? current segmentTxId ????????? segmentTxId
        curSegmentTxId = segmentTxId;

        // TODO_MA ????????? ????????? ????????????
        state = State.IN_SEGMENT;
    }

    /*************************************************
     * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
     *  ????????? ?????? inprogress editlog file ??? LogSegment
     */
    synchronized void startLogSegmentAndWriteHeaderTxn(final long segmentTxId, int layoutVersion) throws IOException {

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ????????? current ???????????? edits_inprogress_startTxId ?????? EditLogFileOutputStream ?????????
         */
        startLogSegment(segmentTxId, layoutVersion);

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ????????? ???????????????????????? ???24, StartLogSegmentOp???
         */
        logEdit(LogSegmentOp.getInstance(cache.get(), FSEditLogOpCodes.OP_START_LOG_SEGMENT));

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ????????? ??????????????????
         */
        logSync();
    }

    /**
     * // TODO_MA ????????? ????????? ????????? curSegmentTxid ??? lastTxId ??????????????????????????????
     * // TODO_MA ????????? ????????? ????????????????????? BETWEEN_LOG_SEGMENTS
     * Finalize the current log segment.
     * Transitions from IN_SEGMENT state to BETWEEN_LOG_SEGMENTS state.
     * // TODO_MA ????????? ????????? 1???endCurrentLogSegment() ?????? edits_firstTxId_lastTxId
     * // TODO_MA ????????? ????????? 2???FSEditLog ????????? ????????? BETWEEN_LOG_SEGMENTS ??????
     */
    public synchronized void endCurrentLogSegment(boolean writeEndTxn) {
        LOG.info("Ending log segment " + curSegmentTxId + ", " + getLastWrittenTxId());
        Preconditions.checkState(isSegmentOpen(), "Bad state: %s", state);

        // TODO_MA ????????? ????????? LogSegment ?????????????????????????????????
        if (writeEndTxn) {
            logEdit(LogSegmentOp.getInstance(cache.get(), FSEditLogOpCodes.OP_END_LOG_SEGMENT));
        }

        // TODO_MA ????????? ????????? ?????? flush
        // always sync to ensure all edits are flushed.
        logSyncAll();

        printStatistics(true);

        // TODO_MA ????????? ????????? ??????????????? txid ??? ?????? txid ????????????????????????
        final long lastTxId = getLastWrittenTxId();
        final long lastSyncedTxId = getSyncTxId();
        Preconditions.checkArgument(lastTxId == lastSyncedTxId,
                "LastWrittenTxId %s is expected to be the same as lastSyncedTxId %s", lastTxId, lastSyncedTxId
        );
        try {
            /*************************************************
             * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
             *  ????????? ?????? LogSegment ?????????
             *   curSegmentTxid ??? lastTxId ?????????
             */
            journalSet.finalizeLogSegment(curSegmentTxId, lastTxId);
            editLogStream = null;
        } catch (IOException e) {
            //All journals have failed, it will be handled in logSync.
        }

        // TODO_MA ????????? ????????? ??????????????? BETWEEN_LOG_SEGMENTS
        state = State.BETWEEN_LOG_SEGMENTS;
    }

    /**
     * Abort all current logs. Called from the backup node.
     */
    synchronized void abortCurrentLogSegment() {
        try {
            //Check for null, as abort can be called any time.
            if (editLogStream != null) {
                editLogStream.abort();
                editLogStream = null;
                state = State.BETWEEN_LOG_SEGMENTS;
            }
        } catch (IOException e) {
            LOG.warn("All journals failed to abort", e);
        }
    }

    /**
     * Archive any log files that are older than the given txid.
     *
     * If the edit log is not open for write, then this call returns with no
     * effect.
     */
    @Override
    public synchronized void purgeLogsOlderThan(final long minTxIdToKeep) {
        // Should not purge logs unless they are open for write.
        // This prevents the SBN from purging logs on shared storage, for example.
        if (!isOpenForWrite()) {
            return;
        }

        assert curSegmentTxId == HdfsServerConstants.INVALID_TXID || // on format this is no-op
                minTxIdToKeep <= curSegmentTxId : "cannot purge logs older than txid " + minTxIdToKeep + " when current segment starts at " + curSegmentTxId;
        if (minTxIdToKeep == 0) {
            return;
        }

        // This could be improved to not need synchronization. But currently,
        // journalSet is not threadsafe, so we need to synchronize this method.
        try {
            journalSet.purgeLogsOlderThan(minTxIdToKeep);
        } catch (IOException ex) {
            //All journals have failed, it will be handled in logSync.
        }
    }


    /**
     * The actual sync activity happens while not synchronized on this object.
     * Thus, synchronized activities that require that they are not concurrent
     * with file operations should wait for any running sync to finish.
     */
    synchronized void waitForSyncToFinish() {
        while (isSyncRunning) {
            try {
                wait(1000);
            } catch (InterruptedException ie) {
            }
        }
    }

    /**
     * Return the txid of the last synced transaction.
     */
    public synchronized long getSyncTxId() {
        return synctxid;
    }


    // sets the initial capacity of the flush buffer.
    synchronized void setOutputBufferCapacity(int size) {
        journalSet.setOutputBufferCapacity(size);
    }

    /**
     * Create (or find if already exists) an edit output stream, which
     * streams journal records (edits) to the specified backup node.<br>
     *
     * The new BackupNode will start receiving edits the next time this
     * NameNode's logs roll.
     *
     * @param bnReg the backup node registration information.
     * @param nnReg this (active) name-node registration.
     * @throws IOException
     */
    synchronized void registerBackupNode(NamenodeRegistration bnReg, // backup node
                                         NamenodeRegistration nnReg) // active name-node
            throws IOException {
        if (bnReg.isRole(NamenodeRole.CHECKPOINT)) return; // checkpoint node does not stream edits

        JournalManager jas = findBackupJournal(bnReg);
        if (jas != null) {
            // already registered
            LOG.info("Backup node " + bnReg + " re-registers");
            return;
        }

        LOG.info("Registering new backup node: " + bnReg);
        BackupJournalManager bjm = new BackupJournalManager(bnReg, nnReg);
        synchronized (journalSetLock) {
            journalSet.add(bjm, false);
        }
    }

    synchronized void releaseBackupStream(NamenodeRegistration registration) throws IOException {
        BackupJournalManager bjm = this.findBackupJournal(registration);
        if (bjm != null) {
            LOG.info("Removing backup journal " + bjm);
            synchronized (journalSetLock) {
                journalSet.remove(bjm);
            }
        }
    }

    /**
     * Find the JournalAndStream associated with this BackupNode.
     *
     * @return null if it cannot be found
     */
    private synchronized BackupJournalManager findBackupJournal(NamenodeRegistration bnReg) {
        for (JournalManager bjm : journalSet.getJournalManagers()) {
            if ((bjm instanceof BackupJournalManager) && ((BackupJournalManager) bjm).matchesRegistration(bnReg)) {
                return (BackupJournalManager) bjm;
            }
        }
        return null;
    }

    /** Write the batch of edits to edit log. */
    public synchronized void journal(long firstTxId, int numTxns, byte[] data) {
        final long expectedTxId = getLastWrittenTxId() + 1;
        Preconditions.checkState(firstTxId == expectedTxId, "received txid batch starting at %s but expected txid %s", firstTxId,
                expectedTxId
        );
        setNextTxId(firstTxId + numTxns - 1);

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ?????????
         */
        logEdit(data.length, data);

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ?????????
         */
        logSync();
    }

    /**
     * Write an operation to the edit log. Do not sync to persistent
     * store yet.
     */
    synchronized void logEdit(final int length, final byte[] data) {

        // TODO_MA ????????? ????????? ????????????
        long start = beginTransaction();

        try {
            /*************************************************
             * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
             *  ????????? ?????????
             */
            editLogStream.writeRaw(data, 0, length);
        } catch (IOException ex) {
            // All journals have failed, it will be handled in logSync.
        }

        // TODO_MA ????????? ????????? ????????????
        endTransaction(start);
    }

    /**
     * Run recovery on all journals to recover any unclosed segments
     */
    synchronized void recoverUnclosedStreams() {
        Preconditions.checkState(state == State.BETWEEN_LOG_SEGMENTS, "May not recover segments - wrong state: %s", state);
        try {
            journalSet.recoverUnfinalizedSegments();
        } catch (IOException ex) {
            // All journals have failed, it is handled in logSync.
            // TODO: are we sure this is OK?
        }
    }

    public long getSharedLogCTime() throws IOException {
        for (JournalAndStream jas : journalSet.getAllJournalStreams()) {
            if (jas.isShared()) {
                return jas.getManager().getJournalCTime();
            }
        }
        throw new IOException("No shared log found.");
    }

    public synchronized void doPreUpgradeOfSharedLog() throws IOException {
        for (JournalAndStream jas : journalSet.getAllJournalStreams()) {
            if (jas.isShared()) {
                jas.getManager().doPreUpgrade();
            }
        }
    }

    public synchronized void doUpgradeOfSharedLog() throws IOException {
        for (JournalAndStream jas : journalSet.getAllJournalStreams()) {
            if (jas.isShared()) {
                jas.getManager().doUpgrade(storage);
            }
        }
    }

    public synchronized void doFinalizeOfSharedLog() throws IOException {
        for (JournalAndStream jas : journalSet.getAllJournalStreams()) {
            if (jas.isShared()) {
                jas.getManager().doFinalize();
            }
        }
    }

    public synchronized boolean canRollBackSharedLog(StorageInfo prevStorage, int targetLayoutVersion) throws IOException {
        for (JournalAndStream jas : journalSet.getAllJournalStreams()) {
            if (jas.isShared()) {
                return jas.getManager().canRollBack(storage, prevStorage, targetLayoutVersion);
            }
        }
        throw new IOException("No shared log found.");
    }

    public synchronized void doRollback() throws IOException {
        for (JournalAndStream jas : journalSet.getAllJournalStreams()) {
            if (jas.isShared()) {
                jas.getManager().doRollback();
            }
        }
    }

    public synchronized void discardSegments(long markerTxid) throws IOException {
        for (JournalAndStream jas : journalSet.getAllJournalStreams()) {
            jas.getManager().discardSegments(markerTxid);
        }
    }

    @Override
    public void selectInputStreams(Collection<EditLogInputStream> streams, long fromTxId, boolean inProgressOk,
                                   boolean onlyDurableTxns) throws IOException {
        journalSet.selectInputStreams(streams, fromTxId, inProgressOk, onlyDurableTxns);
    }

    public Collection<EditLogInputStream> selectInputStreams(long fromTxId, long toAtLeastTxId) throws IOException {
        return selectInputStreams(fromTxId, toAtLeastTxId, null, true, false);
    }

    public Collection<EditLogInputStream> selectInputStreams(long fromTxId, long toAtLeastTxId, MetaRecoveryContext recovery,
                                                             boolean inProgressOK) throws IOException {
        return selectInputStreams(fromTxId, toAtLeastTxId, recovery, inProgressOK, false);
    }

    /**
     * Select a list of input streams.
     *
     * @param fromTxId first transaction in the selected streams
     * @param toAtLeastTxId the selected streams must contain this transaction
     * @param recovery recovery context
     * @param inProgressOk set to true if in-progress streams are OK
     * @param onlyDurableTxns set to true if streams are bounded
     *                        by the durable TxId
     */
    public Collection<EditLogInputStream> selectInputStreams(long fromTxId, long toAtLeastTxId, MetaRecoveryContext recovery,
                                                             boolean inProgressOk, boolean onlyDurableTxns) throws IOException {

        List<EditLogInputStream> streams = new ArrayList<EditLogInputStream>();
        synchronized (journalSetLock) {
            Preconditions.checkState(journalSet.isOpen(), "Cannot call " + "selectInputStreams() on closed FSEditLog");
            selectInputStreams(streams, fromTxId, inProgressOk, onlyDurableTxns);
        }

        try {
            checkForGaps(streams, fromTxId, toAtLeastTxId, inProgressOk);
        } catch (IOException e) {
            if (recovery != null) {
                // If recovery mode is enabled, continue loading even if we know we
                // can't load up to toAtLeastTxId.
                LOG.error("Exception while selecting input streams", e);
            } else {
                closeAllStreams(streams);
                throw e;
            }
        }
        return streams;
    }

    /**
     * Check for gaps in the edit log input stream list.
     * Note: we're assuming that the list is sorted and that txid ranges don't
     * overlap.  This could be done better and with more generality with an
     * interval tree.
     */
    private void checkForGaps(List<EditLogInputStream> streams, long fromTxId, long toAtLeastTxId,
                              boolean inProgressOk) throws IOException {
        Iterator<EditLogInputStream> iter = streams.iterator();
        long txId = fromTxId;
        while (true) {
            if (txId > toAtLeastTxId) return;
            if (!iter.hasNext()) break;
            EditLogInputStream elis = iter.next();
            if (elis.getFirstTxId() > txId) break;
            long next = elis.getLastTxId();
            if (next == HdfsServerConstants.INVALID_TXID) {
                if (!inProgressOk) {
                    throw new RuntimeException(
                            "inProgressOk = false, but " + "selectInputStreams returned an in-progress edit " + "log input stream (" + elis + ")");
                }
                // We don't know where the in-progress stream ends.
                // It could certainly go all the way up to toAtLeastTxId.
                return;
            }
            txId = next + 1;
        }
        throw new IOException(String.format(
                "Gap in transactions. Expected to " + "be able to read up until at least txid %d but unable to find any " + "edit logs containing txid %d",
                toAtLeastTxId, txId
        ));
    }

    /**
     * Close all the streams in a collection
     * @param streams The list of streams to close
     */
    static void closeAllStreams(Iterable<EditLogInputStream> streams) {
        for (EditLogInputStream s : streams) {
            IOUtils.closeStream(s);
        }
    }

    /**
     * Retrieve the implementation class for a Journal scheme.
     * @param conf The configuration to retrieve the information from
     * @param uriScheme The uri scheme to look up.
     * @return the class of the journal implementation
     * @throws IllegalArgumentException if no class is configured for uri
     */
    static Class<? extends JournalManager> getJournalClass(Configuration conf, String uriScheme) {

        // TODO_MA ????????? ????????? dfs.namenode.edits.journal-plugin.uriScheme
        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ?????????
         *  <property>
         *         <name>dfs.namenode.edits.journal-plugin.qjournal</name>
         *         <value>org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager</value>
         *  </property>
         */
        String key = DFSConfigKeys.DFS_NAMENODE_EDITS_PLUGIN_PREFIX + "." + uriScheme;
        Class<? extends JournalManager> clazz = null;
        try {
            clazz = conf.getClass(key, null, JournalManager.class);
        } catch (RuntimeException re) {
            throw new IllegalArgumentException("Invalid class specified for " + uriScheme, re);
        }

        if (clazz == null) {
            LOG.warn("No class configured for " + uriScheme + ", " + key + " is empty");
            throw new IllegalArgumentException("No class configured for " + uriScheme);
        }
        return clazz;
    }

    /**
     * Construct a custom journal manager.
     * The class to construct is taken from the configuration.
     * @param uri Uri to construct
     * @return The constructed journal manager
     * @throws IllegalArgumentException if no class is configured for uri
     */
    @VisibleForTesting
    JournalManager createJournal(URI uri) {

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ????????? ????????? schema ??????????????? JournalManager ??????
         *  qjournal ?????? = QuorumJournalManager
         */
        Class<? extends JournalManager> clazz = getJournalClass(conf, uri.getScheme());

        try {
            // TODO_MA ????????? ????????? ?????????????????? QuorumJournalManager
            Constructor<? extends JournalManager> cons = clazz.getConstructor(Configuration.class, URI.class, NamespaceInfo.class,
                    String.class
            );
            String nameServiceId = conf.get(DFSConfigKeys.DFS_NAMESERVICE_ID);

            /*************************************************
             * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
             *  ????????? ????????????????????? ??????????????????????????????
             */
            return cons.newInstance(conf, uri, storage.getNamespaceInfo(), nameServiceId);
        } catch (NoSuchMethodException ne) {
            try {
                Constructor<? extends JournalManager> cons = clazz.getConstructor(Configuration.class, URI.class,
                        NamespaceInfo.class
                );
                return cons.newInstance(conf, uri, storage.getNamespaceInfo());
            } catch (Exception e) {
                throw new IllegalArgumentException("Unable to construct journal, " + uri, e);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to construct journal, " + uri, e);
        }
    }

    @VisibleForTesting
    // needed by async impl to restart thread when edit log is replaced by a
    // spy because a spy is a shallow copy
    public void restart() {
    }

    /**
     * Return total number of syncs happened on this edit log.
     * @return long - count
     */
    public long getTotalSyncCount() {
        // Avoid NPE as possible.
        if (editLogStream == null) {
            return 0;
        }
        long count = 0;
        try {
            count = editLogStream.getNumSync();
        } catch (NullPointerException ignore) {
            // This method is used for metrics, so we don't synchronize it.
            // Therefore NPE can happen even if there is a null check before.
        }
        return count;
    }
}
