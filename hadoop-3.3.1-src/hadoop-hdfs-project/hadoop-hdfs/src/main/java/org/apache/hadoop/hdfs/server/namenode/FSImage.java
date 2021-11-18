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

import static org.apache.hadoop.util.Time.now;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.RollingUpgradeStartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.FormatConfirmable;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Storage.StorageState;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.FSImageStorageInspector.FSImageFile;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.protocol.CheckpointCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.log.LogThrottlingHelper;
import org.apache.hadoop.log.LogThrottlingHelper.LogAction;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Time;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;

/**
 * FSImage handles checkpointing and logging of the namespace edits.
 * // TODO_MA 马中华 注释： FSImage 处理命名空间编辑的检查点和日志记录。
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSImage implements Closeable {

    public static final Logger LOG = LoggerFactory.getLogger(FSImage.class.getName());

    /**
     * Priority of the FSImageSaver shutdown hook: {@value}.
     */
    public static final int SHUTDOWN_HOOK_PRIORITY = 10;

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：
     */
    protected FSEditLog editLog = null;
    private boolean isUpgradeFinalized = false;

    // If true, then image corruption was detected. The NameNode process will
    // exit immediately after saving the image.
    private AtomicBoolean exitAfterSave = new AtomicBoolean(false);

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：
     */
    protected NNStorage storage;

    /**
     * The last transaction ID that was either loaded from an image
     * or loaded by loading edits files.
     */
    protected long lastAppliedTxId = 0;

    final private Configuration conf;

    protected NNStorageRetentionManager archivalManager;

    /**
     * The collection of newly added storage directories. These are partially
     * formatted then later fully populated along with a VERSION file.
     * For HA, the second part is done when the next checkpoint is saved.
     * This set will be cleared once a VERSION file is created.
     * For non-HA, a new fsimage will be locally generated along with a new
     * VERSION file. This set is not used for non-HA mode.
     */
    private Set<StorageDirectory> newDirs = null;

    /* Used to make sure there are no concurrent checkpoints for a given txid
     * The checkpoint here could be one of the following operations.
     * a. checkpoint when NN is in standby.
     * b. admin saveNameSpace operation.
     * c. download checkpoint file from any remote checkpointer.
     */
    private final Set<Long> currentlyCheckpointing = Collections.<Long>synchronizedSet(new HashSet<Long>());

    /** Limit logging about edit loading to every 5 seconds max. */
    private static final long LOAD_EDIT_LOG_INTERVAL_MS = 5000;
    private final LogThrottlingHelper loadEditLogHelper = new LogThrottlingHelper(LOAD_EDIT_LOG_INTERVAL_MS);

    /**
     * Construct an FSImage
     * @param conf Configuration
     * @throws IOException if default directories are invalid.
     */
    public FSImage(Configuration conf) throws IOException {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         *  1、 dfs.namenode.name.dir
         *  2、 dfs.namenode.shared.edits.dir
         */
        this(conf, FSNamesystem.getNamespaceDirs(conf), FSNamesystem.getNamespaceEditsDirs(conf));
    }

    /**
     * Construct the FSImage. Set the default checkpoint directories.
     *
     * Setup storage and initialize the edit log.
     *
     * @param conf Configuration
     * @param imageDirs Directories the image can be stored in.
     * @param editsDirs Directories the editlog can be stored in.
     * @throws IOException if directories are invalid.
     */
    protected FSImage(Configuration conf, Collection<URI> imageDirs, List<URI> editsDirs) throws IOException {
        this.conf = conf;

        // TODO_MA 马中华 注释： NNStorage 负责管理 HDFS 的 StorageDirectories
        storage = new NNStorage(conf, imageDirs, editsDirs);

        // TODO_MA 马中华 注释： dfs.namenode.name.dir.restore = false
        // TODO_MA 马中华 注释： 如果需要恢复以前失败的 dfs.NameNode.name.dir，则更改为 True
        if (conf.getBoolean(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_KEY,
                DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_DEFAULT
        )) {
            storage.setRestoreFailedStorage(true);
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 创建的是：FSEditLogAsync
         */
        this.editLog = FSEditLog.newInstance(conf, storage, editsDirs);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        archivalManager = new NNStorageRetentionManager(conf, storage, editLog);

        // TODO_MA 马中华 注释： dfs.image.parallel.load = false
        FSImageFormatProtobuf.initParallelLoad(conf);
    }

    void format(FSNamesystem fsn, String clusterId, boolean force) throws IOException {
        long fileCount = fsn.getFilesTotal();
        // Expect 1 file, which is the root inode
        Preconditions.checkState(fileCount == 1,
                "FSImage.format should be called with an uninitialized namesystem, has " + fileCount + " files"
        );

        // TODO_MA 马中华 注释： namespace 元数据
        NamespaceInfo ns = NNStorage.newNamespaceInfo();
        LOG.info("Allocated new BlockPoolId: " + ns.getBlockPoolID());
        ns.clusterID = clusterId;

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 其实格式化，就做两件事
         *  1、创建 VERSION 文件
         *  2、创建 seen_txid 保存 txid
         */
        storage.format(ns);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        editLog.formatNonFileJournals(ns, force);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        saveFSImageInAllDirs(fsn, 0);
    }

    /**
     * Check whether the storage directories and non-file journals exist.
     * If running in interactive mode, will prompt the user for each
     * directory to allow them to format anyway. Otherwise, returns
     * false, unless 'force' is specified.
     *
     * @param force if true, format regardless of whether dirs exist
     * @param interactive prompt the user when a dir exists
     * @return true if formatting should proceed
     * @throws IOException if some storage cannot be accessed
     */
    boolean confirmFormat(boolean force, boolean interactive) throws IOException {
        List<FormatConfirmable> confirms = Lists.newArrayList();
        for (StorageDirectory sd : storage.dirIterable(null)) {
            confirms.add(sd);
        }

        confirms.addAll(editLog.getFormatConfirmables());
        return Storage.confirmFormat(confirms, force, interactive);
    }

    /**
     * Analyze storage directories.
     * Recover from previous transitions if required.
     * Perform fs state transition if necessary depending on the namespace info.
     * Read storage info.
     *
     * @throws IOException
     * @return true if the image needs to be saved or false otherwise
     */
    boolean recoverTransitionRead(StartupOption startOpt, FSNamesystem target,
                                  MetaRecoveryContext recovery) throws IOException {
        assert startOpt != StartupOption.FORMAT : "NameNode formatting should be performed before reading the image";

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 获取 fsimage 存储目录
         */
        Collection<URI> imageDirs = storage.getImageDirectories();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 获取 edits 存储目录
         */
        Collection<URI> editsDirs = editLog.getEditURIs();

        // none of the data dirs exist
        if ((imageDirs.size() == 0 || editsDirs.size() == 0) && startOpt != StartupOption.IMPORT)
            throw new IOException("All specified directories are not accessible or do not exist.");

        // 1. For each data directory calculate its state and
        // check whether all is consistent before transitioning.
        Map<StorageDirectory, StorageState> dataDirStates = new HashMap<StorageDirectory, StorageState>();
        boolean isFormatted = recoverStorageDirs(startOpt, storage, dataDirStates);

        if (LOG.isTraceEnabled()) {
            LOG.trace("Data dir states:\n  " + Joiner.on("\n  ").withKeyValueSeparator(": ").join(dataDirStates));
        }

        if (!isFormatted && startOpt != StartupOption.ROLLBACK && startOpt != StartupOption.IMPORT) {
            throw new IOException("NameNode is not formatted.");
        }

        int layoutVersion = storage.getLayoutVersion();
        if (startOpt == StartupOption.METADATAVERSION) {
            System.out.println("HDFS Image Version: " + layoutVersion);
            System.out.println("Software format version: " + HdfsServerConstants.NAMENODE_LAYOUT_VERSION);
            return false;
        }

        if (layoutVersion < Storage.LAST_PRE_UPGRADE_LAYOUT_VERSION) {
            NNStorage.checkVersionUpgradable(storage.getLayoutVersion());
        }
        if (startOpt != StartupOption.UPGRADE && startOpt != StartupOption.UPGRADEONLY && !RollingUpgradeStartupOption.STARTED.matches(
                startOpt) && layoutVersion < Storage.LAST_PRE_UPGRADE_LAYOUT_VERSION && layoutVersion != HdfsServerConstants.NAMENODE_LAYOUT_VERSION) {
            throw new IOException(
                    "\nFile system image contains an old layout version " + storage.getLayoutVersion() + ".\nAn upgrade to version " + HdfsServerConstants.NAMENODE_LAYOUT_VERSION + " is required.\n" + "Please restart NameNode with the \"" + RollingUpgradeStartupOption.STARTED.getOptionString() + "\" option if a rolling upgrade is already started;" + " or restart NameNode with the \"" + StartupOption.UPGRADE.getName() + "\" option to start" + " a new upgrade.");
        }

        storage.processStartupOptionsForUpgrade(startOpt, layoutVersion);

        // 2. Format unformatted dirs.
        for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext(); ) {
            StorageDirectory sd = it.next();
            StorageState curState = dataDirStates.get(sd);
            switch (curState) {
                case NON_EXISTENT:
                    throw new IOException(StorageState.NON_EXISTENT + " state cannot be here");
                case NOT_FORMATTED:
                    // Create a dir structure, but not the VERSION file. The presence of
                    // VERSION is checked in the inspector's needToSave() method and
                    // saveNamespace is triggered if it is absent. This will bring
                    // the storage state uptodate along with a new VERSION file.
                    // If HA is enabled, NNs start up as standby so saveNamespace is not
                    // triggered.
                    LOG.info("Storage directory " + sd.getRoot() + " is not formatted.");
                    LOG.info("Formatting ...");
                    sd.clearDirectory(); // create empty current dir
                    // For non-HA, no further action is needed here, as saveNamespace will
                    // take care of the rest.
                    if (!target.isHaEnabled()) {
                        continue;
                    }
                    // If HA is enabled, save the dirs to create a version file later when
                    // a checkpoint image is saved.
                    if (newDirs == null) {
                        newDirs = new HashSet<StorageDirectory>();
                    }
                    newDirs.add(sd);
                    break;
                default:
                    break;
            }
        }

        // 3. Do transitions
        switch (startOpt) {
            case UPGRADE:
            case UPGRADEONLY:
                doUpgrade(target);
                return false; // upgrade saved image already
            case IMPORT:
                doImportCheckpoint(target);
                return false; // import checkpoint saved image already
            case ROLLBACK:
                throw new AssertionError(
                        "Rollback is now a standalone command, " + "NameNode should not be starting with this option.");
            case REGULAR:
            default:
                // just load the image
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 加载 Namenode 磁盘元数据 到 内存
         */
        return loadFSImage(target, startOpt, recovery);
    }

    /**
     * Create a VERSION file in the newly added storage directories.
     */
    private void initNewDirs() {
        if (newDirs == null) {
            return;
        }
        for (StorageDirectory sd : newDirs) {
            try {
                storage.writeProperties(sd);
                LOG.info("Wrote VERSION in the new storage, " + sd.getCurrentDir());
            } catch (IOException e) {
                // Failed to create a VERSION file. Report the error.
                storage.reportErrorOnFile(sd.getVersionFile());
            }
        }
        newDirs.clear();
        newDirs = null;
    }

    /**
     * For each storage directory, performs recovery of incomplete transitions
     * (eg. upgrade, rollback, checkpoint) and inserts the directory's storage
     * state into the dataDirStates map.
     * @param dataDirStates output of storage directory states
     * @return true if there is at least one valid formatted storage directory
     */
    public static boolean recoverStorageDirs(StartupOption startOpt, NNStorage storage,
                                             Map<StorageDirectory, StorageState> dataDirStates) throws IOException {
        boolean isFormatted = false;
        // This loop needs to be over all storage dirs, even shared dirs, to make
        // sure that we properly examine their state, but we make sure we don't
        // mutate the shared dir below in the actual loop.
        for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext(); ) {
            StorageDirectory sd = it.next();
            StorageState curState;
            if (startOpt == StartupOption.METADATAVERSION) {
                /* All we need is the layout version. */
                storage.readProperties(sd);
                return true;
            }

            try {
                curState = sd.analyzeStorage(startOpt, storage);
                // sd is locked but not opened
                switch (curState) {
                    case NON_EXISTENT:
                        // name-node fails if any of the configured storage dirs are missing
                        throw new InconsistentFSStateException(sd.getRoot(),
                                "storage directory does not exist or is not accessible."
                        );
                    case NOT_FORMATTED:
                        break;
                    case NORMAL:
                        break;
                    default:  // recovery is possible
                        sd.doRecover(curState);
                }
                if (curState != StorageState.NOT_FORMATTED && startOpt != StartupOption.ROLLBACK) {
                    // read and verify consistency with other directories
                    storage.readProperties(sd, startOpt);
                    isFormatted = true;
                }
                if (startOpt == StartupOption.IMPORT && isFormatted)
                    // import of a checkpoint is allowed only into empty image directories
                    throw new IOException(
                            "Cannot import image from a checkpoint. " + " NameNode already contains an image in " + sd.getRoot());
            } catch (IOException ioe) {
                sd.unlock();
                throw ioe;
            }
            dataDirStates.put(sd, curState);
        }
        return isFormatted;
    }

    /** Check if upgrade is in progress. */
    public static void checkUpgrade(NNStorage storage) throws IOException {
        // Upgrade or rolling upgrade is allowed only if there are
        // no previous fs states in any of the local directories
        for (Iterator<StorageDirectory> it = storage.dirIterator(false); it.hasNext(); ) {
            StorageDirectory sd = it.next();
            if (sd.getPreviousDir().exists()) throw new InconsistentFSStateException(sd.getRoot(),
                    "previous fs state should not exist during upgrade. " + "Finalize or rollback first."
            );
        }
    }

    void checkUpgrade() throws IOException {
        checkUpgrade(storage);
    }

    /**
     * @return true if there is rollback fsimage (for rolling upgrade) in NameNode
     * directory.
     */
    public boolean hasRollbackFSImage() throws IOException {
        final FSImageStorageInspector inspector = new FSImageTransactionalStorageInspector(
                EnumSet.of(NameNodeFile.IMAGE_ROLLBACK));
        storage.inspectStorageDirs(inspector);
        try {
            List<FSImageFile> images = inspector.getLatestImages();
            return images != null && !images.isEmpty();
        } catch (FileNotFoundException e) {
            return false;
        }
    }

    void doUpgrade(FSNamesystem target) throws IOException {
        checkUpgrade();

        // load the latest image

        // Do upgrade for each directory
        this.loadFSImage(target, StartupOption.UPGRADE, null);
        target.checkRollingUpgrade("upgrade namenode");

        long oldCTime = storage.getCTime();
        storage.cTime = now();  // generate new cTime for the state
        int oldLV = storage.getLayoutVersion();
        storage.layoutVersion = HdfsServerConstants.NAMENODE_LAYOUT_VERSION;

        List<StorageDirectory> errorSDs = Collections.synchronizedList(new ArrayList<StorageDirectory>());
        assert !editLog.isSegmentOpen() : "Edits log must not be open.";
        LOG.info(
                "Starting upgrade of local storage directories." + "\n   old LV = " + oldLV + "; old CTime = " + oldCTime + ".\n   new LV = " + storage.getLayoutVersion() + "; new CTime = " + storage.getCTime());
        // Do upgrade for each directory
        for (Iterator<StorageDirectory> it = storage.dirIterator(false); it.hasNext(); ) {
            StorageDirectory sd = it.next();
            try {
                NNUpgradeUtil.doPreUpgrade(conf, sd);
            } catch (Exception e) {
                LOG.error("Failed to move aside pre-upgrade storage " + "in image directory " + sd.getRoot(), e);
                errorSDs.add(sd);
                continue;
            }
        }
        if (target.isHaEnabled()) {
            editLog.doPreUpgradeOfSharedLog();
        }
        storage.reportErrorsOnDirectories(errorSDs);
        errorSDs.clear();

        saveFSImageInAllDirs(target, editLog.getLastWrittenTxId());

        // upgrade shared edit storage first
        if (target.isHaEnabled()) {
            editLog.doUpgradeOfSharedLog();
        }
        for (Iterator<StorageDirectory> it = storage.dirIterator(false); it.hasNext(); ) {
            StorageDirectory sd = it.next();
            try {
                NNUpgradeUtil.doUpgrade(sd, storage);
            } catch (IOException ioe) {
                errorSDs.add(sd);
                continue;
            }
        }
        storage.reportErrorsOnDirectories(errorSDs);

        isUpgradeFinalized = false;
        if (!storage.getRemovedStorageDirs().isEmpty()) {
            // during upgrade, it's a fatal error to fail any storage directory
            throw new IOException("Upgrade failed in " + storage.getRemovedStorageDirs()
                    .size() + " storage directory(ies), previously logged.");
        }
    }

    void doRollback(FSNamesystem fsns) throws IOException {
        // Rollback is allowed only if there is
        // a previous fs states in at least one of the storage directories.
        // Directories that don't have previous state do not rollback
        boolean canRollback = false;
        FSImage prevState = new FSImage(conf);
        try {
            prevState.getStorage().layoutVersion = HdfsServerConstants.NAMENODE_LAYOUT_VERSION;
            for (Iterator<StorageDirectory> it = storage.dirIterator(false); it.hasNext(); ) {
                StorageDirectory sd = it.next();
                if (!NNUpgradeUtil.canRollBack(sd, storage, prevState.getStorage(),
                        HdfsServerConstants.NAMENODE_LAYOUT_VERSION
                )) {
                    continue;
                }
                LOG.info("Can perform rollback for " + sd);
                canRollback = true;
            }

            if (fsns.isHaEnabled()) {
                // If HA is enabled, check if the shared log can be rolled back as well.
                editLog.initJournalsForWrite();
                boolean canRollBackSharedEditLog = editLog.canRollBackSharedLog(prevState.getStorage(),
                        HdfsServerConstants.NAMENODE_LAYOUT_VERSION
                );
                if (canRollBackSharedEditLog) {
                    LOG.info("Can perform rollback for shared edit log.");
                    canRollback = true;
                }
            }

            if (!canRollback)
                throw new IOException("Cannot rollback. None of the storage " + "directories contain previous fs state.");

            // Now that we know all directories are going to be consistent
            // Do rollback for each directory containing previous state
            for (Iterator<StorageDirectory> it = storage.dirIterator(false); it.hasNext(); ) {
                StorageDirectory sd = it.next();
                LOG.info("Rolling back storage directory " + sd.getRoot() + ".\n   new LV = " + prevState.getStorage()
                        .getLayoutVersion() + "; new CTime = " + prevState.getStorage().getCTime());
                NNUpgradeUtil.doRollBack(sd);
            }
            if (fsns.isHaEnabled()) {
                // If HA is enabled, try to roll back the shared log as well.
                editLog.doRollback();
            }

            isUpgradeFinalized = true;
        } finally {
            prevState.close();
        }
    }

    /**
     * Load image from a checkpoint directory and save it into the current one.
     * @param target the NameSystem to import into
     * @throws IOException
     */
    void doImportCheckpoint(FSNamesystem target) throws IOException {
        Collection<URI> checkpointDirs = FSImage.getCheckpointDirs(conf, null);
        List<URI> checkpointEditsDirs = FSImage.getCheckpointEditsDirs(conf, null);

        if (checkpointDirs == null || checkpointDirs.isEmpty()) {
            throw new IOException("Cannot import image from a checkpoint. " + "\"dfs.namenode.checkpoint.dir\" is not set.");
        }

        if (checkpointEditsDirs == null || checkpointEditsDirs.isEmpty()) {
            throw new IOException("Cannot import image from a checkpoint. " + "\"dfs.namenode.checkpoint.dir\" is not set.");
        }

        FSImage realImage = target.getFSImage();
        FSImage ckptImage = new FSImage(conf, checkpointDirs, checkpointEditsDirs);
        // load from the checkpoint dirs
        try {
            ckptImage.recoverTransitionRead(StartupOption.REGULAR, target, null);
        } finally {
            ckptImage.close();
        }
        // return back the real image
        realImage.getStorage().setStorageInfo(ckptImage.getStorage());
        realImage.getEditLog().setNextTxId(ckptImage.getEditLog().getLastWrittenTxId() + 1);
        realImage.initEditLog(StartupOption.IMPORT);

        realImage.getStorage().setBlockPoolID(ckptImage.getBlockPoolID());

        // and save it but keep the same checkpointTime
        saveNamespace(target);
        updateStorageVersion();
    }

    void finalizeUpgrade(boolean finalizeEditLog) throws IOException {
        LOG.info(
                "Finalizing upgrade for local dirs. " + (storage.getLayoutVersion() == 0 ? "" : "\n   cur LV = " + storage.getLayoutVersion() + "; cur CTime = " + storage.getCTime()));
        for (Iterator<StorageDirectory> it = storage.dirIterator(false); it.hasNext(); ) {
            StorageDirectory sd = it.next();
            NNUpgradeUtil.doFinalize(sd);
        }
        if (finalizeEditLog) {
            // We only do this in the case that HA is enabled and we're active. In any
            // other case the NN will have done the upgrade of the edits directories
            // already by virtue of the fact that they're local.
            editLog.doFinalizeOfSharedLog();
        }
        isUpgradeFinalized = true;
    }

    boolean isUpgradeFinalized() {
        return isUpgradeFinalized;
    }

    public FSEditLog getEditLog() {
        return editLog;
    }

    void openEditLogForWrite(int layoutVersion) throws IOException {
        assert editLog != null : "editLog must be initialized";

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        editLog.openForWrite(layoutVersion);

        // TODO_MA 马中华 注释： 生成 seen_txid 文件
        storage.writeTransactionIdFileToStorage(editLog.getCurSegmentTxId());
    }

    /**
     * Toss the current image and namesystem, reloading from the specified
     * file.
     */
    void reloadFromImageFile(File file, FSNamesystem target) throws IOException {
        target.clear();
        LOG.debug("Reloading namespace from " + file);
        loadFSImage(file, target, null, false);
    }

    /**
     * Choose latest image from one of the directories,
     * load it and merge with the edits.
     *
     * Saving and loading fsimage should never trigger symlink resolution.
     * The paths that are persisted do not have *intermediate* symlinks
     * because intermediate symlinks are resolved at the time files,
     * directories, and symlinks are created. All paths accessed while
     * loading or saving fsimage should therefore only see symlinks as
     * the final path component, and the functions called below do not
     * resolve symlinks that are the final path component.
     *
     * // TODO_MA 马中华 注释： 因为 FSImage 就是管理所有磁盘元数据的，所以....
     *
     * @return whether the image should be saved
     * @throws IOException
     */
    private boolean loadFSImage(FSNamesystem target, StartupOption startOpt,
                                MetaRecoveryContext recovery) throws IOException {
        final boolean rollingRollback = RollingUpgradeStartupOption.ROLLBACK.matches(startOpt);
        final EnumSet<NameNodeFile> nnfs;
        if (rollingRollback) {
            // if it is rollback of rolling upgrade, only load from the rollback image
            nnfs = EnumSet.of(NameNodeFile.IMAGE_ROLLBACK);
        } else {
            // otherwise we can load from both IMAGE and IMAGE_ROLLBACK
            nnfs = EnumSet.of(NameNodeFile.IMAGE, NameNodeFile.IMAGE_ROLLBACK);
        }
        final FSImageStorageInspector inspector = storage.readAndInspectDirs(nnfs, startOpt);
        isUpgradeFinalized = inspector.isUpgradeFinalized();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 获取 fsimage 文件列表
         */
        List<FSImageFile> imageFiles = inspector.getLatestImages();

        StartupProgress prog = NameNode.getStartupProgress();
        prog.beginPhase(Phase.LOADING_FSIMAGE);
        File phaseFile = imageFiles.get(0).getFile();
        prog.setFile(Phase.LOADING_FSIMAGE, phaseFile.getAbsolutePath());
        prog.setSize(Phase.LOADING_FSIMAGE, phaseFile.length());
        boolean needToSave = inspector.needToSave();

        Iterable<EditLogInputStream> editStreams = null;

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 初始化 JournalManager
         */
        initEditLog(startOpt);

        if (NameNodeLayoutVersion.supports(LayoutVersion.Feature.TXID_BASED_LAYOUT, getLayoutVersion())) {
            // If we're open for write, we're either non-HA or we're the active NN, so
            // we better be able to load all the edits. If we're the standby NN, it's
            // OK to not be able to read all of edits right now.
            // In the meanwhile, for HA upgrade, we will still write editlog thus need
            // this toAtLeastTxId to be set to the max-seen txid
            // For rollback in rolling upgrade, we need to set the toAtLeastTxId to
            // the txid right before the upgrade marker.
            long toAtLeastTxId = editLog.isOpenForWrite() ? inspector.getMaxSeenTxId() : 0;
            if (rollingRollback) {
                // note that the first image in imageFiles is the special checkpoint
                // for the rolling upgrade
                toAtLeastTxId = imageFiles.get(0).getCheckpointTxId() + 2;
            }
            editStreams = editLog.selectInputStreams(imageFiles.get(0).getCheckpointTxId() + 1, toAtLeastTxId, recovery,
                    false
            );
        } else {
            editStreams = FSImagePreTransactionalStorageInspector.getEditLogStreams(storage);
        }
        int maxOpSize = conf.getInt(DFSConfigKeys.DFS_NAMENODE_MAX_OP_SIZE_KEY,
                DFSConfigKeys.DFS_NAMENODE_MAX_OP_SIZE_DEFAULT
        );
        for (EditLogInputStream elis : editStreams) {
            elis.setMaxOpSize(maxOpSize);
        }

        for (EditLogInputStream l : editStreams) {
            LOG.debug("Planning to load edit log stream: " + l);
        }
        if (!editStreams.iterator().hasNext()) {
            LOG.info("No edit log streams selected.");
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 第一件事： 加载 fsimage 文件，
         *  imageFiles 包含了多个 FSImage 文件，但是肯定是按照 txid 做了降序排序的
         *  最新的 FSImage 排在最前面
         */
        FSImageFile imageFile = null;
        for (int i = 0; i < imageFiles.size(); i++) {
            try {
                imageFile = imageFiles.get(i);

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： fsimage_txid = fsimage_5566
                 */
                loadFSImageFile(target, recovery, imageFile, startOpt);

                // TODO_MA 马中华 注释： 加载 fsimage 文件成功，就跳出循环
                break;
            } catch (IllegalReservedPathException ie) {
                throw new IOException("Failed to load image from " + imageFile, ie);
            } catch (Exception e) {
                LOG.error("Failed to load image from " + imageFile, e);
                target.clear();
                imageFile = null;
            }
        }
        // Failed to load any images, error out
        if (imageFile == null) {
            FSEditLog.closeAllStreams(editStreams);
            throw new IOException("Failed to load FSImage file, see error(s) " + "above for more info.");
        }
        prog.endPhase(Phase.LOADING_FSIMAGE);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 加载 editlog 日志文件
         */
        if (!rollingRollback) {
            prog.beginPhase(Phase.LOADING_EDITS);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 加载 edits_firstid_lastid 文件的内容到内存
             *  edits_5566_5599
             *  edits_5599_6655
             */
            long txnsAdvanced = loadEdits(editStreams, target, Long.MAX_VALUE, startOpt, recovery);
            prog.endPhase(Phase.LOADING_EDITS);

            needToSave |= needsResaveBasedOnStaleCheckpoint(imageFile.getFile(), txnsAdvanced);
        } else {
            // Trigger the rollback for rolling upgrade. Here lastAppliedTxId equals
            // to the last txid in rollback fsimage.
            rollingRollback(lastAppliedTxId + 1, imageFiles.get(0).getCheckpointTxId());
            needToSave = false;
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        editLog.setNextTxId(lastAppliedTxId + 1);

        // TODO_MA 马中华 注释：
        return needToSave;
    }

    /** rollback for rolling upgrade. */
    private void rollingRollback(long discardSegmentTxId, long ckptId) throws IOException {
        // discard discard unnecessary editlog segments starting from the given id
        this.editLog.discardSegments(discardSegmentTxId);
        // rename the special checkpoint
        renameCheckpoint(ckptId, NameNodeFile.IMAGE_ROLLBACK, NameNodeFile.IMAGE, true);
        // purge all the checkpoints after the marker
        archivalManager.purgeCheckpoinsAfter(NameNodeFile.IMAGE, ckptId);
        // HDFS-7939: purge all old fsimage_rollback_*
        archivalManager.purgeCheckpoints(NameNodeFile.IMAGE_ROLLBACK);
        String nameserviceId = DFSUtil.getNamenodeNameServiceId(conf);
        if (HAUtil.isHAEnabled(conf, nameserviceId)) {
            // close the editlog since it is currently open for write
            this.editLog.close();
            // reopen the editlog for read
            this.editLog.initSharedJournalsForRead();
        }
    }

    void loadFSImageFile(FSNamesystem target, MetaRecoveryContext recovery, FSImageFile imageFile,
                         StartupOption startupOption) throws IOException {
        LOG.info("Planning to load image: " + imageFile);
        StorageDirectory sdForProperties = imageFile.sd;
        storage.readProperties(sdForProperties, startupOption);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 对比 LayoutVersion 信息
         */
        if (NameNodeLayoutVersion.supports(LayoutVersion.Feature.TXID_BASED_LAYOUT, getLayoutVersion())) {
            // For txid-based layout, we should have a .md5 file
            // next to the image file
            boolean isRollingRollback = RollingUpgradeStartupOption.ROLLBACK.matches(startupOption);
            loadFSImage(imageFile.getFile(), target, recovery, isRollingRollback);
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 校验 CHECKSUM
         */
        else if (NameNodeLayoutVersion.supports(LayoutVersion.Feature.FSIMAGE_CHECKSUM, getLayoutVersion())) {
            // In 0.22, we have the checksum stored in the VERSION file.
            String md5 = storage.getDeprecatedProperty(NNStorage.DEPRECATED_MESSAGE_DIGEST_PROPERTY);
            if (md5 == null) {
                throw new InconsistentFSStateException(sdForProperties.getRoot(),
                        "Message digest property " + NNStorage.DEPRECATED_MESSAGE_DIGEST_PROPERTY + " not set for storage directory " + sdForProperties.getRoot()
                );
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 常规走这儿
             */
            loadFSImage(imageFile.getFile(), new MD5Hash(md5), target, recovery, false);
        }

        // TODO_MA 马中华 注释：  我们没有任何关于 md5sum 的记录
        else {
            // We don't have any record of the md5sum
            loadFSImage(imageFile.getFile(), null, target, recovery, false);
        }
    }

    public void initEditLog(StartupOption startOpt) throws IOException {
        Preconditions.checkState(getNamespaceID() != 0, "Must know namespace ID before initting edit log");

        // TODO_MA 马中华 注释： nameserviceID
        String nameserviceId = DFSUtil.getNamenodeNameServiceId(conf);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 普通分布式模式
         */
        if (!HAUtil.isHAEnabled(conf, nameserviceId)) {
            // If this NN is not HA
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 只针对 editsDirs 创建 JournalManager
             */
            editLog.initJournalsForWrite();
            editLog.recoverUnclosedStreams();
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： HA 模式
         */
        else if (HAUtil.isHAEnabled(conf, nameserviceId) &&
                // TODO_MA 马中华 注释： UPGRADE 或者 UPGRADEONLY 或者 ROLLBACK
                // TODO_MA 马中华 注释： 常规启动是 REGULAR 所以不走这儿
                (startOpt == StartupOption.UPGRADE || startOpt == StartupOption.UPGRADEONLY || RollingUpgradeStartupOption.ROLLBACK.matches(
                        startOpt)
                )) {
            // This NN is HA, but we're doing an upgrade or a rollback of rolling
            // upgrade so init the edit log for write.
            editLog.initJournalsForWrite();
            if (startOpt == StartupOption.UPGRADE || startOpt == StartupOption.UPGRADEONLY) {
                long sharedLogCTime = editLog.getSharedLogCTime();
                if (this.storage.getCTime() < sharedLogCTime) {
                    throw new IOException(
                            "It looks like the shared log is already " + "being upgraded but this NN has not been upgraded yet. You " + "should restart this NameNode with the '" + StartupOption.BOOTSTRAPSTANDBY.getName() + "' option to bring " + "this NN in sync with the other.");
                }
            }
            editLog.recoverUnclosedStreams();
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 正常启动情况下，是走这儿
         */
        else {
            // This NN is HA and we're not doing an upgrade.
            editLog.initSharedJournalsForRead();
        }
    }

    /**
     * @param imageFile the image file that was loaded
     * @param numEditsLoaded the number of edits loaded from edits logs
     * @return true if the NameNode should automatically save the namespace
     * when it is started, due to the latest checkpoint being too old.
     */
    private boolean needsResaveBasedOnStaleCheckpoint(File imageFile, long numEditsLoaded) {
        final long checkpointPeriod = conf.getTimeDuration(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY,
                DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_DEFAULT, TimeUnit.SECONDS
        );
        final long checkpointTxnCount = conf.getLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY,
                DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_DEFAULT
        );
        long checkpointAge = Time.now() - imageFile.lastModified();

        return (checkpointAge > checkpointPeriod * 1000) || (numEditsLoaded > checkpointTxnCount);
    }

    /**
     * Load the specified list of edit files into the image.
     */
    public long loadEdits(Iterable<EditLogInputStream> editStreams, FSNamesystem target) throws IOException {
        return loadEdits(editStreams, target, Long.MAX_VALUE, null, null);
    }

    // TODO_MA 马中华 注释： 返回加载的日志条数
    public long loadEdits(Iterable<EditLogInputStream> editStreams, FSNamesystem target, long maxTxnsToRead,
                          StartupOption startOpt, MetaRecoveryContext recovery) throws IOException {
        LOG.debug("About to load edits:\n  " + Joiner.on("\n  ").join(editStreams));

        // TODO_MA 马中华 注释： 记录命名空间中加载的最新的事务id
        long prevLastAppliedTxId = lastAppliedTxId;
        long remainingReadTxns = maxTxnsToRead;
        try {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 构造 FSEditLogLoader 实例用于加载 editlog 文件
             */
            FSEditLogLoader loader = new FSEditLogLoader(target, lastAppliedTxId);

            // TODO_MA 马中华 注释： 顺序加载多个 edits 编辑日志文件
            // Load latest edits
            for (EditLogInputStream editIn : editStreams) {
                LogAction logAction = loadEditLogHelper.record();
                if (logAction.shouldLog()) {
                    String logSuppressed = "";
                    if (logAction.getCount() > 1) {
                        logSuppressed = "; suppressed logging for " + (logAction.getCount() - 1) + " edit reads";
                    }
                    LOG.info("Reading " + editIn + " expecting start txid #" + (lastAppliedTxId + 1) + logSuppressed);
                }
                try {
                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 加载日志
                     */
                    remainingReadTxns -= loader.loadFSEdits(editIn, lastAppliedTxId + 1, remainingReadTxns, startOpt,
                            recovery
                    );
                } finally {

                    // TODO_MA 马中华 注释： 记录最后一条事务日志id
                    // Update lastAppliedTxId even in case of error, since some ops may
                    // have been successfully applied before the error.
                    lastAppliedTxId = loader.getLastAppliedTxId();
                }
                // If we are in recovery mode, we may have skipped over some txids.
                if (editIn.getLastTxId() != HdfsServerConstants.INVALID_TXID && recovery != null) {
                    lastAppliedTxId = editIn.getLastTxId();
                }
                if (remainingReadTxns <= 0) {
                    break;
                }
            }
        } finally {
            FSEditLog.closeAllStreams(editStreams);
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 返回加载的日志条数
         */
        return lastAppliedTxId - prevLastAppliedTxId;
    }

    /**
     * Load the image namespace from the given image file, verifying
     * it against the MD5 sum stored in its associated .md5 file.
     */
    private void loadFSImage(File imageFile, FSNamesystem target, MetaRecoveryContext recovery,
                             boolean requireSameLayoutVersion) throws IOException {
        MD5Hash expectedMD5 = MD5FileUtils.readStoredMd5ForFile(imageFile);
        if (expectedMD5 == null) {
            throw new IOException("No MD5 file found corresponding to image file " + imageFile);
        }
        loadFSImage(imageFile, expectedMD5, target, recovery, requireSameLayoutVersion);
    }

    /**
     * Load in the filesystem image from file. It's a big list of
     * filenames and blocks.
     */
    private void loadFSImage(File curFile, MD5Hash expectedMd5, FSNamesystem target, MetaRecoveryContext recovery,
                             boolean requireSameLayoutVersion) throws IOException {
        // BlockPoolId is required when the FsImageLoader loads the rolling upgrade
        // information. Make sure the ID is properly set.
        target.setBlockPoolId(this.getBlockPoolID());

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 加载 fsimage 文件
         */
        FSImageFormat.LoaderDelegator loader = FSImageFormat.newLoader(conf, target);
        loader.load(curFile, requireSameLayoutVersion);

        // Check that the image digest we loaded matches up with what we expected
        MD5Hash readImageMd5 = loader.getLoadedImageMd5();
        if (expectedMd5 != null && !expectedMd5.equals(readImageMd5)) {
            throw new IOException(
                    "Image file " + curFile + " is corrupt with MD5 checksum of " + readImageMd5 + " but expecting " + expectedMd5);
        }

        // TODO_MA 马中华 注释： 得到 fsimage 的 最后一个 txid
        long txId = loader.getLoadedImageTxId();
        LOG.info("Loaded image for txid " + txId + " from " + curFile);
        lastAppliedTxId = txId;
        storage.setMostRecentCheckpointInfo(txId, curFile.lastModified());
    }

    /**
     * Save the contents of the FS image to the file.
     */
    void saveFSImage(SaveNamespaceContext context, StorageDirectory sd, NameNodeFile dstType) throws IOException {

        // TODO_MA 马中华 注释： 最新事务id
        long txid = context.getTxId();

        // TODO_MA 马中华 注释： 生成 fsimage.ckpt 文件
        File newFile = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE_NEW, txid);
        // TODO_MA 马中华 注释： 生成 fsimage 文件
        File dstFile = NNStorage.getStorageFile(sd, dstType, txid);

        // TODO_MA 马中华 注释： 构建 FSImageFormatProtobuf.Saver 实例
        // TODO_MA 马中华 注释： FSImageFormatProtobuf.Saver 类就是以 protobuf 格式将 Namenode 的命名空间保存至 fsimage 文件的工具类
        FSImageFormatProtobuf.Saver saver = new FSImageFormatProtobuf.Saver(context, conf);
        FSImageCompression compression = FSImageCompression.createCompression(conf);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 完成 namenode 命名空间到 fsimage 的保存工作， 临时文件名： fsimage.ckpt
         */
        long numErrors = saver.save(newFile, compression);
        if (numErrors > 0) {
            // The image is likely corrupted.
            LOG.error("Detected " + numErrors + " errors while saving FsImage " + dstFile);
            exitAfterSave.set(true);
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 生成 md5 文件
         */
        MD5FileUtils.saveMD5File(dstFile, saver.getSavedDigest());

        // TODO_MA 马中华 注释： 保存最近一次 checkpoint 的信息
        storage.setMostRecentCheckpointInfo(txid, Time.now());
    }

    /**
     * Save FSimage in the legacy format. This is not for NN consumption,
     * but for tools like OIV.
     */
    public void saveLegacyOIVImage(FSNamesystem source, String targetDir, Canceler canceler) throws IOException {
        FSImageCompression compression = FSImageCompression.createCompression(conf);
        long txid = getCorrectLastAppliedOrWrittenTxId();
        SaveNamespaceContext ctx = new SaveNamespaceContext(source, txid, canceler);
        FSImageFormat.Saver saver = new FSImageFormat.Saver(ctx);
        String imageFileName = NNStorage.getLegacyOIVImageFileName(txid);
        File imageFile = new File(targetDir, imageFileName);
        saver.save(imageFile, compression);
        archivalManager.purgeOldLegacyOIVImages(targetDir, txid);
    }


    /**
     * FSImageSaver is being run in a separate thread when saving
     * FSImage. There is one thread per each copy of the image.
     *
     * FSImageSaver assumes that it was launched from a thread that holds
     * FSNamesystem lock and waits for the execution of FSImageSaver thread
     * to finish.
     * This way we are guaranteed that the namespace is not being updated
     * while multiple instances of FSImageSaver are traversing it
     * and writing it out.
     * // TODO_MA 马中华 注释： NameNode 的每个 StorageDirectory 都有一个 FSImageSaver 线程类来完成保存 fsimage 文件动作
     * // TODO_MA 马中华 注释： 线程的核心工作，是调用 saveFSImage() 来保存 fsimage 文件
     */
    private class FSImageSaver implements Runnable {
        private final SaveNamespaceContext context;
        private final StorageDirectory sd;
        private final NameNodeFile nnf;

        public FSImageSaver(SaveNamespaceContext context, StorageDirectory sd, NameNodeFile nnf) {
            this.context = context;
            this.sd = sd;
            this.nnf = nnf;
        }

        @Override
        public void run() {

            // TODO_MA 马中华 注释： 删除已取消的 checkpoint 对应的 fsimage.ckpt
            // Deletes checkpoint file in every storage directory when shutdown.
            Runnable cancelCheckpointFinalizer = () -> {
                try {
                    deleteCancelledCheckpoint(context.getTxId());
                    LOG.info("FSImageSaver clean checkpoint: txid={} when meet " + "shutdown.", context.getTxId());
                } catch (IOException e) {
                    LOG.error("FSImageSaver cancel checkpoint threw an exception:", e);
                }
            };
            ShutdownHookManager.get().addShutdownHook(cancelCheckpointFinalizer, SHUTDOWN_HOOK_PRIORITY);
            try {
                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 保存 fsimage 文件
                 */
                saveFSImage(context, sd, nnf);
            } catch (SaveNamespaceCancelledException snce) {
                LOG.info("Cancelled image saving for " + sd.getRoot() + ": " + snce.getMessage());
                // don't report an error on the storage dir!
            } catch (Throwable t) {
                LOG.error("Unable to save image for " + sd.getRoot(), t);
                context.reportErrorOnStorageDirectory(sd);
                try {
                    deleteCancelledCheckpoint(context.getTxId());
                    LOG.info("FSImageSaver clean checkpoint: txid={} when meet " + "Throwable.", context.getTxId());
                } catch (IOException e) {
                    LOG.error("FSImageSaver cancel checkpoint threw an exception:", e);
                }
            }
        }

        @Override
        public String toString() {
            return "FSImageSaver for " + sd.getRoot() + " of type " + sd.getStorageDirType();
        }
    }

    private void waitForThreads(List<Thread> threads) {
        for (Thread thread : threads) {
            while (thread.isAlive()) {
                try {
                    thread.join();
                } catch (InterruptedException iex) {
                    LOG.error(
                            "Caught interrupted exception while waiting for thread " + thread.getName() + " to finish. Retrying join");
                }
            }
        }
    }

    /**
     * Update version of all storage directories.
     */
    public synchronized void updateStorageVersion() throws IOException {
        storage.writeAll();
    }

    /**
     * @param timeWindow a checkpoint is done if the latest checkpoint
     *                   was done more than this number of seconds ago.
     * @param txGap a checkpoint is done also if the gap between the latest tx id
     *              and the latest checkpoint is greater than this number.
     * @return true if a checkpoint has been made
     * @see #saveNamespace(FSNamesystem, NameNodeFile, Canceler)
     */
    public synchronized boolean saveNamespace(long timeWindow, long txGap, FSNamesystem source) throws IOException {
        if (timeWindow > 0 || txGap > 0) {
            final FSImageStorageInspector inspector = storage.readAndInspectDirs(
                    EnumSet.of(NameNodeFile.IMAGE, NameNodeFile.IMAGE_ROLLBACK), StartupOption.REGULAR);
            FSImageFile image = inspector.getLatestImages().get(0);
            File imageFile = image.getFile();

            final long checkpointTxId = image.getCheckpointTxId();
            final long checkpointAge = Time.now() - imageFile.lastModified();
            if (checkpointAge <= timeWindow * 1000 && checkpointTxId >= this.getCorrectLastAppliedOrWrittenTxId() - txGap) {
                return false;
            }
        }
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        saveNamespace(source, NameNodeFile.IMAGE, null);
        return true;
    }

    public void saveNamespace(FSNamesystem source) throws IOException {
        saveNamespace(0, 0, source);
    }

    /**
     * Save the contents of the FS image to a new image file in each of the
     * current storage directories.
     */
    public synchronized void saveNamespace(FSNamesystem source, NameNodeFile nnf, Canceler canceler) throws IOException {
        assert editLog != null : "editLog must be initialized";
        LOG.info("Save namespace ...");
        storage.attemptRestoreRemovedStorage();

        boolean editLogWasOpen = editLog.isSegmentOpen();
        if (editLogWasOpen) {
            editLog.endCurrentLogSegment(true);
        }

        long imageTxId = getCorrectLastAppliedOrWrittenTxId();
        if (!addToCheckpointing(imageTxId)) {
            throw new IOException("FS image is being downloaded from another NN at txid " + imageTxId);
        }
        try {
            try {
                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 每个存储目录，都生成一个新的 fsimage 文件
                 */
                saveFSImageInAllDirs(source, nnf, imageTxId, canceler);
                if (!source.isRollingUpgrade()) {
                    updateStorageVersion();
                }
            } finally {
                // TODO_MA 马中华 注释： 做两件收尾工作
                if (editLogWasOpen) {
                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 1、创建一个新的 edits_inprogress_txid 文件
                     */
                    editLog.startLogSegmentAndWriteHeaderTxn(imageTxId + 1, source.getEffectiveLayoutVersion());
                    // Take this opportunity to note the current transaction.
                    // Even if the namespace save was cancelled, this marker
                    // is only used to determine what transaction ID is required
                    // for startup. So, it doesn't hurt to update it unnecessarily.

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 2、更新 txid 到 seen_txid 文件中
                     */
                    storage.writeTransactionIdFileToStorage(imageTxId + 1);
                }
            }
        } finally {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            removeFromCheckpointing(imageTxId);
        }
        //Update NameDirSize Metric
        getStorage().updateNameDirSize();

        if (exitAfterSave.get()) {
            LOG.error("NameNode process will exit now... The saved FsImage " + nnf + " is potentially corrupted.");
            ExitUtil.terminate(-1);
        }
    }

    /**
     * @see #saveFSImageInAllDirs(FSNamesystem, NameNodeFile, long, Canceler)
     */
    protected synchronized void saveFSImageInAllDirs(FSNamesystem source, long txid) throws IOException {
        if (!addToCheckpointing(txid)) {
            throw new IOException(("FS image is being downloaded from another NN"));
        }
        try {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            saveFSImageInAllDirs(source, NameNodeFile.IMAGE, txid, null);
        } finally {
            removeFromCheckpointing(txid);
        }
    }

    public boolean addToCheckpointing(long txid) {
        return currentlyCheckpointing.add(txid);
    }

    public void removeFromCheckpointing(long txid) {
        currentlyCheckpointing.remove(txid);
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 核心思路
     *  1、针对指定的多个存储目录，会使用多个线程，来完成 fsimage 保存工作
     *  2、具体保存 fsimage 工作是先 生成一个 fsimage.ckpt 文件，如果没出错，则重命名成正式 fsimage 文件
     *  3、清理 namenode 元数据中，过期的 editlog 和 fsimage 文件
     */
    private synchronized void saveFSImageInAllDirs(FSNamesystem source, NameNodeFile nnf, long txid,
                                                   Canceler canceler) throws IOException {
        StartupProgress prog = NameNode.getStartupProgress();

        // TODO_MA 马中华 注释： 开始
        prog.beginPhase(Phase.SAVING_CHECKPOINT);
        if (storage.getNumStorageDirs(NameNodeDirType.IMAGE) == 0) {
            throw new IOException("No image directories available!");
        }
        if (canceler == null) {
            canceler = new Canceler();
        }

        // TODO_MA 马中华 注释： 上下文
        SaveNamespaceContext ctx = new SaveNamespaceContext(source, txid, canceler);

        try {
            List<Thread> saveThreads = new ArrayList<Thread>();

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 迭代器每一个 StorageDirectory（指定的一个元数据存储目录），通过一个 FSImageSaver 线程来完成 fsiamge 生成和保存工作
             */
            // save images into current
            for (Iterator<StorageDirectory> it = storage.dirIterator(NameNodeDirType.IMAGE); it.hasNext(); ) {
                // TODO_MA 马中华 注释： 1、迭代获取一个 StorageDirectory
                StorageDirectory sd = it.next();
                // TODO_MA 马中华 注释： 2、构建一个 FSImageSaver 线程
                FSImageSaver saver = new FSImageSaver(ctx, sd, nnf);
                Thread saveThread = new Thread(saver, saver.toString());
                saveThreads.add(saveThread);
                // TODO_MA 马中华 注释： 3、启动线程
                saveThread.start();
            }

            // TODO_MA 马中华 注释： 等待所有线程执行完毕
            waitForThreads(saveThreads);
            saveThreads.clear();

            storage.reportErrorsOnDirectories(ctx.getErrorSDs());

            // TODO_MA 马中华 注释： 保存失败，抛出 IOException 异常
            if (storage.getNumStorageDirs(NameNodeDirType.IMAGE) == 0) {
                throw new IOException("Failed to save in any storage directories while saving namespace.");
            }
            if (canceler.isCancelled()) {
                deleteCancelledCheckpoint(txid);
                ctx.checkCancelled(); // throws
                assert false : "should have thrown above!";
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 重命名 fsimage.ckpt 为正式 fsimage 文件
             */
            renameCheckpoint(txid, NameNodeFile.IMAGE_NEW, nnf, false);

            // Since we now have a new checkpoint, we can clean up some
            // old edit logs and checkpoints.
            // Do not purge anything if we just wrote a corrupted FsImage.
            if (!exitAfterSave.get()) {

                // TODO_MA 马中华 注释： 如果上述 checkpoint 操作成功，则删除一些旧的 editlog 和 fsimage 文件
                purgeOldStorage(nnf);

                // TODO_MA 马中华 注释： 删除 old fsimage 文件
                archivalManager.purgeCheckpoints(NameNodeFile.IMAGE_NEW);
            }
        } finally {
            // Notify any threads waiting on the checkpoint to be canceled
            // that it is complete.
            ctx.markComplete();
            ctx = null;
        }
        prog.endPhase(Phase.SAVING_CHECKPOINT);
    }

    /**
     * // TODO_MA 马中华 注释： 删除 StorageDirectory 中的一些过期文件
     * Purge any files in the storage directories that are no longer necessary.
     */
    void purgeOldStorage(NameNodeFile nnf) {
        try {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            archivalManager.purgeOldStorage(nnf);
        } catch (Exception e) {
            LOG.warn("Unable to purge old storage " + nnf.getName(), e);
        }
    }

    /**
     * Rename FSImage with the specific txid
     */
    private void renameCheckpoint(long txid, NameNodeFile fromNnf, NameNodeFile toNnf,
                                  boolean renameMD5) throws IOException {
        ArrayList<StorageDirectory> al = null;

        for (StorageDirectory sd : storage.dirIterable(NameNodeDirType.IMAGE)) {
            try {
                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 文件重命名
                 */
                renameImageFileInDir(sd, fromNnf, toNnf, txid, renameMD5);
            } catch (IOException ioe) {
                LOG.warn("Unable to rename checkpoint in " + sd, ioe);
                if (al == null) {
                    al = Lists.newArrayList();
                }
                al.add(sd);
            }
        }
        if (al != null) storage.reportErrorsOnDirectories(al);
    }

    /**
     * Rename all the fsimage files with the specific NameNodeFile type. The
     * associated checksum files will also be renamed.
     */
    void renameCheckpoint(NameNodeFile fromNnf, NameNodeFile toNnf) throws IOException {
        ArrayList<StorageDirectory> al = null;
        FSImageTransactionalStorageInspector inspector = new FSImageTransactionalStorageInspector(EnumSet.of(fromNnf));
        storage.inspectStorageDirs(inspector);
        for (FSImageFile image : inspector.getFoundImages()) {
            try {
                renameImageFileInDir(image.sd, fromNnf, toNnf, image.txId, true);
            } catch (IOException ioe) {
                LOG.warn("Unable to rename checkpoint in " + image.sd, ioe);
                if (al == null) {
                    al = Lists.newArrayList();
                }
                al.add(image.sd);
            }
        }
        if (al != null) {
            storage.reportErrorsOnDirectories(al);
        }
    }

    /**
     * Deletes the checkpoint file in every storage directory,
     * since the checkpoint was cancelled.
     */
    private void deleteCancelledCheckpoint(long txid) throws IOException {
        ArrayList<StorageDirectory> al = Lists.newArrayList();

        // TODO_MA 马中华 注释： 删除已取消的 checkpoint 对应的 fsimage.ckpt
        for (StorageDirectory sd : storage.dirIterable(NameNodeDirType.IMAGE)) {
            File ckpt = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE_NEW, txid);
            if (ckpt.exists() && !ckpt.delete()) {
                LOG.warn("Unable to delete cancelled checkpoint in " + sd);
                al.add(sd);
            }
        }
        storage.reportErrorsOnDirectories(al);
    }

    private void renameImageFileInDir(StorageDirectory sd, NameNodeFile fromNnf, NameNodeFile toNnf, long txid,
                                      boolean renameMD5) throws IOException {

        // TODO_MA 马中华 注释： 临时文件
        final File fromFile = NNStorage.getStorageFile(sd, fromNnf, txid);
        // TODO_MA 马中华 注释： 目标文件
        final File toFile = NNStorage.getStorageFile(sd, toNnf, txid);

        // renameTo fails on Windows if the destination file already exists.
        if (LOG.isDebugEnabled()) {
            LOG.debug("renaming  " + fromFile.getAbsolutePath() + " to " + toFile.getAbsolutePath());
        }
        // TODO_MA 马中华 注释： fsimage 文件 重命名
        if (!fromFile.renameTo(toFile)) {
            if (!toFile.delete() || !fromFile.renameTo(toFile)) {
                throw new IOException(
                        "renaming  " + fromFile.getAbsolutePath() + " to " + toFile.getAbsolutePath() + " FAILED");
            }
        }
        // TODO_MA 马中华 注释： md5 文件 重命名
        if (renameMD5) {
            MD5FileUtils.renameMD5File(fromFile, toFile);
        }
    }

    CheckpointSignature rollEditLog(int layoutVersion) throws IOException {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        getEditLog().rollEditLog(layoutVersion);
        // Record this log segment ID in all of the storage directories, so
        // we won't miss this log segment on a restart if the edits directories go missing.
        storage.writeTransactionIdFileToStorage(getEditLog().getCurSegmentTxId());
        //Update NameDirSize Metric
        getStorage().updateNameDirSize();
        return new CheckpointSignature(this);
    }

    /**
     * Start checkpoint.
     * <p>
     * If backup storage contains image that is newer than or incompatible with
     * what the active name-node has, then the backup node should shutdown.<br>
     * If the backup image is older than the active one then it should
     * be discarded and downloaded from the active node.<br>
     * If the images are the same then the backup image will be used as current.
     *
     * @param bnReg the backup node registration.
     * @param nnReg this (active) name-node registration.
     * @return {@link NamenodeCommand} if backup node should shutdown or
     * {@link CheckpointCommand} prescribing what backup node should
     *         do with its image.
     * @throws IOException
     */
    NamenodeCommand startCheckpoint(NamenodeRegistration bnReg, // backup node
                                    NamenodeRegistration nnReg, int layoutVersion) // active name-node
            throws IOException {
        LOG.info("Start checkpoint at txid " + getEditLog().getLastWrittenTxId());
        String msg = null;
        // Verify that checkpoint is allowed
        if (bnReg.getNamespaceID() != storage.getNamespaceID())
            msg = "Name node " + bnReg.getAddress() + " has incompatible namespace id: " + bnReg.getNamespaceID() + " expected: " + storage.getNamespaceID();
        else if (bnReg.isRole(NamenodeRole.NAMENODE))
            msg = "Name node " + bnReg.getAddress() + " role " + bnReg.getRole() + ": checkpoint is not allowed.";
        else if (bnReg.getLayoutVersion() < storage.getLayoutVersion() || (bnReg.getLayoutVersion() == storage.getLayoutVersion() && bnReg.getCTime() > storage.getCTime()))
            // remote node has newer image age
            msg = "Name node " + bnReg.getAddress() + " has newer image layout version: LV = " + bnReg.getLayoutVersion() + " cTime = " + bnReg.getCTime() + ". Current version: LV = " + storage.getLayoutVersion() + " cTime = " + storage.getCTime();
        if (msg != null) {
            LOG.error(msg);
            return new NamenodeCommand(NamenodeProtocol.ACT_SHUTDOWN);
        }
        boolean needToReturnImg = true;
        if (storage.getNumStorageDirs(NameNodeDirType.IMAGE) == 0)
            // do not return image if there are no image directories
            needToReturnImg = false;

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        CheckpointSignature sig = rollEditLog(layoutVersion);
        return new CheckpointCommand(sig, needToReturnImg);
    }

    /**
     * End checkpoint.
     * <p>
     * Validate the current storage info with the given signature.
     *
     * @param sig to validate the current storage info against
     * @throws IOException if the checkpoint fields are inconsistent
     */
    void endCheckpoint(CheckpointSignature sig) throws IOException {
        LOG.info("End checkpoint at txid " + getEditLog().getLastWrittenTxId());
        sig.validateStorageInfo(this);
    }

    /**
     * This is called by the 2NN after having downloaded an image, and by
     * the NN after having received a new image from the 2NN. It
     * renames the image from fsimage_N.ckpt to fsimage_N and also
     * saves the related .md5 file into place.
     */
    public synchronized void saveDigestAndRenameCheckpointImage(NameNodeFile nnf, long txid,
                                                                MD5Hash digest) throws IOException {
        // Write and rename MD5 file
        List<StorageDirectory> badSds = Lists.newArrayList();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        for (StorageDirectory sd : storage.dirIterable(NameNodeDirType.IMAGE)) {
            File imageFile = NNStorage.getImageFile(sd, nnf, txid);
            try {
                MD5FileUtils.saveMD5File(imageFile, digest);
            } catch (IOException ioe) {
                badSds.add(sd);
            }
        }
        storage.reportErrorsOnDirectories(badSds);

        CheckpointFaultInjector.getInstance().afterMD5Rename();

        // Rename image from tmp file
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        renameCheckpoint(txid, NameNodeFile.IMAGE_NEW, nnf, false);

        // So long as this is the newest image available,
        // advertise it as such to other checkpointers
        // from now on
        if (txid > storage.getMostRecentCheckpointTxId()) {
            storage.setMostRecentCheckpointInfo(txid, Time.now());
        }

        // Create a version file in any new storage directory.
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        initNewDirs();
    }

    @Override
    synchronized public void close() throws IOException {
        if (editLog != null) { // 2NN doesn't have any edit log
            getEditLog().close();
        }
        storage.close();
    }


    /**
     * Retrieve checkpoint dirs from configuration.
     *
     * @param conf the Configuration
     * @param defaultValue a default value for the attribute, if null
     * @return a Collection of URIs representing the values in
     * dfs.namenode.checkpoint.dir configuration property
     */
    static Collection<URI> getCheckpointDirs(Configuration conf, String defaultValue) {
        Collection<String> dirNames = conf.getTrimmedStringCollection(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY);
        if (dirNames.size() == 0 && defaultValue != null) {
            dirNames.add(defaultValue);
        }
        return Util.stringCollectionAsURIs(dirNames);
    }

    static List<URI> getCheckpointEditsDirs(Configuration conf, String defaultName) {
        Collection<String> dirNames = conf.getTrimmedStringCollection(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY);
        if (dirNames.size() == 0 && defaultName != null) {
            dirNames.add(defaultName);
        }
        return Util.stringCollectionAsURIs(dirNames);
    }

    public NNStorage getStorage() {
        return storage;
    }

    public int getLayoutVersion() {
        return storage.getLayoutVersion();
    }

    public int getNamespaceID() {
        return storage.getNamespaceID();
    }

    public String getClusterID() {
        return storage.getClusterID();
    }

    public String getBlockPoolID() {
        return storage.getBlockPoolID();
    }

    public synchronized long getLastAppliedTxId() {
        return lastAppliedTxId;
    }

    public long getLastAppliedOrWrittenTxId() {
        return Math.max(lastAppliedTxId, editLog != null ? editLog.getLastWrittenTxIdWithoutLock() : 0);
    }

    /**
     * This method holds a lock of FSEditLog to get the correct value.
     * This method must not be used for metrics.
     */
    public long getCorrectLastAppliedOrWrittenTxId() {
        return Math.max(lastAppliedTxId, editLog != null ? editLog.getLastWrittenTxId() : 0);
    }

    public void updateLastAppliedTxIdFromWritten() {
        this.lastAppliedTxId = editLog.getLastWrittenTxId();
    }

    // Should be OK for this to not be synchronized since all of the places which
    // mutate this value are themselves synchronized so it shouldn't be possible
    // to see this flop back and forth. In the worst case this will just return an
    // old value.
    public long getMostRecentCheckpointTxId() {
        return storage.getMostRecentCheckpointTxId();
    }
}
