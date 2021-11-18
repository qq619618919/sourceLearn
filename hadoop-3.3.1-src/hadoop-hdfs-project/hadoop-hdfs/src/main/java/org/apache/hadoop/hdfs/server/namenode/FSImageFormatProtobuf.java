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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CacheDirectiveInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CachePoolInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ErasureCodingPolicyProto;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockIdManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.CacheManagerSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.NameSystemSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.SecretManagerSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.StringTableSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.ErasureCodingSection;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FSImageFormatPBSnapshot;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress.Counter;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.LimitInputStream;
import org.apache.hadoop.util.Time;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.thirdparty.protobuf.CodedOutputStream;

/**
 * Utility class to read / write fsimage in protobuf format.
 */
@InterfaceAudience.Private
public final class FSImageFormatProtobuf {
    private static final Logger LOG = LoggerFactory.getLogger(FSImageFormatProtobuf.class);

    private static volatile boolean enableParallelLoad = false;

    public static final class LoaderContext {
        private SerialNumberManager.StringTable stringTable;
        private final ArrayList<INodeReference> refList = Lists.newArrayList();

        public SerialNumberManager.StringTable getStringTable() {
            return stringTable;
        }

        public ArrayList<INodeReference> getRefList() {
            return refList;
        }
    }

    public static final class SaverContext {
        public static class DeduplicationMap<E> {
            private final Map<E, Integer> map = Maps.newHashMap();

            private DeduplicationMap() {
            }

            static <T> DeduplicationMap<T> newMap() {
                return new DeduplicationMap<T>();
            }

            int getId(E value) {
                if (value == null) {
                    return 0;
                }
                Integer v = map.get(value);
                if (v == null) {
                    int nv = map.size() + 1;
                    map.put(value, nv);
                    return nv;
                }
                return v;
            }

            int size() {
                return map.size();
            }

            Set<Entry<E, Integer>> entrySet() {
                return map.entrySet();
            }
        }

        private final ArrayList<INodeReference> refList = Lists.newArrayList();

        public ArrayList<INodeReference> getRefList() {
            return refList;
        }
    }

    public static final class Loader implements FSImageFormat.AbstractLoader {
        static final int MINIMUM_FILE_LENGTH = 8;
        private final Configuration conf;
        private final FSNamesystem fsn;
        private final LoaderContext ctx;
        /** The MD5 sum of the loaded file */
        private MD5Hash imgDigest;
        /** The transaction ID of the last edit represented by the loaded file */
        private long imgTxId;
        /**
         * Whether the image's layout version must be the same with
         * {@link HdfsServerConstants#NAMENODE_LAYOUT_VERSION}. This is only set to true
         * when we're doing (rollingUpgrade rollback).
         */
        private final boolean requireSameLayoutVersion;

        private File filename;

        Loader(Configuration conf, FSNamesystem fsn, boolean requireSameLayoutVersion) {
            this.conf = conf;
            this.fsn = fsn;
            this.ctx = new LoaderContext();
            this.requireSameLayoutVersion = requireSameLayoutVersion;
        }

        @Override
        public MD5Hash getLoadedImageMd5() {
            return imgDigest;
        }

        @Override
        public long getLoadedImageTxId() {
            return imgTxId;
        }

        public LoaderContext getLoaderContext() {
            return ctx;
        }

        /**
         * Thread to compute the MD5 of a file as this can be in parallel while
         * loading the image without interfering much.
         */
        private static class DigestThread extends Thread {

            /**
             * Exception thrown when computing the digest if it cannot be calculated.
             */
            private volatile IOException ioe = null;

            /**
             * Calculated digest if there are no error.
             */
            private volatile MD5Hash digest = null;

            /**
             * FsImage file computed MD5.
             */
            private final File file;

            DigestThread(File inFile) {
                file = inFile;
                setName(inFile.getName() + " MD5 compute");
                setDaemon(true);
            }

            public MD5Hash getDigest() throws IOException {
                if (ioe != null) {
                    throw ioe;
                }
                return digest;
            }

            public IOException getException() {
                return ioe;
            }

            @Override
            public void run() {
                try {
                    digest = MD5FileUtils.computeMd5ForFile(file);
                } catch (IOException e) {
                    ioe = e;
                } catch (Throwable t) {
                    ioe = new IOException(t);
                }
            }

            @Override
            public String toString() {
                return "DigestThread{ ThreadName=" + getName() + ", digest=" + digest + ", file=" + file + '}';
            }
        }

        void load(File file) throws IOException {
            filename = file;
            long start = Time.monotonicNow();
            DigestThread dt = new DigestThread(file);
            dt.start();
            RandomAccessFile raFile = new RandomAccessFile(file, "r");
            FileInputStream fin = new FileInputStream(file);
            try {
                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释：
                 */
                loadInternal(raFile, fin);
                try {
                    dt.join();
                    imgDigest = dt.getDigest();
                } catch (InterruptedException ie) {
                    throw new IOException(ie);
                }
                long end = Time.monotonicNow();
                LOG.info("Loaded FSImage in {} seconds.", (end - start) / 1000);
            } finally {
                fin.close();
                raFile.close();
            }
        }

        /**
         * Given a FSImage FileSummary.section, return a LimitInput stream set to
         * the starting position of the section and limited to the section length.
         * @param section The FileSummary.Section containing the offset and length
         * @param compressionCodec The compression codec in use, if any
         * @return An InputStream for the given section
         * @throws IOException
         */
        public InputStream getInputStreamForSection(FileSummary.Section section, String compressionCodec) throws IOException {
            FileInputStream fin = new FileInputStream(filename);
            try {

                FileChannel channel = fin.getChannel();
                channel.position(section.getOffset());
                InputStream in = new BufferedInputStream(new LimitInputStream(fin, section.getLength()));

                in = FSImageUtil.wrapInputStreamForCompression(conf, compressionCodec, in);
                return in;
            } catch (IOException e) {
                fin.close();
                throw e;
            }
        }

        /**
         * Takes an ArrayList of Section's and removes all Section's whose
         * name ends in _SUB, indicating they are sub-sections. The original
         * array list is modified and a new list of the removed Section's is
         * returned.
         * @param sections Array List containing all Sections and Sub Sections
         *                 in the image.
         * @return ArrayList of the sections removed, or an empty list if none are
         *         removed.
         */
        private ArrayList<FileSummary.Section> getAndRemoveSubSections(ArrayList<FileSummary.Section> sections) {
            ArrayList<FileSummary.Section> subSections = new ArrayList<>();
            Iterator<FileSummary.Section> iter = sections.iterator();
            while (iter.hasNext()) {
                FileSummary.Section s = iter.next();
                String name = s.getName();
                if (name.matches(".*_SUB$")) {
                    subSections.add(s);
                    iter.remove();
                }
            }
            return subSections;
        }

        /**
         * Given an ArrayList of Section's, return all Section's with the given
         * name, or an empty list if none are found.
         * @param sections ArrayList of the Section's to search though
         * @param name The name of the Sections to search for
         * @return ArrayList of the sections matching the given name
         */
        private ArrayList<FileSummary.Section> getSubSectionsOfName(ArrayList<FileSummary.Section> sections, SectionName name) {
            ArrayList<FileSummary.Section> subSec = new ArrayList<>();
            for (FileSummary.Section s : sections) {
                String n = s.getName();
                SectionName sectionName = SectionName.fromString(n);
                if (sectionName == name) {
                    subSec.add(s);
                }
            }
            return subSec;
        }

        /**
         * Checks the number of threads configured for parallel loading and
         * return an ExecutorService with configured number of threads. If the
         * thread count is set to less than 1, it will be reset to the default
         * value
         * @return ExecutorServie with the correct number of threads
         */
        private ExecutorService getParallelExecutorService() {
            int threads = conf.getInt(DFSConfigKeys.DFS_IMAGE_PARALLEL_THREADS_KEY, DFSConfigKeys.DFS_IMAGE_PARALLEL_THREADS_DEFAULT);
            if (threads < 1) {
                LOG.warn("Parallel is enabled and {} is set to {}. Setting to the " + "default value {}",
                        DFSConfigKeys.DFS_IMAGE_PARALLEL_THREADS_KEY, threads, DFSConfigKeys.DFS_IMAGE_PARALLEL_THREADS_DEFAULT);
                threads = DFSConfigKeys.DFS_IMAGE_PARALLEL_THREADS_DEFAULT;
            }
            ExecutorService executorService = Executors.newFixedThreadPool(threads);
            LOG.info("The fsimage will be loaded in parallel using {} threads", threads);
            return executorService;
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 读取 fsimage 文件
         */
        private void loadInternal(RandomAccessFile raFile, FileInputStream fin) throws IOException {
            if (!FSImageUtil.checkFileFormat(raFile)) {
                throw new IOException("Unrecognized file format");
            }

            // TODO_MA 马中华 注释： 首先从 fsimage 中加载得到 FileSummary
            // TODO_MA 马中华 注释： FileSummary 保存了各个 Section 的 name,length,offset 信息
            FileSummary summary = FSImageUtil.loadSummary(raFile);

            if (requireSameLayoutVersion && summary.getLayoutVersion() != HdfsServerConstants.NAMENODE_LAYOUT_VERSION) {
                throw new IOException(
                        "Image version " + summary.getLayoutVersion() + " is not equal to the software version " + HdfsServerConstants.NAMENODE_LAYOUT_VERSION);
            }

            FileChannel channel = fin.getChannel();

            FSImageFormatPBINode.Loader inodeLoader = new FSImageFormatPBINode.Loader(fsn, this);
            FSImageFormatPBSnapshot.Loader snapshotLoader = new FSImageFormatPBSnapshot.Loader(fsn, this);

            // TODO_MA 马中华 注释： Section 排序
            ArrayList<FileSummary.Section> sections = Lists.newArrayList(summary.getSectionsList());
            Collections.sort(sections, new Comparator<FileSummary.Section>() {
                @Override
                public int compare(FileSummary.Section s1, FileSummary.Section s2) {
                    SectionName n1 = SectionName.fromString(s1.getName());
                    SectionName n2 = SectionName.fromString(s2.getName());
                    if (n1 == null) {
                        return n2 == null ? 0 : -1;
                    } else if (n2 == null) {
                        return -1;
                    } else {
                        return n1.ordinal() - n2.ordinal();
                    }
                }
            });

            StartupProgress prog = NameNode.getStartupProgress();
            /**
             * beginStep() and the endStep() calls do not match the boundary of the
             * sections. This is because that the current implementation only allows
             * a particular step to be started for once.
             */
            Step currentStep = null;
            // TODO_MA 马中华 注释： 是否启用并行加载
            boolean loadInParallel = enableParallelSaveAndLoad(conf);

            ExecutorService executorService = null;
            ArrayList<FileSummary.Section> subSections = getAndRemoveSubSections(sections);
            if (loadInParallel) {
                executorService = getParallelExecutorService();
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 读取 fsimage 文件中的每个 section
             */
            for (FileSummary.Section s : sections) {

                // TODO_MA 马中华 注释： 读取一个 Section
                channel.position(s.getOffset());
                InputStream in = new BufferedInputStream(new LimitInputStream(fin, s.getLength()));
                in = FSImageUtil.wrapInputStreamForCompression(conf, summary.getCodec(), in);

                // TODO_MA 马中华 注释： section name
                String n = s.getName();
                SectionName sectionName = SectionName.fromString(n);
                if (sectionName == null) {
                    throw new IOException("Unrecognized section " + n);
                }

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 根据名字，加载每个 Section
                 */
                ArrayList<FileSummary.Section> stageSubSections;
                switch (sectionName) {
                    case NS_INFO:
                        loadNameSystemSection(in);
                        break;
                    case STRING_TABLE:
                        loadStringTableSection(in);
                        break;
                    case INODE: {
                        currentStep = new Step(StepType.INODES);
                        prog.beginStep(Phase.LOADING_FSIMAGE, currentStep);
                        stageSubSections = getSubSectionsOfName(subSections, SectionName.INODE_SUB);
                        if (loadInParallel && (stageSubSections.size() > 0)) {
                            inodeLoader.loadINodeSectionInParallel(executorService, stageSubSections, summary.getCodec(), prog, currentStep);
                        } else {
                            inodeLoader.loadINodeSection(in, prog, currentStep);
                        }
                    }
                    break;
                    case INODE_REFERENCE:
                        snapshotLoader.loadINodeReferenceSection(in);
                        break;
                    case INODE_DIR:
                        stageSubSections = getSubSectionsOfName(subSections, SectionName.INODE_DIR_SUB);
                        if (loadInParallel && stageSubSections.size() > 0) {
                            inodeLoader.loadINodeDirectorySectionInParallel(executorService, stageSubSections, summary.getCodec());
                        } else {
                            inodeLoader.loadINodeDirectorySection(in);
                        }
                        inodeLoader.waitBlocksMapAndNameCacheUpdateFinished();
                        break;
                    case FILES_UNDERCONSTRUCTION:
                        inodeLoader.loadFilesUnderConstructionSection(in);
                        break;
                    case SNAPSHOT:
                        snapshotLoader.loadSnapshotSection(in);
                        break;
                    case SNAPSHOT_DIFF:
                        snapshotLoader.loadSnapshotDiffSection(in);
                        break;
                    case SECRET_MANAGER: {
                        prog.endStep(Phase.LOADING_FSIMAGE, currentStep);
                        Step step = new Step(StepType.DELEGATION_TOKENS);
                        prog.beginStep(Phase.LOADING_FSIMAGE, step);
                        loadSecretManagerSection(in, prog, step);
                        prog.endStep(Phase.LOADING_FSIMAGE, step);
                    }
                    break;
                    case CACHE_MANAGER: {
                        Step step = new Step(StepType.CACHE_POOLS);
                        prog.beginStep(Phase.LOADING_FSIMAGE, step);
                        loadCacheManagerSection(in, prog, step);
                        prog.endStep(Phase.LOADING_FSIMAGE, step);
                    }
                    break;
                    case ERASURE_CODING:
                        Step step = new Step(StepType.ERASURE_CODING_POLICIES);
                        prog.beginStep(Phase.LOADING_FSIMAGE, step);
                        loadErasureCodingSection(in);
                        prog.endStep(Phase.LOADING_FSIMAGE, step);
                        break;
                    default:
                        LOG.warn("Unrecognized section {}", n);
                        break;
                }
            }
            if (executorService != null) {
                executorService.shutdown();
            }
        }

        private void loadNameSystemSection(InputStream in) throws IOException {
            NameSystemSection s = NameSystemSection.parseDelimitedFrom(in);
            BlockIdManager blockIdManager = fsn.getBlockManager().getBlockIdManager();
            blockIdManager.setLegacyGenerationStamp(s.getGenstampV1());
            blockIdManager.setGenerationStamp(s.getGenstampV2());
            blockIdManager.setLegacyGenerationStampLimit(s.getGenstampV1Limit());
            blockIdManager.setLastAllocatedContiguousBlockId(s.getLastAllocatedBlockId());
            if (s.hasLastAllocatedStripedBlockId()) {
                blockIdManager.setLastAllocatedStripedBlockId(s.getLastAllocatedStripedBlockId());
            }
            imgTxId = s.getTransactionId();
            if (s.hasRollingUpgradeStartTime() && fsn.getFSImage().hasRollbackFSImage()) {
                // we set the rollingUpgradeInfo only when we make sure we have the
                // rollback image
                fsn.setRollingUpgradeInfo(true, s.getRollingUpgradeStartTime());
            }
        }

        private void loadStringTableSection(InputStream in) throws IOException {
            StringTableSection s = StringTableSection.parseDelimitedFrom(in);
            ctx.stringTable = SerialNumberManager.newStringTable(s.getNumEntry(), s.getMaskBits());
            for (int i = 0; i < s.getNumEntry(); ++i) {
                StringTableSection.Entry e = StringTableSection.Entry.parseDelimitedFrom(in);
                ctx.stringTable.put(e.getId(), e.getStr());
            }
        }

        private void loadSecretManagerSection(InputStream in, StartupProgress prog, Step currentStep) throws IOException {
            SecretManagerSection s = SecretManagerSection.parseDelimitedFrom(in);
            int numKeys = s.getNumKeys(), numTokens = s.getNumTokens();
            ArrayList<SecretManagerSection.DelegationKey> keys = Lists.newArrayListWithCapacity(numKeys);
            ArrayList<SecretManagerSection.PersistToken> tokens = Lists.newArrayListWithCapacity(numTokens);

            for (int i = 0; i < numKeys; ++i)
                keys.add(SecretManagerSection.DelegationKey.parseDelimitedFrom(in));

            prog.setTotal(Phase.LOADING_FSIMAGE, currentStep, numTokens);
            Counter counter = prog.getCounter(Phase.LOADING_FSIMAGE, currentStep);
            for (int i = 0; i < numTokens; ++i) {
                tokens.add(SecretManagerSection.PersistToken.parseDelimitedFrom(in));
            }

            fsn.loadSecretManagerState(s, keys, tokens, counter);
        }

        private void loadCacheManagerSection(InputStream in, StartupProgress prog, Step currentStep) throws IOException {
            CacheManagerSection s = CacheManagerSection.parseDelimitedFrom(in);
            int numPools = s.getNumPools();
            ArrayList<CachePoolInfoProto> pools = Lists.newArrayListWithCapacity(numPools);
            ArrayList<CacheDirectiveInfoProto> directives = Lists.newArrayListWithCapacity(s.getNumDirectives());
            prog.setTotal(Phase.LOADING_FSIMAGE, currentStep, numPools);
            Counter counter = prog.getCounter(Phase.LOADING_FSIMAGE, currentStep);
            for (int i = 0; i < numPools; ++i) {
                pools.add(CachePoolInfoProto.parseDelimitedFrom(in));
                counter.increment();
            }
            for (int i = 0; i < s.getNumDirectives(); ++i)
                directives.add(CacheDirectiveInfoProto.parseDelimitedFrom(in));
            fsn.getCacheManager().loadState(new CacheManager.PersistState(s, pools, directives));
        }

        private void loadErasureCodingSection(InputStream in) throws IOException {
            ErasureCodingSection s = ErasureCodingSection.parseDelimitedFrom(in);
            List<ErasureCodingPolicyInfo> ecPolicies = Lists.newArrayListWithCapacity(s.getPoliciesCount());
            for (int i = 0; i < s.getPoliciesCount(); ++i) {
                ecPolicies.add(PBHelperClient.convertErasureCodingPolicyInfo(s.getPolicies(i)));
            }
            fsn.getErasureCodingPolicyManager().loadPolicies(ecPolicies, conf);
        }
    }

    private static boolean enableParallelSaveAndLoad(Configuration conf) {
        boolean loadInParallel = enableParallelLoad;
        boolean compressionEnabled = conf.getBoolean(DFSConfigKeys.DFS_IMAGE_COMPRESS_KEY, DFSConfigKeys.DFS_IMAGE_COMPRESS_DEFAULT);

        if (loadInParallel) {
            if (compressionEnabled) {
                LOG.warn("Parallel Image loading and saving is not supported when {}" + " is set to true. Parallel will be disabled.",
                        DFSConfigKeys.DFS_IMAGE_COMPRESS_KEY);
                loadInParallel = false;
            }
        }
        return loadInParallel;
    }

    public static void initParallelLoad(Configuration conf) {
        // TODO_MA 马中华 注释： dfs.image.parallel.load = false
        enableParallelLoad = conf.getBoolean(DFSConfigKeys.DFS_IMAGE_PARALLEL_LOAD_KEY, DFSConfigKeys.DFS_IMAGE_PARALLEL_LOAD_DEFAULT);
    }

    public static void refreshParallelSaveAndLoad(boolean enable) {
        enableParallelLoad = enable;
    }

    public static boolean getEnableParallelLoad() {
        return enableParallelLoad;
    }

    public static final class Saver {
        public static final int CHECK_CANCEL_INTERVAL = 4096;
        private boolean writeSubSections = false;
        private int inodesPerSubSection = Integer.MAX_VALUE;

        private final SaveNamespaceContext context;
        private final SaverContext saverContext;
        private long currentOffset = FSImageUtil.MAGIC_HEADER.length;
        private long subSectionOffset = currentOffset;
        private MD5Hash savedDigest;

        private FileChannel fileChannel;
        // OutputStream for the section data
        private OutputStream sectionOutputStream;
        private CompressionCodec codec;
        private OutputStream underlyingOutputStream;
        private Configuration conf;

        Saver(SaveNamespaceContext context, Configuration conf) {
            this.context = context;
            this.saverContext = new SaverContext();
            this.conf = conf;
        }

        public MD5Hash getSavedDigest() {
            return savedDigest;
        }

        public SaveNamespaceContext getContext() {
            return context;
        }

        public SaverContext getSaverContext() {
            return saverContext;
        }

        public int getInodesPerSubSection() {
            return inodesPerSubSection;
        }

        /**
         * Commit the length and offset of a fsimage section to the summary index,
         * including the sub section, which will be committed before the section is
         * committed.
         * @param summary The image summary object
         * @param name The name of the section to commit
         * @param subSectionName The name of the sub-section to commit
         * @throws IOException
         */
        public void commitSectionAndSubSection(FileSummary.Builder summary, SectionName name, SectionName subSectionName) throws IOException {
            commitSubSection(summary, subSectionName);
            commitSection(summary, name);
        }

        public void commitSection(FileSummary.Builder summary, SectionName name) throws IOException {
            long oldOffset = currentOffset;
            flushSectionOutputStream();

            if (codec != null) {
                sectionOutputStream = codec.createOutputStream(underlyingOutputStream);
            } else {
                sectionOutputStream = underlyingOutputStream;
            }
            long length = fileChannel.position() - oldOffset;

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 添加一个 Section
             *  设置了三个重要信息：
             *  1、name
             *  2、length
             *  3、offset
             */
            summary.addSections(FileSummary.Section.newBuilder().setName(name.name).setLength(length).setOffset(currentOffset));

            currentOffset += length;
            subSectionOffset = currentOffset;
        }

        /**
         * Commit the length and offset of a fsimage sub-section to the summary
         * index.
         * @param summary The image summary object
         * @param name The name of the sub-section to commit
         * @throws IOException
         */
        public void commitSubSection(FileSummary.Builder summary, SectionName name) throws IOException {
            if (!writeSubSections) {
                return;
            }

            LOG.debug("Saving a subsection for {}", name.toString());
            // The output stream must be flushed before the length is obtained
            // as the flush can move the length forward.
            sectionOutputStream.flush();
            long length = fileChannel.position() - subSectionOffset;
            if (length == 0) {
                LOG.warn("The requested section for {} is empty. It will not be " + "output to the image", name.toString());
                return;
            }
            summary.addSections(FileSummary.Section.newBuilder().setName(name.name).setLength(length).setOffset(subSectionOffset));
            subSectionOffset += length;
        }

        private void flushSectionOutputStream() throws IOException {
            if (codec != null) {
                ((CompressionOutputStream) sectionOutputStream).finish();
            }
            sectionOutputStream.flush();
        }

        /**
         * @return number of non-fatal errors detected while writing the image.
         * @throws IOException on fatal error.
         */
        long save(File file, FSImageCompression compression) throws IOException {
            enableSubSectionsIfRequired();

            // TODO_MA 马中华 注释： 构建文件输出流
            FileOutputStream fout = new FileOutputStream(file);
            fileChannel = fout.getChannel();
            try {
                LOG.info("Saving image file {} using {}", file, compression);
                long startTime = monotonicNow();

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 保存命名空间到 fsimage
                 */
                long numErrors = saveInternal(fout, compression, file.getAbsolutePath());
                LOG.info("Image file {} of size {} bytes saved in {} seconds {}.", file, file.length(), (monotonicNow() - startTime) / 1000,
                        (numErrors > 0 ? (" with" + numErrors + " errors") : ""));
                return numErrors;
            } finally {
                fout.close();
            }
        }

        private void enableSubSectionsIfRequired() {
            boolean parallelEnabled = enableParallelSaveAndLoad(conf);
            int inodeThreshold = conf.getInt(DFSConfigKeys.DFS_IMAGE_PARALLEL_INODE_THRESHOLD_KEY,
                    DFSConfigKeys.DFS_IMAGE_PARALLEL_INODE_THRESHOLD_DEFAULT);
            int targetSections = conf.getInt(DFSConfigKeys.DFS_IMAGE_PARALLEL_TARGET_SECTIONS_KEY,
                    DFSConfigKeys.DFS_IMAGE_PARALLEL_TARGET_SECTIONS_DEFAULT);

            if (parallelEnabled) {
                if (targetSections <= 0) {
                    LOG.warn("{} is set to {}. It must be greater than zero. Setting to" + " default of {}",
                            DFSConfigKeys.DFS_IMAGE_PARALLEL_TARGET_SECTIONS_KEY, targetSections,
                            DFSConfigKeys.DFS_IMAGE_PARALLEL_TARGET_SECTIONS_DEFAULT);
                    targetSections = DFSConfigKeys.DFS_IMAGE_PARALLEL_TARGET_SECTIONS_DEFAULT;
                }
                if (inodeThreshold <= 0) {
                    LOG.warn("{} is set to {}. It must be greater than zero. Setting to" + " default of {}",
                            DFSConfigKeys.DFS_IMAGE_PARALLEL_INODE_THRESHOLD_KEY, inodeThreshold,
                            DFSConfigKeys.DFS_IMAGE_PARALLEL_INODE_THRESHOLD_DEFAULT);
                    inodeThreshold = DFSConfigKeys.DFS_IMAGE_PARALLEL_INODE_THRESHOLD_DEFAULT;
                }
                int inodeCount = context.getSourceNamesystem().dir.getInodeMapSize();
                // Only enable parallel sections if there are enough inodes
                if (inodeCount >= inodeThreshold) {
                    writeSubSections = true;
                    // Calculate the inodes per section rounded up to the nearest int
                    inodesPerSubSection = (inodeCount + targetSections - 1) / targetSections;
                }
            } else {
                writeSubSections = false;
            }
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 保存 FileSummary
         */
        private static void saveFileSummary(OutputStream out, FileSummary summary) throws IOException {
            summary.writeDelimitedTo(out);
            int length = getOndiskTrunkSize(summary);
            byte[] lengthBytes = new byte[4];
            ByteBuffer.wrap(lengthBytes).asIntBuffer().put(length);
            out.write(lengthBytes);
        }

        private long saveInodes(FileSummary.Builder summary) throws IOException {
            FSImageFormatPBINode.Saver saver = new FSImageFormatPBINode.Saver(this, summary);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 保存 INodeSection
             */
            saver.serializeINodeSection(sectionOutputStream);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 保存 INodeDirectorySection
             */
            saver.serializeINodeDirectorySection(sectionOutputStream);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 保存 FilesUCSection 部分
             */
            saver.serializeFilesUCSection(sectionOutputStream);

            return saver.getNumImageErrors();
        }

        /**
         * @return number of non-fatal errors detected while saving the image.
         * @throws IOException on fatal error.
         */
        private long saveSnapshots(FileSummary.Builder summary) throws IOException {
            FSImageFormatPBSnapshot.Saver snapshotSaver = new FSImageFormatPBSnapshot.Saver(this, summary, context,
                    context.getSourceNamesystem());

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： SnapshotSection
             */
            snapshotSaver.serializeSnapshotSection(sectionOutputStream);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： SnapshotDiffSection
             */
            // Skip snapshot-related sections when there is no snapshot.
            if (context.getSourceNamesystem().getSnapshotManager().getNumSnapshots() > 0) {
                snapshotSaver.serializeSnapshotDiffSection(sectionOutputStream);
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： INodeReferenceSection
             */
            snapshotSaver.serializeINodeReferenceSection(sectionOutputStream);
            return snapshotSaver.getNumImageErrors();
        }

        /**
         * // TODO_MA 马中华 注释： saveInternal()方法以section为单位写入元数据信息时，
         * // TODO_MA 马中华 注释： 还会在FileSummary中记录这个section的长度，
         * // TODO_MA 马中华 注释： 以及 section 在 fsimage 文件中的起始位置等信息。
         * // TODO_MA 马中华 注释： 当完成了所有 section 的写入后， FileSummary 对象也就构造完毕了
         * // TODO_MA 马中华 注释： .saveInternal() 最后会将FileSummary对象写入fsimage文件中。
         * @return number of non-fatal errors detected while writing the FsImage.
         * @throws IOException on fatal error.
         */
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 核心步骤
         *  1、首先在 FileSummary 中记录 ondiskVersion、 layoutVersion、 codec 等信息
         *  2、向 fsimage 中，依次记录
         *      NameSystemSection 命名空间信息
         *      ErasureCodingSection 纠删码信息
         *      INodeSection 文件系统节点信息
         *      SnapshotSection 快照信息
         *      SecretManagerSection 安全信息
         *      CacheManagerSection 缓存信息
         *      StringTableSection 缓存信息
         *  上述每个 Section 写入 fsimage 的时候，都是按照 fsimage.proto 定义的格式写入的
         *  3、FileSummary 会记录每个 Section 的 name,length,offset 三个信息，最后将 FileSummary 也写入 FileSummary
         */
        private long saveInternal(FileOutputStream fout, FSImageCompression compression, String filePath) throws IOException {
            StartupProgress prog = NameNode.getStartupProgress();
            MessageDigest digester = MD5Hash.getDigester();
            int layoutVersion = context.getSourceNamesystem().getEffectiveLayoutVersion();

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 构建一个封装输出流，一边写数据，一边校验
             */
            underlyingOutputStream = new DigestOutputStream(new BufferedOutputStream(fout), digester);

            // TODO_MA 马中华 注释： 1、先写出 MAGIC
            underlyingOutputStream.write(FSImageUtil.MAGIC_HEADER);

            fileChannel = fout.getChannel();

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 2、写出 SECTIONS
             */
            FileSummary.Builder b = FileSummary.newBuilder()
                    // TODO_MA 马中华 注释： fsimage 文件的版本号
                    .setOndiskVersion(FSImageUtil.FILE_VERSION)
                    // TODO_MA 马中华 注释： HDFS 的文件系统布局版本号
                    .setLayoutVersion(context.getSourceNamesystem().getEffectiveLayoutVersion());
            // TODO_MA 马中华 注释： 压缩信息
            codec = compression.getImageCodec();
            if (codec != null) {
                b.setCodec(codec.getClass().getCanonicalName());
                sectionOutputStream = codec.createOutputStream(underlyingOutputStream);
            } else {
                sectionOutputStream = underlyingOutputStream;
            }
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 3、保存文件信息元数据信息的 Section
             */
            saveNameSystemSection(b);

            // Check for cancellation right after serializing the name system section.
            // Some unit tests, such as TestSaveNamespace#testCancelSaveNameSpace
            // depends on this behavior.
            context.checkCancelled();

            Step step;

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 4、保存 ErasureCoding 纠删码的 section
             */
            // Erasure coding policies should be saved before inodes
            if (NameNodeLayoutVersion.supports(NameNodeLayoutVersion.Feature.ERASURE_CODING, layoutVersion)) {
                step = new Step(StepType.ERASURE_CODING_POLICIES, filePath);
                prog.beginStep(Phase.SAVING_CHECKPOINT, step);
                saveErasureCodingSection(b);
                prog.endStep(Phase.SAVING_CHECKPOINT, step);
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             *  5、保存 INode Section
             *  6、保存 Snapshot Section
             */
            step = new Step(StepType.INODES, filePath);
            prog.beginStep(Phase.SAVING_CHECKPOINT, step);
            // Count number of non-fatal errors when saving inodes and snapshots.
            long numErrors = saveInodes(b);
            numErrors += saveSnapshots(b);
            prog.endStep(Phase.SAVING_CHECKPOINT, step);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 7、保存 SecretManager Section
             */
            step = new Step(StepType.DELEGATION_TOKENS, filePath);
            prog.beginStep(Phase.SAVING_CHECKPOINT, step);
            saveSecretManagerSection(b);
            prog.endStep(Phase.SAVING_CHECKPOINT, step);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 8、保存 CacheManager Section
             */
            step = new Step(StepType.CACHE_POOLS, filePath);
            prog.beginStep(Phase.SAVING_CHECKPOINT, step);
            saveCacheManagerSection(b);
            prog.endStep(Phase.SAVING_CHECKPOINT, step);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 9、保存 String Table Section
             */
            saveStringTableSection(b);

            // TODO_MA 马中华 注释： 执行 flush
            // We use the underlyingOutputStream to write the header. Therefore flush
            // the buffered stream (which is potentially compressed) first.
            flushSectionOutputStream();

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 10、保存 FileSummary
             */
            FileSummary summary = b.build();
            saveFileSummary(underlyingOutputStream, summary);

            underlyingOutputStream.close();
            savedDigest = new MD5Hash(digester.digest());
            return numErrors;
        }

        private void saveSecretManagerSection(FileSummary.Builder summary) throws IOException {
            final FSNamesystem fsn = context.getSourceNamesystem();
            DelegationTokenSecretManager.SecretManagerState state = fsn.saveSecretManagerState();
            state.section.writeDelimitedTo(sectionOutputStream);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： SecretManagerSection.DelegationKey
             */
            for (SecretManagerSection.DelegationKey k : state.keys)
                k.writeDelimitedTo(sectionOutputStream);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： SecretManagerSection.PersistToken
             */
            for (SecretManagerSection.PersistToken t : state.tokens)
                t.writeDelimitedTo(sectionOutputStream);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：SectionName.SECRET_MANAGER
             */
            commitSection(summary, SectionName.SECRET_MANAGER);
        }

        private void saveCacheManagerSection(FileSummary.Builder summary) throws IOException {
            final FSNamesystem fsn = context.getSourceNamesystem();
            CacheManager.PersistState state = fsn.getCacheManager().saveState();
            state.section.writeDelimitedTo(sectionOutputStream);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： CachePoolInfoProto
             */
            for (CachePoolInfoProto p : state.pools)
                p.writeDelimitedTo(sectionOutputStream);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： CacheDirectiveInfoProto
             */
            for (CacheDirectiveInfoProto p : state.directives)
                p.writeDelimitedTo(sectionOutputStream);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 执行 flush
             */
            commitSection(summary, SectionName.CACHE_MANAGER);
        }

        private void saveErasureCodingSection(FileSummary.Builder summary) throws IOException {
            final FSNamesystem fsn = context.getSourceNamesystem();
            ErasureCodingPolicyInfo[] ecPolicies = fsn.getErasureCodingPolicyManager().getPersistedPolicies();
            ArrayList<ErasureCodingPolicyProto> ecPolicyProtoes = new ArrayList<ErasureCodingPolicyProto>();
            for (ErasureCodingPolicyInfo p : ecPolicies) {
                ecPolicyProtoes.add(PBHelperClient.convertErasureCodingPolicy(p));
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 构建 ErasureCodingSection
             */
            ErasureCodingSection section = ErasureCodingSection.newBuilder().addAllPolicies(ecPolicyProtoes).build();
            section.writeDelimitedTo(sectionOutputStream);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 写入 ErasureCodingSection
             */
            commitSection(summary, SectionName.ERASURE_CODING);
        }

        private void saveNameSystemSection(FileSummary.Builder summary) throws IOException {
            final FSNamesystem fsn = context.getSourceNamesystem();
            OutputStream out = sectionOutputStream;
            BlockIdManager blockIdManager = fsn.getBlockManager().getBlockIdManager();

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 构建 NameSystemSection
             */
            NameSystemSection.Builder b = NameSystemSection.newBuilder()
                    .setGenstampV1(blockIdManager.getLegacyGenerationStamp())
                    .setGenstampV1Limit(blockIdManager.getLegacyGenerationStampLimit())
                    .setGenstampV2(blockIdManager.getGenerationStamp())
                    .setLastAllocatedBlockId(blockIdManager.getLastAllocatedContiguousBlockId())
                    .setLastAllocatedStripedBlockId(blockIdManager.getLastAllocatedStripedBlockId())
                    .setTransactionId(context.getTxId());

            // We use the non-locked version of getNamespaceInfo here since
            // the coordinating thread of saveNamespace already has read-locked
            // the namespace for us. If we attempt to take another readlock
            // from the actual saver thread, there's a potential of a
            // fairness-related deadlock. See the comments on HDFS-2223.
            b.setNamespaceId(fsn.unprotectedGetNamespaceInfo().getNamespaceID());
            if (fsn.isRollingUpgrade()) {
                b.setRollingUpgradeStartTime(fsn.getRollingUpgradeInfo().getStartTime());
            }
            NameSystemSection s = b.build();
            s.writeDelimitedTo(out);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 写入 NameSystemSection
             */
            commitSection(summary, SectionName.NS_INFO);
        }

        private void saveStringTableSection(FileSummary.Builder summary) throws IOException {
            OutputStream out = sectionOutputStream;

            SerialNumberManager.StringTable stringTable = SerialNumberManager.getStringTable();

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 构建 StringTableSection
             */
            StringTableSection.Builder b = StringTableSection.newBuilder().setNumEntry(stringTable.size())
                    .setMaskBits(stringTable.getMaskBits());
            b.build().writeDelimitedTo(out);
            for (Entry<Integer, String> e : stringTable) {
                StringTableSection.Entry.Builder eb = StringTableSection.Entry.newBuilder().setId(e.getKey()).setStr(e.getValue());
                eb.build().writeDelimitedTo(out);
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： flush SectionName.STRING_TABLE
             */
            commitSection(summary, SectionName.STRING_TABLE);
        }
    }

    /**
     * Supported section name. The order of the enum determines the order of loading.
     * // TODO_MA 马中华 注释： 下述的序号，是 写入 fsimage 文件的顺序编号
     */
    public enum SectionName {
        NS_INFO("NS_INFO"),                     // TODO_MA 马中华 注释： 1、NameSystemSection
        STRING_TABLE("STRING_TABLE"),           // TODO_MA 马中华 注释： 11、StringTableSection
        EXTENDED_ACL("EXTENDED_ACL"),
        ERASURE_CODING("ERASURE_CODING"),       // TODO_MA 马中华 注释： 2、ErasureCodingSection
        INODE("INODE"),                         // TODO_MA 马中华 注释： 3、INodeSection
        INODE_SUB("INODE_SUB"),
        INODE_REFERENCE("INODE_REFERENCE"),     // TODO_MA 马中华 注释： 8、INodeReferenceSection
        INODE_REFERENCE_SUB("INODE_REFERENCE_SUB"),
        SNAPSHOT("SNAPSHOT"),                   // TODO_MA 马中华 注释： 6、SnapshotSection
        INODE_DIR("INODE_DIR"),                 // TODO_MA 马中华 注释： 4、INodeDirectorySection
        INODE_DIR_SUB("INODE_DIR_SUB"),
        FILES_UNDERCONSTRUCTION("FILES_UNDERCONSTRUCTION"),         // TODO_MA 马中华 注释： 5、FileUnderConstructionEntry
        SNAPSHOT_DIFF("SNAPSHOT_DIFF"),         // TODO_MA 马中华 注释： 7、SnapshotDiffSection
        SNAPSHOT_DIFF_SUB("SNAPSHOT_DIFF_SUB"),
        SECRET_MANAGER("SECRET_MANAGER"),       // TODO_MA 马中华 注释： 9、SecretManagerSection
        CACHE_MANAGER("CACHE_MANAGER");         // TODO_MA 马中华 注释： 10、CacheManagerSection

        private static final SectionName[] values = SectionName.values();

        public static SectionName fromString(String name) {
            for (SectionName n : values) {
                if (n.name.equals(name)) return n;
            }
            return null;
        }

        private final String name;

        private SectionName(String name) {
            this.name = name;
        }
    }

    private static int getOndiskTrunkSize(org.apache.hadoop.thirdparty.protobuf.GeneratedMessageV3 s) {
        return CodedOutputStream.computeUInt32SizeNoTag(s.getSerializedSize()) + s.getSerializedSize();
    }

    private FSImageFormatProtobuf() {
    }
}
