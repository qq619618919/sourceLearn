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
package org.apache.hadoop.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.CanSetDropBehind;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.fs.impl.StoreImplementationUtils;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.QuotaByStorageTypeExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketReceiver;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenode.RetryStartFileException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.hdfs.util.ByteArrayManager;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DataChecksum.Type;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Time;
import org.apache.htrace.core.TraceScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;


/****************************************************************
 * DFSOutputStream creates files from a stream of bytes.
 *
 * The client application writes data that is cached internally by
 * this stream. Data is broken up into packets, each packet is
 * typically 64K in size. A packet comprises of chunks. Each chunk
 * is typically 512 bytes and has an associated checksum with it.
 *
 * When a client application fills up the currentPacket, it is
 * enqueued into the dataQueue of DataStreamer. DataStreamer is a
 * thread that picks up packets from the dataQueue and sends it to
 * the first datanode in the pipeline.
 *
 ****************************************************************/
@InterfaceAudience.Private
public class DFSOutputStream extends FSOutputSummer implements Syncable, CanSetDropBehind, StreamCapabilities {
    static final Logger LOG = LoggerFactory.getLogger(DFSOutputStream.class);
    /**
     * Number of times to retry creating a file when there are transient
     * errors (typically related to encryption zones and KeyProvider operations).
     */
    @VisibleForTesting
    static final int CREATE_RETRY_COUNT = 10;
    @VisibleForTesting
    static CryptoProtocolVersion[] SUPPORTED_CRYPTO_VERSIONS = CryptoProtocolVersion.supported();

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：
     */
    protected final DFSClient dfsClient;
    protected final ByteArrayManager byteArrayManager;
    // closed is accessed by different threads under different locks.
    protected volatile boolean closed = false;

    protected final String src;
    protected final long fileId;
    protected final long blockSize;
    protected final int bytesPerChecksum;

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 
     */
    protected DFSPacket currentPacket = null;
    protected DataStreamer streamer;
    protected int packetSize = 0; // write packet size, not including the header.
    protected int chunksPerPacket = 0;
    protected long lastFlushOffset = 0; // offset when flush was invoked
    protected long initialFileSize = 0; // at time of file open
    private final short blockReplication; // replication factor of file
    protected boolean shouldSyncBlock = false; // force blocks to disk upon close
    private final EnumSet<AddBlockFlag> addBlockFlags;
    protected final AtomicReference<CachingStrategy> cachingStrategy;
    private FileEncryptionInfo fileEncryptionInfo;
    private int writePacketSize;

    /** Use {@link ByteArrayManager} to create buffer for non-heartbeat packets.*/
    protected DFSPacket createPacket(int packetSize, int chunksPerPkt, long offsetInBlock, long seqno,
                                     boolean lastPacketInBlock) throws InterruptedIOException {
        final byte[] buf;

        // TODO_MA 马中华 注释： bufferSize = 33 + 65016 = 65049
        final int bufferSize = PacketHeader.PKT_MAX_HEADER_LEN + packetSize;

        try {
            // TODO_MA 马中华 注释： 构建一个长度为 bufferSize 的 byte[] 数组
            buf = byteArrayManager.newByteArray(bufferSize);
        } catch (InterruptedException ie) {
            final InterruptedIOException iioe = new InterruptedIOException("seqno=" + seqno);
            iioe.initCause(ie);
            throw iioe;
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return new DFSPacket(buf, chunksPerPkt, offsetInBlock, seqno, getChecksumSize(), lastPacketInBlock);
    }

    @Override
    protected void checkClosed() throws IOException {
        if (isClosed()) {
            getStreamer().getLastException().throwException4Close();
        }
    }

    //
    // returns the list of targets, if any, that is being currently used.
    //
    @VisibleForTesting
    public synchronized DatanodeInfo[] getPipeline() {
        if (getStreamer().streamerClosed()) {
            return null;
        }
        DatanodeInfo[] currentNodes = getStreamer().getNodes();
        if (currentNodes == null) {
            return null;
        }
        DatanodeInfo[] value = new DatanodeInfo[currentNodes.length];
        System.arraycopy(currentNodes, 0, value, 0, currentNodes.length);
        return value;
    }

    /**
     * @return the object for computing checksum.
     *         The type is NULL if checksum is not computed.
     */
    private static DataChecksum getChecksum4Compute(DataChecksum checksum, HdfsFileStatus stat) {
        if (DataStreamer.isLazyPersist(stat) && stat.getReplication() == 1) {
            // do not compute checksum for writing to single replica to memory
            return DataChecksum.newDataChecksum(Type.NULL, checksum.getBytesPerChecksum());
        }
        return checksum;
    }

    private DFSOutputStream(DFSClient dfsClient, String src, EnumSet<CreateFlag> flag, Progressable progress, HdfsFileStatus stat,
                            DataChecksum checksum) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        super(getChecksum4Compute(checksum, stat));
        this.dfsClient = dfsClient;
        this.src = src;
        this.fileId = stat.getFileId();
        this.blockSize = stat.getBlockSize();
        this.blockReplication = stat.getReplication();
        this.fileEncryptionInfo = stat.getFileEncryptionInfo();
        this.cachingStrategy = new AtomicReference<>(dfsClient.getDefaultWriteCachingStrategy());
        this.addBlockFlags = EnumSet.noneOf(AddBlockFlag.class);
        if (flag.contains(CreateFlag.NO_LOCAL_WRITE)) {
            this.addBlockFlags.add(AddBlockFlag.NO_LOCAL_WRITE);
        }
        if (flag.contains(CreateFlag.NO_LOCAL_RACK)) {
            this.addBlockFlags.add(AddBlockFlag.NO_LOCAL_RACK);
        }
        if (flag.contains(CreateFlag.IGNORE_CLIENT_LOCALITY)) {
            this.addBlockFlags.add(AddBlockFlag.IGNORE_CLIENT_LOCALITY);
        }
        if (progress != null) {
            DFSClient.LOG.debug("Set non-null progress callback on DFSOutputStream " + "{}", src);
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 默认 64kb， 最大不能超过 16M
         */
        initWritePacketSize();

        this.bytesPerChecksum = checksum.getBytesPerChecksum();
        if (bytesPerChecksum <= 0) {
            throw new HadoopIllegalArgumentException("Invalid value: bytesPerChecksum = " + bytesPerChecksum + " <= 0");
        }
        if (blockSize % bytesPerChecksum != 0) {
            throw new HadoopIllegalArgumentException(
                    "Invalid values: " + HdfsClientConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY + " (=" + bytesPerChecksum + ") must divide block size (=" + blockSize + ").");
        }
        this.byteArrayManager = dfsClient.getClientContext().getByteArrayManager();
    }

    /**
     * Ensures the configured writePacketSize never exceeds
     * PacketReceiver.MAX_PACKET_SIZE.
     */
    private void initWritePacketSize() {

        // TODO_MA 马中华 注释： 获取 packetsize
        writePacketSize = dfsClient.getConf().getWritePacketSize();

        // TODO_MA 马中华 注释： packetsize 不能超过 16M
        if (writePacketSize > PacketReceiver.MAX_PACKET_SIZE) {
            LOG.warn("Configured write packet exceeds {} bytes as max," + " using {} bytes.", PacketReceiver.MAX_PACKET_SIZE,
                    PacketReceiver.MAX_PACKET_SIZE
            );
            writePacketSize = PacketReceiver.MAX_PACKET_SIZE;
        }
    }

    /** Construct a new output stream for creating a file. */
    protected DFSOutputStream(DFSClient dfsClient, String src, HdfsFileStatus stat, EnumSet<CreateFlag> flag, Progressable progress,
                              DataChecksum checksum, String[] favoredNodes, boolean createStreamer) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        this(dfsClient, src, flag, progress, stat, checksum);
        this.shouldSyncBlock = flag.contains(CreateFlag.SYNC_BLOCK);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 重要：计算出来写数据的时候，需要的关于 packet、 chunk 等的信息
         *  bytesPerChecksum = 512b
         *  dfs.client-write-packet-size = 64kb
         *  1、directory
         *  2、file
         *  3、block = 128M
         *  4、packet = 64KB
         *  5、chunk = 512b
         *  由于有校验数据，checksumsize = 4b ， 所以一个完整的 chunksize = 512 + 4 = 516B
         *  那么 （64KB - 头信息） / 516B 取整 = 126 最多
         */
        computePacketChunkSize(dfsClient.getConf().getWritePacketSize(), bytesPerChecksum);

        if (createStreamer) {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 构造 DataStreamer 线程
             */
            streamer = new DataStreamer(stat, null, dfsClient, src, progress, checksum, cachingStrategy, byteArrayManager,
                    favoredNodes, addBlockFlags
            );
        }
    }

    static DFSOutputStream newStreamForCreate(DFSClient dfsClient, String src, FsPermission masked, EnumSet<CreateFlag> flag,
                                              boolean createParent, short replication, long blockSize, Progressable progress,
                                              DataChecksum checksum, String[] favoredNodes, String ecPolicyName,
                                              String storagePolicy) throws IOException {
        try (TraceScope ignored = dfsClient.newPathTraceScope("newStreamForCreate", src)) {

            // TODO_MA 马中华 注释： 这个文件有一些描述信息： 文件大小，数据副本个数，数据块大小，权限，路径,....
            HdfsFileStatus stat = null;

            // Retry the create if we get a RetryStartFileException up to a maximum
            // number of times
            boolean shouldRetry = true;
            int retryCount = CREATE_RETRY_COUNT;
            while (shouldRetry) {
                shouldRetry = false;
                try {

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 发送 RPC 请求给 NameNode 创建文件
                     *  HdfsFileStatus stat = 刚才创建的文件的元数据！
                     */
                    stat = dfsClient.namenode.create(src, masked, dfsClient.clientName, new EnumSetWritable<>(flag), createParent,
                            replication, blockSize, SUPPORTED_CRYPTO_VERSIONS, ecPolicyName, storagePolicy
                    );
                    break;
                } catch (RemoteException re) {
                    IOException e = re.unwrapRemoteException(AccessControlException.class, DSQuotaExceededException.class,
                            QuotaByStorageTypeExceededException.class, FileAlreadyExistsException.class,
                            FileNotFoundException.class, ParentNotDirectoryException.class, NSQuotaExceededException.class,
                            RetryStartFileException.class, SafeModeException.class, UnresolvedPathException.class,
                            SnapshotAccessControlException.class, UnknownCryptoProtocolVersionException.class
                    );
                    if (e instanceof RetryStartFileException) {
                        if (retryCount > 0) {
                            shouldRetry = true;
                            retryCount--;
                        } else {
                            throw new IOException("Too many retries because of encryption" + " zone operations", e);
                        }
                    } else {
                        throw e;
                    }
                }
            }
            Preconditions.checkNotNull(stat, "HdfsFileStatus should not be null!");

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 构造一个输出流，返回
             */
            final DFSOutputStream out;

            // TODO_MA 马中华 注释： 纠删码
            if (stat.getErasureCodingPolicy() != null) {
                out = new DFSStripedOutputStream(dfsClient, src, stat, flag, progress, checksum, favoredNodes);
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 副本机制, 内部最重要的事情就是 创建 DataStreamer ，是一个线程
             */
            else {
                out = new DFSOutputStream(dfsClient, src, stat, flag, progress, checksum, favoredNodes, true);
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 启动内部的 DataStreamer 线程
             */
            out.start();

            // TODO_MA 马中华 注释：
            return out;
        }
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：
     */
    /** Construct a new output stream for append. */
    private DFSOutputStream(DFSClient dfsClient, String src, EnumSet<CreateFlag> flags, Progressable progress,
                            LocatedBlock lastBlock, HdfsFileStatus stat, DataChecksum checksum,
                            String[] favoredNodes) throws IOException {
        this(dfsClient, src, flags, progress, stat, checksum);
        initialFileSize = stat.getLen(); // length of file when opened
        this.shouldSyncBlock = flags.contains(CreateFlag.SYNC_BLOCK);

        boolean toNewBlock = flags.contains(CreateFlag.NEW_BLOCK);

        this.fileEncryptionInfo = stat.getFileEncryptionInfo();

        // The last partial block of the file has to be filled.
        if (!toNewBlock && lastBlock != null) {
            // indicate that we are appending to an existing block
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            streamer = new DataStreamer(lastBlock, stat, dfsClient, src, progress, checksum, cachingStrategy, byteArrayManager);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            getStreamer().setBytesCurBlock(lastBlock.getBlockSize());
            adjustPacketChunkSize(stat);
            getStreamer().setPipelineInConstruction(lastBlock);
        } else {
            computePacketChunkSize(dfsClient.getConf().getWritePacketSize(), bytesPerChecksum);
            streamer = new DataStreamer(stat, lastBlock != null ? lastBlock.getBlock() : null, dfsClient, src, progress, checksum,
                    cachingStrategy, byteArrayManager, favoredNodes, addBlockFlags
            );
        }
    }

    private void adjustPacketChunkSize(HdfsFileStatus stat) throws IOException {

        long usedInLastBlock = stat.getLen() % blockSize;
        int freeInLastBlock = (int) (blockSize - usedInLastBlock);

        // calculate the amount of free space in the pre-existing
        // last crc chunk
        int usedInCksum = (int) (stat.getLen() % bytesPerChecksum);
        int freeInCksum = bytesPerChecksum - usedInCksum;

        // if there is space in the last block, then we have to
        // append to that block
        if (freeInLastBlock == blockSize) {
            throw new IOException("The last block for file " + src + " is full.");
        }

        if (usedInCksum > 0 && freeInCksum > 0) {
            // if there is space in the last partial chunk, then
            // setup in such a way that the next packet will have only
            // one chunk that fills up the partial chunk.
            //
            computePacketChunkSize(0, freeInCksum);
            setChecksumBufSize(freeInCksum);
            getStreamer().setAppendChunk(true);
        } else {
            // if the remaining space in the block is smaller than
            // that expected size of of a packet, then create
            // smaller size packet.
            //
            computePacketChunkSize(Math.min(dfsClient.getConf().getWritePacketSize(), freeInLastBlock), bytesPerChecksum);
        }
    }

    static DFSOutputStream newStreamForAppend(DFSClient dfsClient, String src, EnumSet<CreateFlag> flags, Progressable progress,
                                              LocatedBlock lastBlock, HdfsFileStatus stat, DataChecksum checksum,
                                              String[] favoredNodes) throws IOException {
        try (TraceScope ignored = dfsClient.newPathTraceScope("newStreamForAppend", src)) {
            DFSOutputStream out;
            if (stat.isErasureCoded()) {
                out = new DFSStripedOutputStream(dfsClient, src, flags, progress, lastBlock, stat, checksum, favoredNodes);
            } else {
                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释：
                 */
                out = new DFSOutputStream(dfsClient, src, flags, progress, lastBlock, stat, checksum, favoredNodes);
            }
            out.start();
            return out;
        }
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：
     *  1、psize = packetsize = 64 * 1024 = 65536
     *  2、csize = bytesPerChecksum = 512b
     *  正常计算是 128 倍，但是 一个 packet 包含了 packetheader，每个 chunksize 是 516
     *  所以 (65536 - 33) / chunksize = 126
     */
    protected void computePacketChunkSize(int psize, int csize) {

        // TODO_MA 马中华 注释： 64kb - (MAX_PROTO_SIZE + 6) = 64kb - 33 = 65536 - 33 = 65503
        // TODO_MA 马中华 注释： PKT_MAX_HEADER_LEN = MAX_PROTO_SIZE + 6 = 33
        final int bodySize = psize - PacketHeader.PKT_MAX_HEADER_LEN;

        // TODO_MA 马中华 注释： chunkSize = 512 + 4
        // TODO_MA 马中华 注释： 每个 chunk 的真实数据大小是 512b， 每个 chunk 有一个校验信息，长度为 4
        // TODO_MA 马中华 注释： buffer + chksumbuffer
        final int chunkSize = csize + getChecksumSize();

        // TODO_MA 马中华 注释： 计算得到每个 packet 的总 chunks 个数
        // TODO_MA 马中华 注释： chunksPerPacket = 65503 / 516 的整数 = 126
        chunksPerPacket = Math.max(bodySize / chunkSize, 1);

        // TODO_MA 马中华 注释： 计算得到 packet 的总大小
        // TODO_MA 马中华 注释： packetSize 的实际大小：516 * 126 = 65016
        packetSize = chunkSize * chunksPerPacket;

        // TODO_MA 马中华 注释： 所以一个完整的 packet 最终还会包含一个 packetheader = 33b
        // TODO_MA 马中华 注释： 一个 packet 的总大小：  65016 + 33 = 65049
        // TODO_MA 马中华 注释： 三部分组成：
        // TODO_MA 马中华 注释： packetheader= 33b
        // TODO_MA 马中华 注释： crc32校验信息： 126 * 4 = 504
        // TODO_MA 马中华 注释： 真实数据 = 126 * 512
        // TODO_MA 马中华 注释： 每次写过去的数据还是 64kb，但是只有 65049 是有效的！
        DFSClient.LOG.debug("computePacketChunkSize: src={}, chunkSize={}, " + "chunksPerPacket={}, packetSize={}", src, chunkSize,
                chunksPerPacket, packetSize
        );
        // TODO_MA 马中华 注释： 一个packet真实数据 = 64512
        // TODO_MA 马中华 注释： 一个数据块 128M = 2081 个 packet
        // TODO_MA 马中华 注释： 当发送 第 2082 个packet 的会发生什么？
        // TODO_MA 马中华 注释： 重新申请 block 然后建立数据管道，再进行packet
    }

    protected TraceScope createWriteTraceScope() {
        return dfsClient.newPathTraceScope("DFSOutputStream#write", src);
    }

    // @see FSOutputSummer#writeChunk()
    // TODO_MA 马中华 注释： 构建一个 chunk
    @Override
    protected synchronized void writeChunk(byte[] b, int offset, int len, byte[] checksum, int ckoff,
                                           int cklen) throws IOException {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 如果 currentPacket = null， 则生成一个 Packet 对象
         */
        writeChunkPrepare(len, ckoff, cklen);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 写 checksum 4 个字节
         *  checksum 这个 buffer 中每次写入 4 个字节进来。
         */
        currentPacket.writeChecksum(checksum, ckoff, cklen);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 写数据 512B， b 每次写进来 512字节的数据
         */
        currentPacket.writeData(b, offset, len);

        // TODO_MA 马中华 注释： chunk 计数 +1
        currentPacket.incNumChunks();

        // TODO_MA 马中华 注释： 位移增长
        getStreamer().incBytesCurBlock(len);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 如果写满了 chunk 数量，或者 数据块写完了， 则形成了一个 packet ，加入到 dataQueue 队列
         */
        // If packet is full, enqueue it for transmission
        if (currentPacket.getNumChunks() == currentPacket.getMaxChunks() || getStreamer().getBytesCurBlock() == blockSize) {
            enqueueCurrentPacketFull();
        }
    }

    /* write the data chunk in <code>buffer</code> staring at
     * <code>buffer.position</code> with
     * a length of <code>len > 0</code>, and its checksum
     */
    protected synchronized void writeChunk(ByteBuffer buffer, int len, byte[] checksum, int ckoff, int cklen) throws IOException {
        writeChunkPrepare(len, ckoff, cklen);

        currentPacket.writeChecksum(checksum, ckoff, cklen);
        currentPacket.writeData(buffer, len);
        currentPacket.incNumChunks();
        getStreamer().incBytesCurBlock(len);

        // If packet is full, enqueue it for transmission
        if (currentPacket.getNumChunks() == currentPacket.getMaxChunks() || getStreamer().getBytesCurBlock() == blockSize) {
            enqueueCurrentPacketFull();
        }
    }

    private synchronized void writeChunkPrepare(int buflen, int ckoff, int cklen) throws IOException {
        dfsClient.checkOpen();
        checkClosed();

        if (buflen > bytesPerChecksum) {
            throw new IOException(
                    "writeChunk() buffer size is " + buflen + " is larger than supported  bytesPerChecksum " + bytesPerChecksum);
        }
        if (cklen != 0 && cklen != getChecksumSize()) {
            throw new IOException(
                    "writeChunk() checksum size is supposed to be " + getChecksumSize() + " but found to be " + cklen);
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         *  1、packetSize = 每个 packet 的大小
         *  2、chunksPerPacket = 一个 packet 中的 chunk 的大小
         *  3、bytesCurBlock packet 在当前 block 的偏移量
         *  4、packet 序号
         *  5、是否最后一个 packet
         */
        if (currentPacket == null) {
            currentPacket = createPacket(packetSize, chunksPerPacket, getStreamer().getBytesCurBlock(),
                    getStreamer().getAndIncCurrentSeqno(), false
            );
            DFSClient.LOG.debug(
                    "WriteChunk allocating new packet seqno={}," + " src={}, packetSize={}, chunksPerPacket={}, bytesCurBlock={}",
                    currentPacket.getSeqno(), src, packetSize, chunksPerPacket, getStreamer().getBytesCurBlock() + ", " + this
            );
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         *  if(isFullPacket()){
         *      enqueue(currentPacket) ===> dataQueue.put(currentPacket)
         *      currentPacket = null;
         *  }
         */
    }

    void enqueueCurrentPacket() throws IOException {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 阻塞加入 DataStreamer 中的 dataQueue 队列
         */
        getStreamer().waitAndQueuePacket(currentPacket);

        // TODO_MA 马中华 注释： 当前 packet 已经加入队列
        currentPacket = null;
    }

    synchronized void enqueueCurrentPacketFull() throws IOException {
        LOG.debug("enqueue full {}, src={}, bytesCurBlock={}, blockSize={}," + " appendChunk={}, {}", currentPacket, src,
                getStreamer().getBytesCurBlock(), blockSize, getStreamer().getAppendChunk(), getStreamer()
        );

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 加入 packet 到队列中
         */
        enqueueCurrentPacket();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 调整 chunk 边界
         */
        adjustChunkBoundary();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 结束当前 block 的写， 重置一些变量，为下个 block 的写准备环境
         */
        endBlock();
    }

    /** create an empty packet to mark the end of the block. */
    void setCurrentPacketToEmpty() throws InterruptedIOException {
        currentPacket = createPacket(0, 0, getStreamer().getBytesCurBlock(), getStreamer().getAndIncCurrentSeqno(), true);
        currentPacket.setSyncBlock(shouldSyncBlock);
    }

    /**
     * If the reopened file did not end at chunk boundary and the above
     * write filled up its partial chunk. Tell the summer to generate full
     * crc chunks from now on.
     */
    protected void adjustChunkBoundary() {
        if (getStreamer().getAppendChunk() && getStreamer().getBytesCurBlock() % bytesPerChecksum == 0) {
            getStreamer().setAppendChunk(false);
            resetChecksumBufSize();
        }

        if (!getStreamer().getAppendChunk()) {
            final int psize = (int) Math.min(blockSize - getStreamer().getBytesCurBlock(), writePacketSize);
            computePacketChunkSize(psize, bytesPerChecksum);
        }
    }

    /**
     * Used in test only.
     */
    @VisibleForTesting
    void setAppendChunk(final boolean appendChunk) {
        getStreamer().setAppendChunk(appendChunk);
    }

    /**
     * Used in test only.
     */
    @VisibleForTesting
    void setBytesCurBlock(final long bytesCurBlock) {
        getStreamer().setBytesCurBlock(bytesCurBlock);
    }

    /**
     * if encountering a block boundary, send an empty packet to
     * indicate the end of block and reset bytesCurBlock.
     *
     * @throws IOException
     */
    void endBlock() throws IOException {
        if (getStreamer().getBytesCurBlock() == blockSize) {

            // TODO_MA 马中华 注释： 生成一个 last packet 标记块
            setCurrentPacketToEmpty();

            // TODO_MA 马中华 注释： 加入队列
            enqueueCurrentPacket();

            // TODO_MA 马中华 注释： 数据块便宜计数 重新归零
            getStreamer().setBytesCurBlock(0);

            // TODO_MA 马中华 注释： flush 位置归零
            lastFlushOffset = 0;
        }
    }

    @Override
    public boolean hasCapability(String capability) {
        return StoreImplementationUtils.isProbeForSyncable(capability);
    }

    /**
     * Flushes out to all replicas of the block. The data is in the buffers
     * of the DNs but not necessarily in the DN's OS buffers.
     *
     * It is a synchronous operation. When it returns,
     * it guarantees that flushed data become visible to new readers.
     * It is not guaranteed that data has been flushed to
     * persistent store on the datanode.
     * Block allocations are persisted on namenode.
     */
    @Override
    public void hflush() throws IOException {
        try (TraceScope ignored = dfsClient.newPathTraceScope("hflush", src)) {
            flushOrSync(false, EnumSet.noneOf(SyncFlag.class));
        }
    }

    @Override
    public void hsync() throws IOException {
        try (TraceScope ignored = dfsClient.newPathTraceScope("hsync", src)) {
            flushOrSync(true, EnumSet.noneOf(SyncFlag.class));
        }
    }

    /**
     * The expected semantics is all data have flushed out to all replicas
     * and all replicas have done posix fsync equivalent - ie the OS has
     * flushed it to the disk device (but the disk may have it in its cache).
     *
     * Note that only the current block is flushed to the disk device.
     * To guarantee durable sync across block boundaries the stream should
     * be created with {@link CreateFlag#SYNC_BLOCK}.
     *
     * @param syncFlags
     *          Indicate the semantic of the sync. Currently used to specify
     *          whether or not to update the block length in NameNode.
     */
    public void hsync(EnumSet<SyncFlag> syncFlags) throws IOException {
        try (TraceScope ignored = dfsClient.newPathTraceScope("hsync", src)) {
            flushOrSync(true, syncFlags);
        }
    }

    /**
     * Flush/Sync buffered data to DataNodes.
     *
     * @param isSync
     *          Whether or not to require all replicas to flush data to the disk
     *          device
     * @param syncFlags
     *          Indicate extra detailed semantic of the flush/sync. Currently
     *          mainly used to specify whether or not to update the file length in
     *          the NameNode
     * @throws IOException
     */
    private void flushOrSync(boolean isSync, EnumSet<SyncFlag> syncFlags) throws IOException {
        dfsClient.checkOpen();
        checkClosed();
        try {
            long toWaitFor;
            long lastBlockLength = -1L;
            boolean updateLength = syncFlags.contains(SyncFlag.UPDATE_LENGTH);
            boolean endBlock = syncFlags.contains(SyncFlag.END_BLOCK);
            synchronized (this) {
                // flush checksum buffer, but keep checksum buffer intact if we do not
                // need to end the current block
                int numKept = flushBuffer(!endBlock, true);
                // bytesCurBlock potentially incremented if there was buffered data

                DFSClient.LOG.debug("DFSClient flush():  bytesCurBlock={}, " + "lastFlushOffset={}, createNewBlock={}",
                        getStreamer().getBytesCurBlock(), lastFlushOffset, endBlock
                );
                // Flush only if we haven't already flushed till this offset.
                if (lastFlushOffset != getStreamer().getBytesCurBlock()) {
                    assert getStreamer().getBytesCurBlock() > lastFlushOffset;
                    // record the valid offset of this flush
                    lastFlushOffset = getStreamer().getBytesCurBlock();
                    if (isSync && currentPacket == null && !endBlock) {
                        // Nothing to send right now,
                        // but sync was requested.
                        // Send an empty packet if we do not end the block right now
                        currentPacket = createPacket(packetSize, chunksPerPacket, getStreamer().getBytesCurBlock(),
                                getStreamer().getAndIncCurrentSeqno(), false
                        );
                    }
                } else {
                    if (isSync && getStreamer().getBytesCurBlock() > 0 && !endBlock) {
                        // Nothing to send right now,
                        // and the block was partially written,
                        // and sync was requested.
                        // So send an empty sync packet if we do not end the block right now
                        currentPacket = createPacket(packetSize, chunksPerPacket, getStreamer().getBytesCurBlock(),
                                getStreamer().getAndIncCurrentSeqno(), false
                        );
                    } else if (currentPacket != null) {
                        // just discard the current packet since it is already been sent.
                        currentPacket.releaseBuffer(byteArrayManager);
                        currentPacket = null;
                    }
                }
                if (currentPacket != null) {
                    currentPacket.setSyncBlock(isSync);
                    enqueueCurrentPacket();
                }
                if (endBlock && getStreamer().getBytesCurBlock() > 0) {
                    // Need to end the current block, thus send an empty packet to
                    // indicate this is the end of the block and reset bytesCurBlock
                    currentPacket = createPacket(0, 0, getStreamer().getBytesCurBlock(), getStreamer().getAndIncCurrentSeqno(),
                            true
                    );
                    currentPacket.setSyncBlock(shouldSyncBlock || isSync);
                    enqueueCurrentPacket();
                    getStreamer().setBytesCurBlock(0);
                    lastFlushOffset = 0;
                } else {
                    // Restore state of stream. Record the last flush offset
                    // of the last full chunk that was flushed.
                    getStreamer().setBytesCurBlock(getStreamer().getBytesCurBlock() - numKept);
                }

                toWaitFor = getStreamer().getLastQueuedSeqno();
            } // end synchronized

            getStreamer().waitForAckedSeqno(toWaitFor);

            // update the block length first time irrespective of flag
            if (updateLength || getStreamer().getPersistBlocks().get()) {
                synchronized (this) {
                    if (!getStreamer().streamerClosed() && getStreamer().getBlock() != null) {
                        lastBlockLength = getStreamer().getBlock().getNumBytes();
                    }
                }
            }
            // If 1) any new blocks were allocated since the last flush, or 2) to
            // update length in NN is required, then persist block locations on
            // namenode.
            if (getStreamer().getPersistBlocks().getAndSet(false) || updateLength) {
                try {
                    dfsClient.namenode.fsync(src, fileId, dfsClient.clientName, lastBlockLength);
                } catch (IOException ioe) {
                    DFSClient.LOG.warn("Unable to persist blocks in hflush for " + src, ioe);
                    // If we got an error here, it might be because some other thread
                    // called close before our hflush completed. In that case, we should
                    // throw an exception that the stream is closed.
                    checkClosed();
                    // If we aren't closed but failed to sync, we should expose that to
                    // the caller.
                    throw ioe;
                }
            }

            synchronized (this) {
                if (!getStreamer().streamerClosed()) {
                    getStreamer().setHflush();
                }
            }
        } catch (InterruptedIOException interrupt) {
            // This kind of error doesn't mean that the stream itself is broken - just
            // the flushing thread got interrupted. So, we shouldn't close down the
            // writer, but instead just propagate the error
            throw interrupt;
        } catch (IOException e) {
            DFSClient.LOG.warn("Error while syncing", e);
            synchronized (this) {
                if (!isClosed()) {
                    getStreamer().getLastException().set(e);
                    closeThreads(true);
                }
            }
            throw e;
        }
    }

    /**
     * @deprecated use {@link HdfsDataOutputStream#getCurrentBlockReplication()}.
     */
    @Deprecated
    public synchronized int getNumCurrentReplicas() throws IOException {
        return getCurrentBlockReplication();
    }

    /**
     * Note that this is not a public API;
     * use {@link HdfsDataOutputStream#getCurrentBlockReplication()} instead.
     *
     * @return the number of valid replicas of the current block
     */
    public synchronized int getCurrentBlockReplication() throws IOException {
        dfsClient.checkOpen();
        checkClosed();
        if (getStreamer().streamerClosed()) {
            return blockReplication; // no pipeline, return repl factor of file
        }
        DatanodeInfo[] currentNodes = getStreamer().getNodes();
        if (currentNodes == null) {
            return blockReplication; // no pipeline, return repl factor of file
        }
        return currentNodes.length;
    }

    /**
     * Waits till all existing data is flushed and confirmations
     * received from datanodes.
     */
    protected void flushInternal() throws IOException {
        long toWaitFor = flushInternalWithoutWaitingAck();
        getStreamer().waitForAckedSeqno(toWaitFor);
    }

    protected synchronized void start() {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        getStreamer().start();
    }

    /**
     * Aborts this output stream and releases any system
     * resources associated with this stream.
     */
    void abort() throws IOException {
        final MultipleIOException.Builder b = new MultipleIOException.Builder();
        synchronized (this) {
            if (isClosed()) {
                return;
            }
            getStreamer().getLastException().set(new IOException(
                    "Lease timeout of " + (dfsClient.getConf().getHdfsTimeout() / 1000) + " seconds expired."));

            try {
                closeThreads(true);
            } catch (IOException e) {
                b.add(e);
            }
        }
        final IOException ioe = b.build();
        if (ioe != null) {
            throw ioe;
        }
    }

    boolean isClosed() {
        return closed || getStreamer().streamerClosed();
    }

    void setClosed() {
        closed = true;
        dfsClient.endFileLease(fileId);
        getStreamer().release();
    }

    // shutdown datastreamer and responseprocessor threads.
    // interrupt datastreamer if force is true
    protected void closeThreads(boolean force) throws IOException {
        try {
            getStreamer().close(force);
            getStreamer().join();
            getStreamer().closeSocket();
        } catch (InterruptedException e) {
            throw new IOException("Failed to shutdown streamer");
        } finally {
            getStreamer().setSocketToNull();
            setClosed();
        }
    }

    /**
     * Closes this output stream and releases any system
     * resources associated with this stream.
     */
    @Override
    public void close() throws IOException {
        final MultipleIOException.Builder b = new MultipleIOException.Builder();
        synchronized (this) {
            try (TraceScope ignored = dfsClient.newPathTraceScope("DFSOutputStream#close", src)) {
                closeImpl();
            } catch (IOException e) {
                b.add(e);
            }
        }
        final IOException ioe = b.build();
        if (ioe != null) {
            throw ioe;
        }
    }

    protected synchronized void closeImpl() throws IOException {
        if (isClosed()) {
            LOG.debug("Closing an already closed stream. [Stream:{}, streamer:{}]", closed, getStreamer().streamerClosed());
            try {
                getStreamer().getLastException().check(true);
            } catch (IOException ioe) {
                cleanupAndRethrowIOException(ioe);
            } finally {
                if (!closed) {
                    // If stream is not closed but streamer closed, clean up the stream.
                    // Most importantly, end the file lease.
                    closeThreads(true);
                }
            }
            return;
        }

        try {
            flushBuffer();       // flush from all upper layers

            if (currentPacket != null) {
                enqueueCurrentPacket();
            }

            if (getStreamer().getBytesCurBlock() != 0) {
                setCurrentPacketToEmpty();
            }

            try {
                flushInternal();             // flush all data to Datanodes
            } catch (IOException ioe) {
                cleanupAndRethrowIOException(ioe);
            }
            completeFile();
        } catch (ClosedChannelException ignored) {
        } finally {
            // Failures may happen when flushing data.
            // Streamers may keep waiting for the new block information.
            // Thus need to force closing these threads.
            // Don't need to call setClosed() because closeThreads(true)
            // calls setClosed() in the finally block.
            closeThreads(true);
        }
    }

    private void completeFile() throws IOException {
        // get last block before destroying the streamer
        ExtendedBlock lastBlock = getStreamer().getBlock();
        try (TraceScope ignored = dfsClient.getTracer().newScope("completeFile")) {
            completeFile(lastBlock);
        }
    }

    /**
     * Determines whether an IOException thrown needs extra cleanup on the stream.
     * Space quota exceptions will be thrown when getting new blocks, so the
     * open HDFS file need to be closed.
     *
     * @param ioe the IOException
     * @return whether the stream needs cleanup for the given IOException
     */
    private boolean exceptionNeedsCleanup(IOException ioe) {
        return ioe instanceof DSQuotaExceededException || ioe instanceof QuotaByStorageTypeExceededException;
    }

    private void cleanupAndRethrowIOException(IOException ioe) throws IOException {
        if (exceptionNeedsCleanup(ioe)) {
            final MultipleIOException.Builder b = new MultipleIOException.Builder();
            b.add(ioe);
            try {
                completeFile();
            } catch (IOException e) {
                b.add(e);
                throw b.build();
            }
        }
        throw ioe;
    }

    // should be called holding (this) lock since setTestFilename() may
    // be called during unit tests
    protected void completeFile(ExtendedBlock last) throws IOException {
        long localstart = Time.monotonicNow();
        final DfsClientConf conf = dfsClient.getConf();
        long sleeptime = conf.getBlockWriteLocateFollowingInitialDelayMs();
        long maxSleepTime = conf.getBlockWriteLocateFollowingMaxDelayMs();
        boolean fileComplete = false;
        int retries = conf.getNumBlockWriteLocateFollowingRetry();
        while (!fileComplete) {
            fileComplete = dfsClient.namenode.complete(src, dfsClient.clientName, last, fileId);
            if (!fileComplete) {
                final int hdfsTimeout = conf.getHdfsTimeout();
                if (!dfsClient.clientRunning || (hdfsTimeout > 0 && localstart + hdfsTimeout < Time.monotonicNow())) {
                    String msg = "Unable to close file because dfsclient " + " was unable to contact the HDFS servers. clientRunning " + dfsClient.clientRunning + " hdfsTimeout " + hdfsTimeout;
                    DFSClient.LOG.info(msg);
                    throw new IOException(msg);
                }
                try {
                    if (retries == 0) {
                        throw new IOException(
                                "Unable to close file because the last block " + last + " does not have enough number of replicas.");
                    }
                    retries--;
                    Thread.sleep(sleeptime);
                    sleeptime = calculateDelayForNextRetry(sleeptime, maxSleepTime);
                    if (Time.monotonicNow() - localstart > 5000) {
                        DFSClient.LOG.info("Could not complete " + src + " retrying...");
                    }
                } catch (InterruptedException ie) {
                    DFSClient.LOG.warn("Caught exception ", ie);
                }
            }
        }
    }

    @VisibleForTesting
    public void setArtificialSlowdown(long period) {
        getStreamer().setArtificialSlowdown(period);
    }

    @VisibleForTesting
    public synchronized void setChunksPerPacket(int value) {
        chunksPerPacket = Math.min(chunksPerPacket, value);
        packetSize = (bytesPerChecksum + getChecksumSize()) * chunksPerPacket;
    }

    /**
     * Returns the size of a file as it was when this stream was opened
     */
    public long getInitialLen() {
        return initialFileSize;
    }

    protected EnumSet<AddBlockFlag> getAddBlockFlags() {
        return addBlockFlags;
    }

    /**
     * @return the FileEncryptionInfo for this stream, or null if not encrypted.
     */
    public FileEncryptionInfo getFileEncryptionInfo() {
        return fileEncryptionInfo;
    }

    /**
     * Returns the access token currently used by streamer, for testing only
     */
    synchronized Token<BlockTokenIdentifier> getBlockToken() {
        return getStreamer().getBlockToken();
    }

    protected long flushInternalWithoutWaitingAck() throws IOException {
        long toWaitFor;
        synchronized (this) {
            dfsClient.checkOpen();
            checkClosed();
            //
            // If there is data in the current buffer, send it across
            //
            getStreamer().queuePacket(currentPacket);
            currentPacket = null;
            toWaitFor = getStreamer().getLastQueuedSeqno();
        }
        return toWaitFor;
    }

    @Override
    public void setDropBehind(Boolean dropBehind) throws IOException {
        CachingStrategy prevStrategy, nextStrategy;
        // CachingStrategy is immutable.  So build a new CachingStrategy with the
        // modifications we want, and compare-and-swap it in.
        do {
            prevStrategy = this.cachingStrategy.get();
            nextStrategy = new CachingStrategy.Builder(prevStrategy).setDropBehind(dropBehind).build();
        } while (!this.cachingStrategy.compareAndSet(prevStrategy, nextStrategy));
    }

    @VisibleForTesting
    ExtendedBlock getBlock() {
        return getStreamer().getBlock();
    }

    @VisibleForTesting
    public long getFileId() {
        return fileId;
    }

    /**
     * Return the source of stream.
     */
    String getSrc() {
        return src;
    }

    /**
     * Returns the data streamer object.
     */
    protected DataStreamer getStreamer() {
        return streamer;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + streamer;
    }

    static LocatedBlock addBlock(DatanodeInfo[] excludedNodes, DFSClient dfsClient, String src, ExtendedBlock prevBlock,
                                 long fileId, String[] favoredNodes, EnumSet<AddBlockFlag> allocFlags) throws IOException {
        final DfsClientConf conf = dfsClient.getConf();
        int retries = conf.getNumBlockWriteLocateFollowingRetry();
        long sleeptime = conf.getBlockWriteLocateFollowingInitialDelayMs();
        long maxSleepTime = conf.getBlockWriteLocateFollowingMaxDelayMs();
        long localstart = Time.monotonicNow();
        while (true) {
            try {
                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 发送 RPC 请求给 NameNode 申请 Block
                 */
                return dfsClient.namenode.addBlock(src, dfsClient.clientName, prevBlock, excludedNodes, fileId, favoredNodes,
                        allocFlags
                );

                // TODO_MA 马中华 注释： 如果想让跟 RPC 有关的代码不要报错： 执行三个编译
                // TODO_MA 马中华 注释： yarn
                // TODO_MA 马中华 注释： hdfs
                // TODO_MA 马中华 注释： mapreduce  三个大项目直接编译就行 ，
                // TODO_MA 马中华 注释： common 编译不过去！

            } catch (RemoteException e) {
                IOException ue = e.unwrapRemoteException(FileNotFoundException.class, AccessControlException.class,
                        NSQuotaExceededException.class, DSQuotaExceededException.class, QuotaByStorageTypeExceededException.class,
                        UnresolvedPathException.class
                );
                if (ue != e) {
                    throw ue; // no need to retry these exceptions
                }
                if (NotReplicatedYetException.class.getName().equals(e.getClassName())) {
                    if (retries == 0) {
                        throw e;
                    } else {
                        --retries;
                        LOG.info("Exception while adding a block", e);
                        long elapsed = Time.monotonicNow() - localstart;
                        if (elapsed > 5000) {
                            LOG.info("Waiting for replication for " + (elapsed / 1000) + " seconds");
                        }
                        try {
                            LOG.warn("NotReplicatedYetException sleeping " + src + " retries left " + retries);
                            Thread.sleep(sleeptime);
                            sleeptime = calculateDelayForNextRetry(sleeptime, maxSleepTime);
                        } catch (InterruptedException ie) {
                            LOG.warn("Caught exception", ie);
                        }
                    }
                } else {
                    throw e;
                }
            }
        }
    }

    /**
     * Calculates the delay for the next retry.
     *
     * The delay is increased exponentially until the maximum delay is reached.
     *
     * @param previousDelay delay for the previous retry
     * @param maxDelay maximum delay
     * @return the minimum of the double of the previous delay
     * and the maximum delay
     */
    private static long calculateDelayForNextRetry(long previousDelay, long maxDelay) {
        return Math.min(previousDelay * 2, maxDelay);
    }
}
