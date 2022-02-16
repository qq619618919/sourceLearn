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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.protobuf.ByteString;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.BlockChecksumOptions;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.StripedBlockInfo;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockPinningException;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Op;
import org.apache.hadoop.hdfs.protocol.datatransfer.Receiver;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataEncryptionKeyFactory;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.InvalidMagicNumberException;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ClientReadStatusProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpBlockChecksumResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ReadOpChecksumInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ReleaseShortCircuitAccessResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ShortCircuitShmResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.BlockChecksumHelper.BlockChecksumComputer;
import org.apache.hadoop.hdfs.server.datanode.BlockChecksumHelper.AbstractBlockChecksumComputer;
import org.apache.hadoop.hdfs.server.datanode.BlockChecksumHelper.ReplicatedBlockChecksumComputer;
import org.apache.hadoop.hdfs.server.datanode.BlockChecksumHelper.BlockGroupNonStripedChecksumComputer;
import org.apache.hadoop.hdfs.server.datanode.DataNode.ShortCircuitFdsUnsupportedException;
import org.apache.hadoop.hdfs.server.datanode.DataNode.ShortCircuitFdsVersionException;
import org.apache.hadoop.hdfs.server.datanode.ShortCircuitRegistry.NewShmInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.SlotId;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StopWatch;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ShortCircuitFdResponse.DO_NOT_USE_RECEIPT_VERIFICATION;
import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ShortCircuitFdResponse.USE_RECEIPT_VERIFICATION;
import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.ERROR;
import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.ERROR_ACCESS_TOKEN;
import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.ERROR_INVALID;
import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.ERROR_UNSUPPORTED;
import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.SUCCESS;
import static org.apache.hadoop.hdfs.server.datanode.DataNode.DN_CLIENTTRACE_FORMAT;
import static org.apache.hadoop.util.Time.monotonicNow;

/**
 * Thread for processing incoming/outgoing data stream.
 * // TODO_MA 马中华 注释： DataXceiver 是一个线程，专门负责数据接收
 */
class DataXceiver extends Receiver implements Runnable {

    public static final Logger LOG = DataNode.LOG;
    static final Log ClientTraceLog = DataNode.ClientTraceLog;

    private Peer peer;
    private final String remoteAddress; // address of remote side
    private final String remoteAddressWithoutPort; // only the address, no port
    private final String localAddress;  // local address of this daemon
    private final DataNode datanode;
    private final DNConf dnConf;
    private final DataXceiverServer dataXceiverServer;
    private final boolean connectToDnViaHostname;
    private long opStartTime; //the start time of receiving an Op
    private final InputStream socketIn;
    private OutputStream socketOut;
    private BlockReceiver blockReceiver = null;
    private final int ioFileBufferSize;
    private final int smallBufferSize;
    private Thread xceiver = null;

    /**
     * Client Name used in previous operation. Not available on first request
     * on the socket.
     */
    private String previousOpClientName;

    public static DataXceiver create(Peer peer, DataNode dn, DataXceiverServer dataXceiverServer) throws IOException {
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 构造一个 DataXceiver 为一个客户端提供数据读写服务
         *  1、peer = 代表了某个一个 Socket 客户端
         */
        return new DataXceiver(peer, dn, dataXceiverServer);
    }

    private DataXceiver(Peer peer, DataNode datanode, DataXceiverServer dataXceiverServer) throws IOException {
        super(datanode.getTracer());
        this.peer = peer;
        this.dnConf = datanode.getDnConf();

        // TODO_MA 马中华 注释： 输入输出流
        this.socketIn = peer.getInputStream();
        this.socketOut = peer.getOutputStream();

        this.datanode = datanode;
        this.dataXceiverServer = dataXceiverServer;
        this.connectToDnViaHostname = datanode.getDnConf().connectToDnViaHostname;
        this.ioFileBufferSize = DFSUtilClient.getIoFileBufferSize(datanode.getConf());
        this.smallBufferSize = DFSUtilClient.getSmallBufferSize(datanode.getConf());
        remoteAddress = peer.getRemoteAddressString();
        final int colonIdx = remoteAddress.indexOf(':');
        remoteAddressWithoutPort = (colonIdx < 0) ? remoteAddress : remoteAddress.substring(0, colonIdx);
        localAddress = peer.getLocalAddressString();
        LOG.debug("Number of active connections is: {}", datanode.getXceiverCount());
    }

    /**
     * Update the current thread's name to contain the current status.
     * Use this only after this receiver has started on its thread, i.e.,
     * outside the constructor.
     */
    private void updateCurrentThreadName(String status) {
        StringBuilder sb = new StringBuilder();
        sb.append("DataXceiver for client ");
        if (previousOpClientName != null) {
            sb.append(previousOpClientName).append(" at ");
        }
        sb.append(remoteAddress);
        if (status != null) {
            sb.append(" [").append(status).append("]");
        }
        Thread.currentThread().setName(sb.toString());
    }

    /** Return the datanode object. */
    DataNode getDataNode() {
        return datanode;
    }

    private OutputStream getOutputStream() {
        return socketOut;
    }

    public void sendOOB() throws IOException, InterruptedException {
        BlockReceiver br = getCurrentBlockReceiver();
        if (br == null) {
            return;
        }
        // This doesn't need to be in a critical section. Althogh the client
        // can resue the connection to issue a different request, trying sending
        // an OOB through the recently closed block receiver is harmless.
        LOG.info("Sending OOB to peer: {}", peer);
        br.sendOOB();
    }

    public void stopWriter() {
        // We want to interrupt the xceiver only when it is serving writes.
        synchronized (this) {
            if (getCurrentBlockReceiver() == null) {
                return;
            }
            xceiver.interrupt();
        }
        LOG.info("Stopped the writer: {}", peer);
    }

    /**
     * blockReceiver is updated at multiple places. Use the synchronized setter
     * and getter.
     */
    private synchronized void setCurrentBlockReceiver(BlockReceiver br) {
        blockReceiver = br;
    }

    private synchronized BlockReceiver getCurrentBlockReceiver() {
        return blockReceiver;
    }

    /**
     * Read/write data from/to the DataXceiverServer.
     */
    @Override
    public void run() {
        int opsProcessed = 0;
        Op op = null;

        try {
            synchronized (this) {
                xceiver = Thread.currentThread();
            }
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 注册
             */
            dataXceiverServer.addPeer(peer, Thread.currentThread(), this);
            peer.setWriteTimeout(datanode.getDnConf().socketWriteTimeout);
            InputStream input = socketIn;
            try {
                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 对输入输出流进行包装
                 */
                IOStreamPair saslStreams = datanode.saslServer.receive(peer, socketOut, socketIn,
                        datanode.getXferAddress().getPort(), datanode.getDatanodeId()
                );

                // TODO_MA 马中华 注释： 获取输入输出流
                input = new BufferedInputStream(saslStreams.in, smallBufferSize);
                socketOut = saslStreams.out;

            } catch (InvalidMagicNumberException imne) {
                if (imne.isHandshake4Encryption()) {
                    LOG.info(
                            "Failed to read expected encryption handshake from client " + "at {}. Perhaps the client " + "is running an older version of Hadoop which does not support " + "encryption",
                            peer.getRemoteAddressString(), imne
                    );
                } else {
                    LOG.info(
                            "Failed to read expected SASL data transfer protection " + "handshake from client at {}" + ". Perhaps the client is running an older version of Hadoop " + "which does not support SASL data transfer protection",
                            peer.getRemoteAddressString(), imne
                    );
                }
                return;
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 将得到的 输入流 设置到 Receiver 中的 in 成员变量
             */
            super.initialize(new DataInputStream(input));


            // TODO_MA 马中华 注释： 我们在一个循环中处理请求，并停留一小段时间。
            // TODO_MA 马中华 注释： 这种乐观的行为允许另一端重用连接。将 keepalive timeout 设置为 0 会禁用此行为。
            // We process requests in a loop, and stay around for a short timeout.
            // This optimistic behaviour allows the other end to reuse connections.
            // Setting keepalive timeout to 0 disable this behavior.

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 进入一个处理循环，处理相关请求
             */
            do {
                updateCurrentThreadName("Waiting for operation #" + (opsProcessed + 1));

                try {
                    if (opsProcessed != 0) {
                        assert dnConf.socketKeepaliveTimeout > 0;
                        peer.setReadTimeout(dnConf.socketKeepaliveTimeout);
                    } else {
                        peer.setReadTimeout(dnConf.socketTimeout);
                    }
                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 读取操作符,  OP 就代表了  客户端/上游 datanode 想要干嘛
                     */
                    op = readOp();
                } catch (InterruptedIOException ignored) {
                    // Time out while we wait for client rpc
                    break;
                } catch (EOFException | ClosedChannelException e) {
                    // Since we optimistically expect the next op, it's quite normal to
                    // get EOF here.
                    LOG.debug("Cached {} closing after {} ops.  " + "This message is usually benign.", peer, opsProcessed);
                    break;
                } catch (IOException err) {
                    incrDatanodeNetworkErrors();
                    throw err;
                }

                // restore normal timeout
                if (opsProcessed != 0) {
                    peer.setReadTimeout(dnConf.socketTimeout);
                }

                opStartTime = monotonicNow();

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 处理具体的操作
                 */
                processOp(op);

                ++opsProcessed;
            } while ((peer != null) && (!peer.isClosed() && dnConf.socketKeepaliveTimeout > 0));
            // TODO_MA 马中华 注释： while 条件

        } catch (Throwable t) {
            String s = datanode.getDisplayName() + ":DataXceiver error processing " + ((op == null) ? "unknown" : op.name()) + " operation " + " src: " + remoteAddress + " dst: " + localAddress;
            if (op == Op.WRITE_BLOCK && t instanceof ReplicaAlreadyExistsException) {
                // For WRITE_BLOCK, it is okay if the replica already exists since
                // client and replication may write the same block to the same datanode
                // at the same time.
                if (LOG.isTraceEnabled()) {
                    LOG.trace(s, t);
                } else {
                    LOG.info("{}; {}", s, t.toString());
                }
            } else if (op == Op.READ_BLOCK && t instanceof SocketTimeoutException) {
                String s1 = "Likely the client has stopped reading, disconnecting it";
                s1 += " (" + s + ")";
                if (LOG.isTraceEnabled()) {
                    LOG.trace(s1, t);
                } else {
                    LOG.info("{}; {}", s1, t.toString());
                }
            } else if (t instanceof InvalidToken || t.getCause() instanceof InvalidToken) {
                // The InvalidToken exception has already been logged in
                // checkAccess() method and this is not a server error.
                LOG.trace(s, t);
            } else {
                LOG.error(s, t);
            }
        } finally {
            collectThreadLocalStates();
            LOG.debug("{}:Number of active connections is: {}", datanode.getDisplayName(), datanode.getXceiverCount());
            updateCurrentThreadName("Cleaning up");
            if (peer != null) {
                dataXceiverServer.closePeer(peer);
                IOUtils.closeStream(in);
            }
        }
    }

    /**
     * In this short living thread, any local states should be collected before
     * the thread dies away.
     */
    private void collectThreadLocalStates() {
        if (datanode.getPeerMetrics() != null) {
            datanode.getPeerMetrics().collectThreadLocalStates();
        }
    }

    @Override
    public void requestShortCircuitFds(final ExtendedBlock blk, final Token<BlockTokenIdentifier> token, SlotId slotId,
                                       int maxVersion, boolean supportsReceiptVerification) throws IOException {
        updateCurrentThreadName("Passing file descriptors for block " + blk);
        DataOutputStream out = getBufferedOutputStream();
        checkAccess(out, true, blk, token, Op.REQUEST_SHORT_CIRCUIT_FDS, BlockTokenIdentifier.AccessMode.READ, null, null);
        BlockOpResponseProto.Builder bld = BlockOpResponseProto.newBuilder();
        FileInputStream fis[] = null;
        SlotId registeredSlotId = null;
        boolean success = false;
        try {
            try {
                if (peer.getDomainSocket() == null) {
                    throw new IOException("You cannot pass file descriptors over " + "anything but a UNIX domain socket.");
                }
                if (slotId != null) {
                    boolean isCached = datanode.data.isCached(blk.getBlockPoolId(), blk.getBlockId());
                    datanode.shortCircuitRegistry.registerSlot(ExtendedBlockId.fromExtendedBlock(blk), slotId, isCached);
                    registeredSlotId = slotId;
                }
                fis = datanode.requestShortCircuitFdsForRead(blk, token, maxVersion);
                Preconditions.checkState(fis != null);
                bld.setStatus(SUCCESS);
                bld.setShortCircuitAccessVersion(DataNode.CURRENT_BLOCK_FORMAT_VERSION);
            } catch (ShortCircuitFdsVersionException e) {
                bld.setStatus(ERROR_UNSUPPORTED);
                bld.setShortCircuitAccessVersion(DataNode.CURRENT_BLOCK_FORMAT_VERSION);
                bld.setMessage(e.getMessage());
            } catch (ShortCircuitFdsUnsupportedException e) {
                bld.setStatus(ERROR_UNSUPPORTED);
                bld.setMessage(e.getMessage());
            } catch (IOException e) {
                bld.setStatus(ERROR);
                bld.setMessage(e.getMessage());
                LOG.error("Request short-circuit read file descriptor" + " failed with unknown error.", e);
            }
            bld.build().writeDelimitedTo(socketOut);
            if (fis != null) {
                FileDescriptor fds[] = new FileDescriptor[fis.length];
                for (int i = 0; i < fds.length; i++) {
                    fds[i] = fis[i].getFD();
                }
                byte buf[] = new byte[1];
                if (supportsReceiptVerification) {
                    buf[0] = (byte) USE_RECEIPT_VERIFICATION.getNumber();
                } else {
                    buf[0] = (byte) DO_NOT_USE_RECEIPT_VERIFICATION.getNumber();
                }
                DomainSocket sock = peer.getDomainSocket();
                sock.sendFileDescriptors(fds, buf, 0, buf.length);
                if (supportsReceiptVerification) {
                    LOG.trace("Reading receipt verification byte for {}", slotId);
                    int val = sock.getInputStream().read();
                    if (val < 0) {
                        throw new EOFException();
                    }
                } else {
                    LOG.trace("Receipt verification is not enabled on the DataNode. " + "Not verifying {}", slotId);
                }
                success = true;
            }
        } finally {
            if ((!success) && (registeredSlotId != null)) {
                LOG.info("Unregistering {} because the " + "requestShortCircuitFdsForRead operation failed.",
                        registeredSlotId
                );
                datanode.shortCircuitRegistry.unregisterSlot(registeredSlotId);
            }
            if (ClientTraceLog.isInfoEnabled()) {
                DatanodeRegistration dnR = datanode.getDNRegistrationForBP(blk.getBlockPoolId());
                BlockSender.ClientTraceLog.info(String.format(
                        "src: 127.0.0.1, dest: 127.0.0.1, op: REQUEST_SHORT_CIRCUIT_FDS," + " blockid: %s, srvID: %s, success: %b",
                        blk.getBlockId(), dnR.getDatanodeUuid(), success
                ));
            }
            if (fis != null) {
                IOUtils.cleanupWithLogger(null, fis);
            }
        }
    }

    @Override
    public void releaseShortCircuitFds(SlotId slotId) throws IOException {
        boolean success = false;
        try {
            String error;
            Status status;
            try {
                datanode.shortCircuitRegistry.unregisterSlot(slotId);
                error = null;
                status = Status.SUCCESS;
            } catch (UnsupportedOperationException e) {
                error = "unsupported operation";
                status = Status.ERROR_UNSUPPORTED;
            } catch (Throwable e) {
                error = e.getMessage();
                status = Status.ERROR_INVALID;
            }
            ReleaseShortCircuitAccessResponseProto.Builder bld = ReleaseShortCircuitAccessResponseProto.newBuilder();
            bld.setStatus(status);
            if (error != null) {
                bld.setError(error);
            }
            bld.build().writeDelimitedTo(socketOut);
            success = true;
        } finally {
            if (ClientTraceLog.isInfoEnabled()) {
                BlockSender.ClientTraceLog.info(String.format(
                        "src: 127.0.0.1, dest: 127.0.0.1, op: RELEASE_SHORT_CIRCUIT_FDS," + " shmId: %016x%016x, slotIdx: %d, srvID: %s, success: %b",
                        slotId.getShmId().getHi(), slotId.getShmId().getLo(), slotId.getSlotIdx(),
                        datanode.getDatanodeUuid(), success
                ));
            }
        }
    }

    private void sendShmErrorResponse(Status status, String error) throws IOException {
        ShortCircuitShmResponseProto.newBuilder().setStatus(status).setError(error).build().writeDelimitedTo(socketOut);
    }

    private void sendShmSuccessResponse(DomainSocket sock, NewShmInfo shmInfo) throws IOException {
        DataNodeFaultInjector.get().sendShortCircuitShmResponse();
        ShortCircuitShmResponseProto.newBuilder().setStatus(SUCCESS).setId(PBHelperClient.convert(shmInfo.getShmId()))
                .build().writeDelimitedTo(socketOut);
        // Send the file descriptor for the shared memory segment.
        byte buf[] = new byte[]{(byte) 0};
        FileDescriptor shmFdArray[] = new FileDescriptor[]{shmInfo.getFileStream().getFD()};
        sock.sendFileDescriptors(shmFdArray, buf, 0, buf.length);
    }

    @Override
    public void requestShortCircuitShm(String clientName) throws IOException {
        NewShmInfo shmInfo = null;
        boolean success = false;
        DomainSocket sock = peer.getDomainSocket();
        try {
            if (sock == null) {
                sendShmErrorResponse(ERROR_INVALID,
                        "Bad request from " + peer + ": must request a shared " + "memory segment over a UNIX domain socket."
                );
                return;
            }
            try {
                shmInfo = datanode.shortCircuitRegistry.createNewMemorySegment(clientName, sock);
                // After calling #{ShortCircuitRegistry#createNewMemorySegment}, the
                // socket is managed by the DomainSocketWatcher, not the DataXceiver.
                releaseSocket();
            } catch (UnsupportedOperationException e) {
                sendShmErrorResponse(ERROR_UNSUPPORTED,
                        "This datanode has not been configured to support " + "short-circuit shared memory segments."
                );
                return;
            } catch (IOException e) {
                sendShmErrorResponse(ERROR, "Failed to create shared file descriptor: " + e.getMessage());
                return;
            }
            sendShmSuccessResponse(sock, shmInfo);
            success = true;
        } finally {
            if (ClientTraceLog.isInfoEnabled()) {
                if (success) {
                    BlockSender.ClientTraceLog.info(String.format(
                            "cliID: %s, src: 127.0.0.1, dest: 127.0.0.1, " + "op: REQUEST_SHORT_CIRCUIT_SHM," + " shmId: %016x%016x, srvID: %s, success: true",
                            clientName, shmInfo.getShmId().getHi(), shmInfo.getShmId().getLo(), datanode.getDatanodeUuid()
                    ));
                } else {
                    BlockSender.ClientTraceLog.info(String.format(
                            "cliID: %s, src: 127.0.0.1, dest: 127.0.0.1, " + "op: REQUEST_SHORT_CIRCUIT_SHM, " + "shmId: n/a, srvID: %s, success: false",
                            clientName, datanode.getDatanodeUuid()
                    ));
                }
            }
            if ((!success) && (peer == null)) {
                // The socket is now managed by the DomainSocketWatcher.  However,
                // we failed to pass it to the client.  We call shutdown() on the
                // UNIX domain socket now.  This will trigger the DomainSocketWatcher
                // callback.  The callback will close the domain socket.
                // We don't want to close the socket here, since that might lead to
                // bad behavior inside the poll() call.  See HADOOP-11802 for details.
                try {
                    LOG.warn("Failed to send success response back to the client. " + "Shutting down socket for {}",
                            shmInfo.getShmId()
                    );
                    sock.shutdown();
                } catch (IOException e) {
                    LOG.warn("Failed to shut down socket in error handler", e);
                }
            }
            IOUtils.cleanupWithLogger(null, shmInfo);
        }
    }

    void releaseSocket() {
        dataXceiverServer.releasePeer(peer);
        peer = null;
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：
     *  1、block = 要读取的数据块
     *  2、blockToken = 数据块的访问令牌
     *  3、clientName = 客户端的名称
     *  4、blockOffset =  要读取数据在数据块中的位置
     *  5、length = 读取数据的长度
     *  6、Datanode = 是否发送校验数据
     *  7、cachingStrategy = 缓存策略
     */
    @Override
    public void readBlock(final ExtendedBlock block, final Token<BlockTokenIdentifier> blockToken, final String clientName,
                          final long blockOffset, final long length, final boolean sendChecksum,
                          final CachingStrategy cachingStrategy) throws IOException {
        previousOpClientName = clientName;
        long read = 0;
        updateCurrentThreadName("Sending block " + block);
        OutputStream baseStream = getOutputStream();
        DataOutputStream out = getBufferedOutputStream();
        checkAccess(out, true, block, blockToken, Op.READ_BLOCK, BlockTokenIdentifier.AccessMode.READ);

        // send the block
        BlockSender blockSender = null;
        DatanodeRegistration dnR = datanode.getDNRegistrationForBP(block.getBlockPoolId());
        final String clientTraceFmt = clientName.length() > 0 && ClientTraceLog.isInfoEnabled() ? String.format(
                DN_CLIENTTRACE_FORMAT, localAddress, remoteAddress, "%d", "HDFS_READ", clientName, "%d",
                dnR.getDatanodeUuid(), block, "%d"
        ) : dnR + " Served block " + block + " to " + remoteAddress;

        try {
            try {
                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 创建BlockSender对象
                 */
                blockSender = new BlockSender(block, blockOffset, length, true, false, sendChecksum, datanode,
                        clientTraceFmt, cachingStrategy
                );
            } catch (IOException e) {
                String msg = "opReadBlock " + block + " received exception " + e;
                LOG.info(msg);
                sendResponse(ERROR, msg);
                throw e;
            }

            // send op status
            // TODO_MA 马中华 注释： 返回 ReadOpChecksumInfoProto 响应给客户端
            writeSuccessWithChecksumInfo(blockSender, new DataOutputStream(getOutputStream()));

            long beginRead = Time.monotonicNow();

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 发送数据块
             */
            read = blockSender.sendBlock(out, baseStream, null); // send data

            long duration = Time.monotonicNow() - beginRead;

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 等待客户端返回一个状态码，看是否是成功
             */
            if (blockSender.didSendEntireByteRange()) {
                // If we sent the entire range, then we should expect the client
                // to respond with a Status enum.
                try {
                    ClientReadStatusProto stat = ClientReadStatusProto.parseFrom(PBHelperClient.vintPrefixed(in));
                    if (!stat.hasStatus()) {
                        LOG.warn("Client {} did not send a valid status code " + "after reading. Will close connection.",
                                peer.getRemoteAddressString()
                        );
                        IOUtils.closeStream(out);
                    }
                } catch (IOException ioe) {
                    LOG.debug("Error reading client status response. Will close connection.", ioe);
                    IOUtils.closeStream(out);
                    incrDatanodeNetworkErrors();
                }
            } else {
                IOUtils.closeStream(out);
            }
            datanode.metrics.incrBytesRead((int) read);
            datanode.metrics.incrBlocksRead();
            datanode.metrics.incrTotalReadTime(duration);
        } catch (SocketException ignored) {
            LOG.trace("{}:Ignoring exception while serving {} to {}", dnR, block, remoteAddress, ignored);
            // Its ok for remote side to close the connection anytime.
            datanode.metrics.incrBlocksRead();
            IOUtils.closeStream(out);
        } catch (IOException ioe) {
            /* What exactly should we do here?
             * Earlier version shutdown() datanode if there is disk error.
             */
            if (!(ioe instanceof SocketTimeoutException)) {
                LOG.warn("{}:Got exception while serving {} to {}", dnR, block, remoteAddress, ioe);
                incrDatanodeNetworkErrors();
            }
            // Normally the client reports a bad block to the NN. However if the
            // meta file is corrupt or an disk error occurs (EIO), then the client
            // never gets a chance to do validation, and hence will never report
            // the block as bad. For some classes of IO exception, the DN should
            // report the block as bad, via the handleBadBlock() method
            datanode.handleBadBlock(block, ioe, false);
            throw ioe;
        } finally {
            IOUtils.closeStream(blockSender);
        }

        //update metrics
        datanode.metrics.addReadBlockOp(elapsed());
        datanode.metrics.incrReadsFromClient(peer.isLocal(), read);
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 注意参数
     *  1、DatanodeInfo[] targets    目标 datanode 列表
     *  2、DatanodeInfo srcDataNode  发送 writeBlock 请求的 datanode
     */
    @Override
    public void writeBlock(final ExtendedBlock block, final StorageType storageType,
                           final Token<BlockTokenIdentifier> blockToken, final String clientname,
                           final DatanodeInfo[] targets, final StorageType[] targetStorageTypes,
                           final DatanodeInfo srcDataNode, final BlockConstructionStage stage, final int pipelineSize,
                           final long minBytesRcvd, final long maxBytesRcvd, final long latestGenerationStamp,
                           DataChecksum requestedChecksum, CachingStrategy cachingStrategy, boolean allowLazyPersist,
                           final boolean pinning, final boolean[] targetPinnings, final String storageId,
                           final String[] targetStorageIds) throws IOException {
        previousOpClientName = clientname;
        updateCurrentThreadName("Receiving block " + block);
        final boolean isDatanode = clientname.length() == 0;
        final boolean isClient = !isDatanode;
        final boolean isTransfer = stage == BlockConstructionStage.TRANSFER_RBW || stage == BlockConstructionStage.TRANSFER_FINALIZED;
        allowLazyPersist = allowLazyPersist && (dnConf.getAllowNonLocalLazyPersist() || peer.isLocal());
        long size = 0;
        // reply to upstream datanode or client
        final DataOutputStream replyOut = getBufferedOutputStream();

        int nst = targetStorageTypes.length;
        StorageType[] storageTypes = new StorageType[nst + 1];
        storageTypes[0] = storageType;
        if (targetStorageTypes.length > 0) {
            System.arraycopy(targetStorageTypes, 0, storageTypes, 1, nst);
        }

        // To support older clients, we don't pass in empty storageIds
        final int nsi = targetStorageIds.length;
        final String[] storageIds;
        if (nsi > 0) {
            storageIds = new String[nsi + 1];
            storageIds[0] = storageId;
            if (targetStorageTypes.length > 0) {
                System.arraycopy(targetStorageIds, 0, storageIds, 1, nsi);
            }
        } else {
            storageIds = new String[0];
        }
        checkAccess(replyOut, isClient, block, blockToken, Op.WRITE_BLOCK, BlockTokenIdentifier.AccessMode.WRITE,
                storageTypes, storageIds
        );

        // check single target for transfer-RBW/Finalized
        if (isTransfer && targets.length > 0) {
            throw new IOException(stage + " does not support multiple targets " + Arrays.asList(targets));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "opWriteBlock: stage={}, clientname={}\n  " + "block  ={}, newGs={}, bytesRcvd=[{}, {}]\n  " + "targets={}; pipelineSize={}, srcDataNode={}, pinning={}",
                    stage, clientname, block, latestGenerationStamp, minBytesRcvd, maxBytesRcvd, Arrays.asList(targets),
                    pipelineSize, srcDataNode, pinning
            );
            LOG.debug("isDatanode={}, isClient={}, isTransfer={}", isDatanode, isClient, isTransfer);
            LOG.debug("writeBlock receive buf size {} tcp no delay {}", peer.getReceiveBufferSize(), peer.getTcpNoDelay());
        }

        // We later mutate block's generation stamp and length, but we need to
        // forward the original version of the block to downstream mirrors, so
        // make a copy here.
        final ExtendedBlock originalBlock = new ExtendedBlock(block);
        if (block.getNumBytes() == 0) {
            block.setNumBytes(dataXceiverServer.estimateBlockSize);
        }
        LOG.info("Receiving {} src: {} dest: {}", block, remoteAddress, localAddress);

        DataOutputStream mirrorOut = null;  // stream to next target
        DataInputStream mirrorIn = null;    // reply from next target

        Socket mirrorSock = null;           // socket to next target
        String mirrorNode = null;           // the name:port of next target
        String firstBadLink = "";           // first datanode that failed in connection setup
        Status mirrorInStatus = SUCCESS;
        final String storageUuid;
        final boolean isOnTransientStorage;
        try {
            final Replica replica;
            if (isDatanode || stage != BlockConstructionStage.PIPELINE_CLOSE_RECOVERY) {
                // open a block receiver
                setCurrentBlockReceiver(
                        /*************************************************
                         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                         *  注释： 构建 BlickReceiver 用于接收 Block 写到本地磁盘
                         *  1、创建一个本地 block 文件
                         *  2、构建一个 block 文件输出流
                         */
                        getBlockReceiver(block, storageType, in, peer.getRemoteAddressString(), peer.getLocalAddressString(),
                                stage, latestGenerationStamp, minBytesRcvd, maxBytesRcvd, clientname, srcDataNode, datanode,
                                requestedChecksum, cachingStrategy, allowLazyPersist, pinning, storageId
                        ));
                replica = blockReceiver.getReplica();
            } else {
                replica = datanode.data.recoverClose(block, latestGenerationStamp, minBytesRcvd);
            }
            storageUuid = replica.getStorageUuid();
            isOnTransientStorage = replica.isOnTransientStorage();

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 当前 datanode 和下游 datanode 之间建立数据管道
             *  如果这个代码，此时时候在 第三个 datanode 里面执行，则不用执行 if 里面的代码了。
             *  如果执行 if 中的代码，就是当前 datanode 和 下一个 datanode 建立连接
             *  如果不执行 if 中的代码 ，表示当前 datanode 就是数据管道上的最后一个 dataonde
             */
            // Connect to downstream machine, if appropriate
            if (targets.length > 0) {
                InetSocketAddress mirrorTarget = null;
                // Connect to backup machine
                mirrorNode = targets[0].getXferAddr(connectToDnViaHostname);
                LOG.debug("Connecting to datanode {}", mirrorNode);

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 创建 Socket 客户端
                 */
                mirrorTarget = NetUtils.createSocketAddr(mirrorNode);
                mirrorSock = datanode.newSocket();
                try {

                    DataNodeFaultInjector.get().failMirrorConnection();

                    int timeoutValue = dnConf.socketTimeout + (HdfsConstants.READ_TIMEOUT_EXTENSION * targets.length);
                    int writeTimeout = dnConf.socketWriteTimeout + (HdfsConstants.WRITE_TIMEOUT_EXTENSION * targets.length);

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 链接 DataNode 数据传输服务端
                     */
                    NetUtils.connect(mirrorSock, mirrorTarget, timeoutValue);

                    mirrorSock.setTcpNoDelay(dnConf.getDataTransferServerTcpNoDelay());
                    mirrorSock.setSoTimeout(timeoutValue);
                    mirrorSock.setKeepAlive(true);
                    if (dnConf.getTransferSocketSendBufferSize() > 0) {
                        mirrorSock.setSendBufferSize(dnConf.getTransferSocketSendBufferSize());
                    }

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 获取输入输出流
                     */
                    OutputStream unbufMirrorOut = NetUtils.getOutputStream(mirrorSock, writeTimeout);
                    InputStream unbufMirrorIn = NetUtils.getInputStream(mirrorSock);

                    DataEncryptionKeyFactory keyFactory = datanode.getDataEncryptionKeyFactoryForBlock(block);
                    SecretKey secretKey = null;
                    if (dnConf.overwriteDownstreamDerivedQOP) {
                        String bpid = block.getBlockPoolId();
                        BlockKey blockKey = datanode.blockPoolTokenSecretManager.get(bpid).getCurrentKey();
                        secretKey = blockKey.getKey();
                    }

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释：
                     */
                    IOStreamPair saslStreams = datanode.saslClient.socketSend(mirrorSock, unbufMirrorOut, unbufMirrorIn,
                            keyFactory, blockToken, targets[0], secretKey
                    );
                    unbufMirrorOut = saslStreams.out;
                    unbufMirrorIn = saslStreams.in;
                    mirrorOut = new DataOutputStream(new BufferedOutputStream(unbufMirrorOut, smallBufferSize));
                    mirrorIn = new DataInputStream(unbufMirrorIn);

                    String targetStorageId = null;
                    if (targetStorageIds.length > 0) {
                        // Older clients may not have provided any targetStorageIds
                        targetStorageId = targetStorageIds[0];
                    }

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 写 Block
                     *  在这个上面 已经完成 当前 datanode 和 下一个 datanode 的链接建立
                     *  当前 datnaode 把意图 发给 下一个 datanode
                     */
                    if (targetPinnings != null && targetPinnings.length > 0) {
                        new Sender(mirrorOut).writeBlock(originalBlock, targetStorageTypes[0], blockToken, clientname,
                                targets, targetStorageTypes, srcDataNode, stage, pipelineSize, minBytesRcvd, maxBytesRcvd,
                                latestGenerationStamp, requestedChecksum, cachingStrategy, allowLazyPersist,
                                targetPinnings[0], targetPinnings, targetStorageId, targetStorageIds
                        );
                    } else {
                        new Sender(mirrorOut).writeBlock(originalBlock, targetStorageTypes[0], blockToken, clientname,
                                targets, targetStorageTypes, srcDataNode, stage, pipelineSize, minBytesRcvd, maxBytesRcvd,
                                latestGenerationStamp, requestedChecksum, cachingStrategy, allowLazyPersist, false,
                                targetPinnings, targetStorageId, targetStorageIds
                        );
                    }

                    mirrorOut.flush();

                    DataNodeFaultInjector.get().writeBlockAfterFlush();

                    // read connect ack (only for clients, not for replication req)
                    if (isClient) {
                        BlockOpResponseProto connectAck = BlockOpResponseProto.parseFrom(
                                PBHelperClient.vintPrefixed(mirrorIn));
                        mirrorInStatus = connectAck.getStatus();
                        firstBadLink = connectAck.getFirstBadLink();
                        if (mirrorInStatus != SUCCESS) {
                            LOG.debug(
                                    "Datanode {} got response for connect" + "ack  from downstream datanode with firstbadlink as {}",
                                    targets.length, firstBadLink
                            );
                        }
                    }

                } catch (IOException e) {
                    if (isClient) {
                        BlockOpResponseProto.newBuilder().setStatus(ERROR)
                                // NB: Unconditionally using the xfer addr w/o hostname
                                .setFirstBadLink(targets[0].getXferAddr()).build().writeDelimitedTo(replyOut);
                        replyOut.flush();
                    }
                    IOUtils.closeStream(mirrorOut);
                    mirrorOut = null;
                    IOUtils.closeStream(mirrorIn);
                    mirrorIn = null;
                    IOUtils.closeSocket(mirrorSock);
                    mirrorSock = null;
                    if (isClient) {
                        LOG.error("{}:Exception transfering block {} to mirror {}", datanode, block, mirrorNode, e);
                        throw e;
                    } else {
                        LOG.info("{}:Exception transfering {} to mirror {}- continuing " + "without the mirror", datanode,
                                block, mirrorNode, e
                        );
                        incrDatanodeNetworkErrors();
                    }
                }
            }

            // send connect-ack to source for clients and not transfer-RBW/Finalized
            if (isClient && !isTransfer) {
                if (mirrorInStatus != SUCCESS) {
                    LOG.debug("Datanode {} forwarding connect ack to upstream " + "firstbadlink is {}", targets.length,
                            firstBadLink
                    );
                }
                BlockOpResponseProto.newBuilder().setStatus(mirrorInStatus).setFirstBadLink(firstBadLink).build()
                        .writeDelimitedTo(replyOut);
                replyOut.flush();
            }

            // receive the block and mirror to the next target
            if (blockReceiver != null) {
                String mirrorAddr = (mirrorSock == null) ? null : mirrorNode;

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 接收上游 datanode 发送过来的 packet 数据
                 */
                blockReceiver.receiveBlock(mirrorOut, mirrorIn, replyOut, mirrorAddr, dataXceiverServer.getWriteThrottler(),
                        targets, false
                );

                // send close-ack for transfer-RBW/Finalized
                // TODO_MA 马中华 注释： 返回 SUCCESS
                if (isTransfer) {
                    LOG.trace("TRANSFER: send close-ack");
                    writeResponse(SUCCESS, null, replyOut);
                }
            }

            // update its generation stamp
            if (isClient && stage == BlockConstructionStage.PIPELINE_CLOSE_RECOVERY) {
                block.setGenerationStamp(latestGenerationStamp);
                block.setNumBytes(minBytesRcvd);
            }

            // if this write is for a replication request or recovering a failed close for client, then confirm block.
            // For other client-writes, the block is finalized in the PacketResponder.
            if (isDatanode || stage == BlockConstructionStage.PIPELINE_CLOSE_RECOVERY) {
                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 关闭 Block
                 */
                datanode.closeBlock(block, null, storageUuid, isOnTransientStorage);
                LOG.info("Received {} src: {} dest: {} of size {}", block, remoteAddress, localAddress, block.getNumBytes());
            }

            if (isClient) {
                size = block.getNumBytes();
            }
        } catch (IOException ioe) {
            LOG.info("opWriteBlock {} received exception {}", block, ioe.toString());
            incrDatanodeNetworkErrors();
            throw ioe;
        } finally {
            // close all opened streams
            IOUtils.closeStream(mirrorOut);
            IOUtils.closeStream(mirrorIn);
            IOUtils.closeStream(replyOut);
            IOUtils.closeSocket(mirrorSock);
            IOUtils.closeStream(blockReceiver);
            setCurrentBlockReceiver(null);
        }

        //update metrics
        datanode.getMetrics().addWriteBlockOp(elapsed());
        datanode.getMetrics().incrWritesFromClient(peer.isLocal(), size);
    }

    @Override
    public void transferBlock(final ExtendedBlock blk, final Token<BlockTokenIdentifier> blockToken, final String clientName,
                              final DatanodeInfo[] targets, final StorageType[] targetStorageTypes,
                              final String[] targetStorageIds) throws IOException {
        previousOpClientName = clientName;
        updateCurrentThreadName(Op.TRANSFER_BLOCK + " " + blk);

        final DataOutputStream out = new DataOutputStream(getOutputStream());
        checkAccess(out, true, blk, blockToken, Op.TRANSFER_BLOCK, BlockTokenIdentifier.AccessMode.COPY, targetStorageTypes,
                targetStorageIds
        );
        try {
            datanode.transferReplicaForPipelineRecovery(blk, targets, targetStorageTypes, targetStorageIds, clientName);
            writeResponse(Status.SUCCESS, null, out);
        } catch (IOException ioe) {
            LOG.info("transferBlock {} received exception {}", blk, ioe.toString());
            incrDatanodeNetworkErrors();
            throw ioe;
        } finally {
            IOUtils.closeStream(out);
        }
    }

    @Override
    public void blockChecksum(ExtendedBlock block, Token<BlockTokenIdentifier> blockToken,
                              BlockChecksumOptions blockChecksumOptions) throws IOException {
        updateCurrentThreadName("Getting checksum for block " + block);
        final DataOutputStream out = new DataOutputStream(getOutputStream());
        checkAccess(out, true, block, blockToken, Op.BLOCK_CHECKSUM, BlockTokenIdentifier.AccessMode.READ);
        BlockChecksumComputer maker = new ReplicatedBlockChecksumComputer(datanode, block, blockChecksumOptions);

        try {
            maker.compute();

            //write reply
            BlockOpResponseProto.newBuilder().setStatus(SUCCESS).setChecksumResponse(
                            OpBlockChecksumResponseProto.newBuilder().setBytesPerCrc(maker.getBytesPerCRC())
                                    .setCrcPerBlock(maker.getCrcPerBlock())
                                    .setBlockChecksum(ByteString.copyFrom(maker.getOutBytes()))
                                    .setCrcType(PBHelperClient.convert(maker.getCrcType()))
                                    .setBlockChecksumOptions(PBHelperClient.convert(blockChecksumOptions))).build()
                    .writeDelimitedTo(out);
            out.flush();
        } catch (IOException ioe) {
            LOG.info("blockChecksum {} received exception {}", block, ioe.toString());
            incrDatanodeNetworkErrors();
            throw ioe;
        } finally {
            IOUtils.closeStream(out);
        }

        //update metrics
        datanode.metrics.addBlockChecksumOp(elapsed());
    }

    @Override
    public void blockGroupChecksum(final StripedBlockInfo stripedBlockInfo, final Token<BlockTokenIdentifier> blockToken,
                                   long requestedNumBytes, BlockChecksumOptions blockChecksumOptions) throws IOException {
        final ExtendedBlock block = stripedBlockInfo.getBlock();
        updateCurrentThreadName("Getting checksum for block group" + block);
        final DataOutputStream out = new DataOutputStream(getOutputStream());
        checkAccess(out, true, block, blockToken, Op.BLOCK_GROUP_CHECKSUM, BlockTokenIdentifier.AccessMode.READ);

        AbstractBlockChecksumComputer maker = new BlockGroupNonStripedChecksumComputer(datanode, stripedBlockInfo,
                requestedNumBytes, blockChecksumOptions
        );

        try {
            maker.compute();

            //write reply
            BlockOpResponseProto.newBuilder().setStatus(SUCCESS).setChecksumResponse(
                            OpBlockChecksumResponseProto.newBuilder().setBytesPerCrc(maker.getBytesPerCRC())
                                    .setCrcPerBlock(maker.getCrcPerBlock())
                                    .setBlockChecksum(ByteString.copyFrom(maker.getOutBytes()))
                                    .setCrcType(PBHelperClient.convert(maker.getCrcType()))
                                    .setBlockChecksumOptions(PBHelperClient.convert(blockChecksumOptions))).build()
                    .writeDelimitedTo(out);
            out.flush();
        } catch (IOException ioe) {
            LOG.info("blockChecksum {} received exception {}", stripedBlockInfo.getBlock(), ioe.toString());
            incrDatanodeNetworkErrors();
            throw ioe;
        } finally {
            IOUtils.closeStream(out);
        }

        //update metrics
        datanode.metrics.addBlockChecksumOp(elapsed());
    }

    @Override
    public void copyBlock(final ExtendedBlock block, final Token<BlockTokenIdentifier> blockToken) throws IOException {
        updateCurrentThreadName("Copying block " + block);
        DataOutputStream reply = getBufferedOutputStream();
        checkAccess(reply, true, block, blockToken, Op.COPY_BLOCK, BlockTokenIdentifier.AccessMode.COPY);

        if (datanode.data.getPinning(block)) {
            String msg = "Not able to copy block " + block.getBlockId() + " " + "to " + peer.getRemoteAddressString() + " because it's pinned ";
            LOG.info(msg);
            sendResponse(Status.ERROR_BLOCK_PINNED, msg);
            return;
        }

        if (!dataXceiverServer.balanceThrottler.acquire()) { // not able to start
            String msg = "Not able to copy block " + block.getBlockId() + " " + "to " + peer.getRemoteAddressString() + " because threads " + "quota is exceeded.";
            LOG.info(msg);
            sendResponse(ERROR, msg);
            return;
        }

        BlockSender blockSender = null;
        boolean isOpSuccess = true;

        try {
            // check if the block exists or not
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            blockSender = new BlockSender(block, 0, -1, false, false, true, datanode, null, CachingStrategy.newDropBehind());

            OutputStream baseStream = getOutputStream();

            // send status first
            writeSuccessWithChecksumInfo(blockSender, reply);

            long beginRead = Time.monotonicNow();
            // send block content to the target
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            long read = blockSender.sendBlock(reply, baseStream, dataXceiverServer.balanceThrottler);
            long duration = Time.monotonicNow() - beginRead;
            datanode.metrics.incrBytesRead((int) read);
            datanode.metrics.incrBlocksRead();
            datanode.metrics.incrTotalReadTime(duration);

            LOG.info("Copied {} to {}", block, peer.getRemoteAddressString());
        } catch (IOException ioe) {
            isOpSuccess = false;
            LOG.info("opCopyBlock {} received exception {}", block, ioe.toString());
            incrDatanodeNetworkErrors();
            // Normally the client reports a bad block to the NN. However if the
            // meta file is corrupt or an disk error occurs (EIO), then the client
            // never gets a chance to do validation, and hence will never report
            // the block as bad. For some classes of IO exception, the DN should
            // report the block as bad, via the handleBadBlock() method
            datanode.handleBadBlock(block, ioe, false);
            throw ioe;
        } finally {
            dataXceiverServer.balanceThrottler.release();
            if (isOpSuccess) {
                try {
                    // send one last byte to indicate that the resource is cleaned.
                    reply.writeChar('d');
                } catch (IOException ignored) {
                }
            }
            IOUtils.closeStream(reply);
            IOUtils.closeStream(blockSender);
        }

        //update metrics
        datanode.metrics.addCopyBlockOp(elapsed());
    }

    @Override
    public void replaceBlock(final ExtendedBlock block, final StorageType storageType,
                             final Token<BlockTokenIdentifier> blockToken, final String delHint,
                             final DatanodeInfo proxySource, final String storageId) throws IOException {
        updateCurrentThreadName("Replacing block " + block + " from " + delHint);
        DataOutputStream replyOut = new DataOutputStream(getOutputStream());
        checkAccess(replyOut, true, block, blockToken, Op.REPLACE_BLOCK, BlockTokenIdentifier.AccessMode.REPLACE,
                new StorageType[]{storageType}, new String[]{storageId}
        );

        if (!dataXceiverServer.balanceThrottler.acquire()) { // not able to start
            String msg = "Not able to receive block " + block.getBlockId() + " from " + peer.getRemoteAddressString() + " because threads " + "quota is exceeded.";
            LOG.warn(msg);
            sendResponse(ERROR, msg);
            return;
        }

        Socket proxySock = null;
        DataOutputStream proxyOut = null;
        Status opStatus = SUCCESS;
        String errMsg = null;
        DataInputStream proxyReply = null;
        boolean IoeDuringCopyBlockOperation = false;
        try {
            // Move the block to different storage in the same datanode
            if (proxySource.equals(datanode.getDatanodeId())) {
                ReplicaInfo oldReplica = datanode.data.moveBlockAcrossStorage(block, storageType, storageId);
                if (oldReplica != null) {
                    LOG.info("Moved {} from StorageType {} to {}", block, oldReplica.getVolume().getStorageType(),
                            storageType
                    );
                }
            } else {
                block.setNumBytes(dataXceiverServer.estimateBlockSize);
                // get the output stream to the proxy
                final String dnAddr = proxySource.getXferAddr(connectToDnViaHostname);
                LOG.debug("Connecting to datanode {}", dnAddr);
                InetSocketAddress proxyAddr = NetUtils.createSocketAddr(dnAddr);

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释：
                 */
                proxySock = datanode.newSocket();
                NetUtils.connect(proxySock, proxyAddr, dnConf.socketTimeout);

                proxySock.setTcpNoDelay(dnConf.getDataTransferServerTcpNoDelay());
                proxySock.setSoTimeout(dnConf.socketTimeout);
                proxySock.setKeepAlive(true);

                OutputStream unbufProxyOut = NetUtils.getOutputStream(proxySock, dnConf.socketWriteTimeout);
                InputStream unbufProxyIn = NetUtils.getInputStream(proxySock);
                DataEncryptionKeyFactory keyFactory = datanode.getDataEncryptionKeyFactoryForBlock(block);
                IOStreamPair saslStreams = datanode.saslClient.socketSend(proxySock, unbufProxyOut, unbufProxyIn, keyFactory,
                        blockToken, proxySource
                );
                unbufProxyOut = saslStreams.out;
                unbufProxyIn = saslStreams.in;

                proxyOut = new DataOutputStream(new BufferedOutputStream(unbufProxyOut, smallBufferSize));
                proxyReply = new DataInputStream(new BufferedInputStream(unbufProxyIn, ioFileBufferSize));

                /* send request to the proxy */
                IoeDuringCopyBlockOperation = true;
                new Sender(proxyOut).copyBlock(block, blockToken);
                IoeDuringCopyBlockOperation = false;

                // receive the response from the proxy

                BlockOpResponseProto copyResponse = BlockOpResponseProto.parseFrom(PBHelperClient.vintPrefixed(proxyReply));

                String logInfo = "copy block " + block + " from " + proxySock.getRemoteSocketAddress();
                DataTransferProtoUtil.checkBlockOpStatus(copyResponse, logInfo, true);

                // get checksum info about the block we're copying
                ReadOpChecksumInfoProto checksumInfo = copyResponse.getReadOpChecksumInfo();
                DataChecksum remoteChecksum = DataTransferProtoUtil.fromProto(checksumInfo.getChecksum());
                // open a block receiver and check if the block does not exist
                setCurrentBlockReceiver(
                        getBlockReceiver(block, storageType, proxyReply, proxySock.getRemoteSocketAddress().toString(),
                                proxySock.getLocalSocketAddress().toString(), null, 0, 0, 0, "", null, datanode,
                                remoteChecksum, CachingStrategy.newDropBehind(), false, false, storageId
                        ));

                // receive a block
                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释：
                 */
                blockReceiver.receiveBlock(null, null, replyOut, null, dataXceiverServer.balanceThrottler, null, true);

                // notify name node
                final Replica r = blockReceiver.getReplica();

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释：
                 */
                datanode.notifyNamenodeReceivedBlock(block, delHint, r.getStorageUuid(), r.isOnTransientStorage());

                LOG.info("Moved {} from {}, delHint={}", block, peer.getRemoteAddressString(), delHint);
            }
        } catch (IOException ioe) {
            opStatus = ERROR;
            if (ioe instanceof BlockPinningException) {
                opStatus = Status.ERROR_BLOCK_PINNED;
            }
            errMsg = "opReplaceBlock " + block + " received exception " + ioe;
            LOG.info(errMsg);
            if (!IoeDuringCopyBlockOperation) {
                // Don't double count IO errors
                incrDatanodeNetworkErrors();
            }
            throw ioe;
        } finally {
            // receive the last byte that indicates the proxy released its thread resource
            if (opStatus == SUCCESS && proxyReply != null) {
                try {
                    proxyReply.readChar();
                } catch (IOException ignored) {
                }
            }

            // now release the thread resource
            dataXceiverServer.balanceThrottler.release();

            // send response back
            try {
                sendResponse(opStatus, errMsg);
            } catch (IOException ioe) {
                LOG.warn("Error writing reply back to {}", peer.getRemoteAddressString());
                incrDatanodeNetworkErrors();
            }
            IOUtils.closeStream(proxyOut);
            IOUtils.closeStream(blockReceiver);
            IOUtils.closeStream(proxyReply);
            IOUtils.closeStream(replyOut);
        }

        //update metrics
        datanode.metrics.addReplaceBlockOp(elapsed());
    }


    /**
     * Separated for testing.
     */
    @VisibleForTesting
    BlockReceiver getBlockReceiver(final ExtendedBlock block, final StorageType storageType, final DataInputStream in,
                                   final String inAddr, final String myAddr, final BlockConstructionStage stage,
                                   final long newGs, final long minBytesRcvd, final long maxBytesRcvd,
                                   final String clientname, final DatanodeInfo srcDataNode, final DataNode dn,
                                   DataChecksum requestedChecksum, CachingStrategy cachingStrategy,
                                   final boolean allowLazyPersist, final boolean pinning,
                                   final String storageId) throws IOException {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return new BlockReceiver(block, storageType, in, inAddr, myAddr, stage, newGs, minBytesRcvd, maxBytesRcvd,
                clientname, srcDataNode, dn, requestedChecksum, cachingStrategy, allowLazyPersist, pinning, storageId
        );
    }

    /**
     * Separated for testing.
     * @return
     */
    @VisibleForTesting
    DataOutputStream getBufferedOutputStream() {
        return new DataOutputStream(new BufferedOutputStream(getOutputStream(), smallBufferSize));
    }


    private long elapsed() {
        return monotonicNow() - opStartTime;
    }

    /**
     * Utility function for sending a response.
     *
     * @param status status message to write
     * @param message message to send to the client or other DN
     */
    private void sendResponse(Status status, String message) throws IOException {
        writeResponse(status, message, getOutputStream());
    }

    private static void writeResponse(Status status, String message, OutputStream out) throws IOException {
        BlockOpResponseProto.Builder response = BlockOpResponseProto.newBuilder().setStatus(status);
        if (message != null) {
            response.setMessage(message);
        }
        response.build().writeDelimitedTo(out);
        out.flush();
    }

    private void writeSuccessWithChecksumInfo(BlockSender blockSender, DataOutputStream out) throws IOException {

        ReadOpChecksumInfoProto ckInfo = ReadOpChecksumInfoProto.newBuilder()
                .setChecksum(DataTransferProtoUtil.toProto(blockSender.getChecksum()))
                .setChunkOffset(blockSender.getOffset()).build();

        BlockOpResponseProto response = BlockOpResponseProto.newBuilder().setStatus(SUCCESS).setReadOpChecksumInfo(ckInfo)
                .build();
        response.writeDelimitedTo(out);
        out.flush();
    }

    private void incrDatanodeNetworkErrors() {
        datanode.incrDatanodeNetworkErrors(remoteAddressWithoutPort);
    }

    /**
     * Wait until the BP is registered, upto the configured amount of time.
     * Throws an exception if times out, which should fail the client request.
     * @param block requested block
     */
    void checkAndWaitForBP(final ExtendedBlock block) throws IOException {
        String bpId = block.getBlockPoolId();

        // The registration is only missing in relatively short time window.
        // Optimistically perform this first.
        try {
            datanode.getDNRegistrationForBP(bpId);
            return;
        } catch (IOException ioe) {
            // not registered
        }

        // retry
        long bpReadyTimeout = dnConf.getBpReadyTimeout();
        StopWatch sw = new StopWatch();
        sw.start();
        while (sw.now(TimeUnit.SECONDS) <= bpReadyTimeout) {
            try {
                datanode.getDNRegistrationForBP(bpId);
                return;
            } catch (IOException ioe) {
                // not registered
            }
            // sleep before trying again
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                throw new IOException("Interrupted while serving request. Aborting.");
            }
        }
        // failed to obtain registration.
        throw new IOException("Not ready to serve the block pool, " + bpId + ".");
    }

    private void checkAccess(OutputStream out, final boolean reply, ExtendedBlock blk, Token<BlockTokenIdentifier> t, Op op,
                             BlockTokenIdentifier.AccessMode mode) throws IOException {
        checkAccess(out, reply, blk, t, op, mode, null, null);
    }

    private void checkAccess(OutputStream out, final boolean reply, final ExtendedBlock blk,
                             final Token<BlockTokenIdentifier> t, final Op op, final BlockTokenIdentifier.AccessMode mode,
                             final StorageType[] storageTypes, final String[] storageIds) throws IOException {
        checkAndWaitForBP(blk);
        if (datanode.isBlockTokenEnabled) {
            LOG.debug("Checking block access token for block '{}' with mode '{}'", blk.getBlockId(), mode);
            try {
                datanode.blockPoolTokenSecretManager.checkAccess(t, null, blk, mode, storageTypes, storageIds);
            } catch (InvalidToken e) {
                try {
                    if (reply) {
                        BlockOpResponseProto.Builder resp = BlockOpResponseProto.newBuilder().setStatus(ERROR_ACCESS_TOKEN);
                        if (mode == BlockTokenIdentifier.AccessMode.WRITE) {
                            DatanodeRegistration dnR = datanode.getDNRegistrationForBP(blk.getBlockPoolId());
                            // NB: Unconditionally using the xfer addr w/o hostname
                            resp.setFirstBadLink(dnR.getXferAddr());
                        }
                        resp.build().writeDelimitedTo(out);
                        out.flush();
                    }
                    LOG.warn("Block token verification failed: op={}, " + "remoteAddress={}, message={}", op, remoteAddress,
                            e.getLocalizedMessage()
                    );
                    throw e;
                } finally {
                    IOUtils.closeStream(out);
                }
            }
        }
    }
}
