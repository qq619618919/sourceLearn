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

package org.apache.zookeeper.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NIOServerCnxnFactory implements a multi-threaded ServerCnxnFactory using
 * NIO non-blocking socket calls. Communication between threads is handled via
 * queues.
 *
 * - 1   accept thread, which accepts new connections and assigns to a
 * selector thread
 * - 1-N selector threads, each of which selects on 1/N of the connections.
 * The reason the factory supports more than one selector thread is that
 * with large numbers of connections, select() itself can become a
 * performance bottleneck.
 * - 0-M socket I/O worker threads, which perform basic socket reads and
 * writes. If configured with 0 worker threads, the selector threads
 * do the socket I/O directly.
 * - 1   connection expiration thread, which closes idle connections; this is
 * necessary to expire connections on which no session is established.
 *
 * Typical (default) thread counts are: on a 32 core machine, 1 accept thread,
 * 1 connection expiration thread, 4 selector threads, and 64 worker threads.
 */
public class NIOServerCnxnFactory extends ServerCnxnFactory {

    private static final Logger LOG = LoggerFactory.getLogger(NIOServerCnxnFactory.class);

    /**
     * Default sessionless connection timeout in ms: 10000 (10s)
     */
    public static final String ZOOKEEPER_NIO_SESSIONLESS_CNXN_TIMEOUT = "zookeeper.nio.sessionlessCnxnTimeout";

    /**
     * With 500 connections to an observer with watchers firing on each, is
     * unable to exceed 1GigE rates with only 1 selector.
     * Defaults to using 2 selector threads with 8 cores and 4 with 32 cores.
     * Expressed as sqrt(numCores/2). Must have at least 1 selector thread.
     */
    public static final String ZOOKEEPER_NIO_NUM_SELECTOR_THREADS = "zookeeper.nio.numSelectorThreads";
    /**
     * Default: 2 * numCores
     */
    public static final String ZOOKEEPER_NIO_NUM_WORKER_THREADS = "zookeeper.nio.numWorkerThreads";
    /**
     * Default: 64kB
     */
    public static final String ZOOKEEPER_NIO_DIRECT_BUFFER_BYTES = "zookeeper.nio.directBufferBytes";
    /**
     * Default worker pool shutdown timeout in ms: 5000 (5s)
     */
    public static final String ZOOKEEPER_NIO_SHUTDOWN_TIMEOUT = "zookeeper.nio.shutdownTimeout";

    static {
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread t, Throwable e) {
                LOG.error("Thread {} died", t, e);
            }
        });
        /**
         * this is to avoid the jvm bug:
         * NullPointerException in Selector.open()
         * http://bugs.sun.com/view_bug.do?bug_id=6427854
         */
        try {
            Selector.open().close();
        } catch(IOException ie) {
            LOG.error("Selector failed to open", ie);
        }

        /**
         * Value of 0 disables use of direct buffers and instead uses
         * gathered write call.
         *
         * Default to using 64k direct buffers.
         */
        directBufferBytes = Integer.getInteger(ZOOKEEPER_NIO_DIRECT_BUFFER_BYTES, 64 * 1024);
    }

    /**
     * AbstractSelectThread is an abstract base class containing a few bits
     * of code shared by the AcceptThread (which selects on the listen socket)
     * and SelectorThread (which selects on client connections) classes.
     */
    private abstract class AbstractSelectThread extends ZooKeeperThread {

        protected final Selector selector;

        public AbstractSelectThread(String name) throws IOException {
            super(name);
            // Allows the JVM to shutdown even if this thread is still running.
            setDaemon(true);
            this.selector = Selector.open();
        }

        public void wakeupSelector() {
            selector.wakeup();
        }

        /**
         * Close the selector. This should be called when the thread is about to
         * exit and no operation is going to be performed on the Selector or
         * SelectionKey
         */
        protected void closeSelector() {
            try {
                selector.close();
            } catch(IOException e) {
                LOG.warn("ignored exception during selector close.", e);
            }
        }

        protected void cleanupSelectionKey(SelectionKey key) {
            if(key != null) {
                try {
                    key.cancel();
                } catch(Exception ex) {
                    LOG.debug("ignoring exception during selectionkey cancel", ex);
                }
            }
        }

        protected void fastCloseSock(SocketChannel sc) {
            if(sc != null) {
                try {
                    // Hard close immediately, discarding buffers
                    sc.socket().setSoLinger(true, 0);
                } catch(SocketException e) {
                    LOG.warn("Unable to set socket linger to 0, socket close may stall in CLOSE_WAIT", e);
                }
                NIOServerCnxn.closeSock(sc);
            }
        }

    }

    /**
     * There is a single AcceptThread which accepts new connections and assigns
     * them to a SelectorThread using a simple round-robin scheme to spread
     * them across the SelectorThreads. It enforces maximum number of
     * connections per IP and attempts to cope with running out of file
     * descriptors by briefly sleeping before retrying.
     */
    private class AcceptThread extends AbstractSelectThread {

        private final ServerSocketChannel acceptSocket;
        private final SelectionKey acceptKey;
        private final RateLogger acceptErrorLogger = new RateLogger(LOG);
        private final Collection<SelectorThread> selectorThreads;
        private Iterator<SelectorThread> selectorIterator;
        private volatile boolean reconfiguring = false;

        public AcceptThread(ServerSocketChannel ss, InetSocketAddress addr,
                Set<SelectorThread> selectorThreads) throws IOException {
            super("NIOServerCxnFactory.AcceptThread:" + addr);
            this.acceptSocket = ss;

            // TODO_MA 注释： 只负责 SelectionKey.OP_ACCEP 事件
            this.acceptKey = acceptSocket.register(selector, SelectionKey.OP_ACCEPT);

            this.selectorThreads = Collections.unmodifiableList(new ArrayList<SelectorThread>(selectorThreads));
            selectorIterator = this.selectorThreads.iterator();
        }

        public void run() {
            try {

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 执行 select
                 */
                while(!stopped && !acceptSocket.socket().isClosed()) {
                    try {
                        select();
                    } catch(RuntimeException e) {
                        LOG.warn("Ignoring unexpected runtime exception", e);
                    } catch(Exception e) {
                        LOG.warn("Ignoring unexpected exception", e);
                    }
                }
            } finally {
                closeSelector();
                // This will wake up the selector threads, and tell the
                // worker thread pool to begin shutdown.
                if(!reconfiguring) {
                    NIOServerCnxnFactory.this.stop();
                }
                LOG.info("accept thread exitted run method");
            }
        }

        public void setReconfiguring() {
            reconfiguring = true;
        }

        private void select() {
            try {

                // TODO_MA 注释： 进行 select
                selector.select();
                Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();

                while(!stopped && selectedKeys.hasNext()) {
                    SelectionKey key = selectedKeys.next();
                    selectedKeys.remove();

                    if(!key.isValid()) {
                        continue;
                    }

                    // TODO_MA 注释： 完成链接
                    if(key.isAcceptable()) {

                        /*************************************************
                         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                         *  注释： 完成链接
                         */
                        if(!doAccept()) {
                            // If unable to pull a new connection off the accept
                            // queue, pause accepting to give us time to free
                            // up file descriptors and so the accept thread
                            // doesn't spin in a tight loop.
                            pauseAccept(10);
                        }
                    } else {
                        LOG.warn("Unexpected ops in accept select {}", key.readyOps());
                    }
                }
            } catch(IOException e) {
                LOG.warn("Ignoring IOException while selecting", e);
            }
        }

        /**
         * Mask off the listen socket interest ops and use select() to sleep
         * so that other threads can wake us up by calling wakeup() on the
         * selector.
         */
        private void pauseAccept(long millisecs) {
            acceptKey.interestOps(0);
            try {
                selector.select(millisecs);
            } catch(IOException e) {
                // ignore
            } finally {
                acceptKey.interestOps(SelectionKey.OP_ACCEPT);
            }
        }

        /**
         * Accept new socket connections. Enforces maximum number of connections
         * per client IP address. Round-robin assigns to selector thread for
         * handling. Returns whether pulled a connection off the accept queue
         * or not. If encounters an error attempts to fast close the socket.
         *
         * @return whether was able to accept a connection or not
         */
        private boolean doAccept() {
            boolean accepted = false;
            SocketChannel sc = null;
            try {

                // TODO_MA 注释： 完成链接, 客户端发送网络链接请求过来了，则这句代码返回！
                sc = acceptSocket.accept();
                accepted = true;

                if(limitTotalNumberOfCnxns()) {
                    throw new IOException("Too many connections max allowed is " + maxCnxns);
                }
                InetAddress ia = sc.socket().getInetAddress();
                int cnxncount = getClientCnxnCount(ia);

                if(maxClientCnxns > 0 && cnxncount >= maxClientCnxns) {
                    throw new IOException("Too many connections from " + ia + " - max is " + maxClientCnxns);
                }

                LOG.debug("Accepted socket connection from {}", sc.socket().getRemoteSocketAddress());

                sc.configureBlocking(false);

                // TODO_MA 注释：  Round-robin 轮询模式使用 SelectorThread
                // Round-robin assign this connection to a selector thread
                if(!selectorIterator.hasNext()) {
                    selectorIterator = selectorThreads.iterator();
                }

                // TODO_MA 注释： 遍历得到一个 SelectorThread
                SelectorThread selectorThread = selectorIterator.next();

                // TODO_MA 注释： 丢入 acceptedQueue 队列，让 selector 来接待
                // TODO_MA 注释： 完成了 accept 链接的 SocketChannel 在 selectorThread 上注册 OP_READ 和 OP_WRITE
                if(!selectorThread.addAcceptedConnection(sc)) {
                    throw new IOException(
                            "Unable to add connection to selector queue" + (stopped ? " (shutdown in progress)" : ""));
                }
                acceptErrorLogger.flush();
            } catch(IOException e) {
                // accept, maxClientCnxns, configureBlocking
                ServerMetrics.getMetrics().CONNECTION_REJECTED.add(1);
                acceptErrorLogger.rateLimitLog("Error accepting new connection: " + e.getMessage());
                fastCloseSock(sc);
            }
            return accepted;
        }

    }

    /**
     * The SelectorThread receives newly accepted connections from the
     * AcceptThread and is responsible for selecting for I/O readiness
     * across the connections. This thread is the only thread that performs
     * any non-threadsafe or potentially blocking calls on the selector
     * (registering new connections and reading/writing interest ops).
     *
     * Assignment of a connection to a SelectorThread is permanent and only
     * one SelectorThread will ever interact with the connection. There are
     * 1-N SelectorThreads, with connections evenly apportioned between the
     * SelectorThreads.
     *
     * If there is a worker thread pool, when a connection has I/O to perform
     * the SelectorThread removes it from selection by clearing its interest
     * ops and schedules the I/O for processing by a worker thread. When the
     * work is complete, the connection is placed on the ready queue to have
     * its interest ops restored and resume selection.
     *
     * If there is no worker thread pool, the SelectorThread performs the I/O
     * directly.
     */
    class SelectorThread extends AbstractSelectThread {

        private final int id;
        private final Queue<SocketChannel> acceptedQueue;
        private final Queue<SelectionKey> updateQueue;

        public SelectorThread(int id) throws IOException {
            super("NIOServerCxnFactory.SelectorThread-" + id);
            this.id = id;
            acceptedQueue = new LinkedBlockingQueue<SocketChannel>();
            updateQueue = new LinkedBlockingQueue<SelectionKey>();
        }

        /**
         * Place new accepted connection onto a queue for adding. Do this
         * so only the selector thread modifies what keys are registered
         * with the selector.
         */
        public boolean addAcceptedConnection(SocketChannel accepted) {

            // TODO_MA 注释： 完成链接的 客户端加入 acceptedQueue
            if(stopped || !acceptedQueue.offer(accepted)) {
                return false;
            }

            // TODO_MA 注释： 唤醒 selectorThread
            wakeupSelector();
            return true;
        }

        /**
         * Place interest op update requests onto a queue so that only the
         * selector thread modifies interest ops, because interest ops
         * reads/sets are potentially blocking operations if other select
         * operations are happening.
         */
        public boolean addInterestOpsUpdateRequest(SelectionKey sk) {
            if(stopped || !updateQueue.offer(sk)) {
                return false;
            }

            // TODO_MA 注释： 唤醒 Selector
            wakeupSelector();
            return true;
        }

        /**
         * The main loop for the thread selects() on the connections and
         * dispatches ready I/O work requests, then registers all pending
         * newly accepted connections and updates any interest ops on the
         * queue.
         */
        public void run() {
            try {

                // TODO_MA 注释： 循环
                while(!stopped) {
                    try {

                        /*************************************************
                         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                         *  注释： 执行 select： 处理读写请求
                         */
                        select();

                        /*************************************************
                         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                         *  注释： 给完成链接的客户端注册 OP_READ 事件，并生成 NIOServerCnxn，并且完成注册
                         */
                        processAcceptedConnections();

                        /*************************************************
                         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                         *  注释： 注册 OP_READ 或者 OP_WRITE 事件
                         */
                        processInterestOpsUpdateRequests();
                    } catch(RuntimeException e) {
                        LOG.warn("Ignoring unexpected runtime exception", e);
                    } catch(Exception e) {
                        LOG.warn("Ignoring unexpected exception", e);
                    }
                }

                // TODO_MA 注释： 如果退出了这个循环，意味着 服务端 shut down 了

                // Close connections still pending on the selector. Any others
                // with in-flight work, let drain out of the work queue.
                for(SelectionKey key : selector.keys()) {
                    NIOServerCnxn cnxn = (NIOServerCnxn) key.attachment();
                    if(cnxn.isSelectable()) {
                        cnxn.close(ServerCnxn.DisconnectReason.SERVER_SHUTDOWN);
                    }
                    cleanupSelectionKey(key);
                }
                SocketChannel accepted;
                while((accepted = acceptedQueue.poll()) != null) {
                    fastCloseSock(accepted);
                }
                updateQueue.clear();
            } finally {
                closeSelector();
                // This will wake up the accept thread and the other selector
                // threads, and tell the worker thread pool to begin shutdown.
                NIOServerCnxnFactory.this.stop();
                LOG.info("selector thread exitted run method");
            }
        }

        private void select() {
            try {

            	// TODO_MA 注释： 这个地方阻塞，SelectThread.wakeupSelector();
                selector.select();
                Set<SelectionKey> selected = selector.selectedKeys();
                ArrayList<SelectionKey> selectedList = new ArrayList<SelectionKey>(selected);
                Collections.shuffle(selectedList);

                // TODO_MA 注释： 迭代 SelectionKey 执行处理
                Iterator<SelectionKey> selectedKeys = selectedList.iterator();
                while(!stopped && selectedKeys.hasNext()) {
                    SelectionKey key = selectedKeys.next();
                    selected.remove(key);

                    if(!key.isValid()) {
                        cleanupSelectionKey(key);
                        continue;
                    }

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 处理读写请求
                     */
                    if(key.isReadable() || key.isWritable()) {

                        /*************************************************
                         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                         *  注释：
                         *  1、客户端发送数据过来
                         *  2、服务端需要给客户端写数据过去
                         */
                        handleIO(key);
                    } else {
                        LOG.warn("Unexpected ops in select {}", key.readyOps());
                    }
                }
            } catch(IOException e) {
                LOG.warn("Ignoring IOException while selecting", e);
            }
        }

        /**
         * Schedule I/O for processing on the connection associated with
         * the given SelectionKey. If a worker thread pool is not being used,
         * I/O is run directly by this thread.
         */
        private void handleIO(SelectionKey key) {

            // TODO_MA 注释： 生成一个 IOWorkRequest
            IOWorkRequest workRequest = new IOWorkRequest(this, key);
            NIOServerCnxn cnxn = (NIOServerCnxn) key.attachment();

            // Stop selecting this key while processing on its connection
            cnxn.disableSelectable();
            key.interestOps(0);

            // TODO_MA 注释：  session 每次活跃的时候，都要更新自己的超时信息
            touchCnxn(cnxn);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 调度请求处理
             *  workerPool = WorkerServer（List workers） => 线程池
             */
            workerPool.schedule(workRequest);
        }

        /**
         * Iterate over the queue of accepted connections that have been
         * assigned to this thread but not yet placed on the selector.
         */
        private void processAcceptedConnections() {
            SocketChannel accepted;

            // TODO_MA 注释： acceptedQueue 不为空，意味着有新建的链接
            while(!stopped && (accepted = acceptedQueue.poll()) != null) {
                SelectionKey key = null;
                try {

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 注册 OP_READ 事件
                     *  注册这个 SelectionKey.OP_READ 的目的，就是 服务端已经完成了和 客户端的网络连接
                     *  接下来，客户端会立即发送 ConnectRequest 过来的
                     */
                    key = accepted.register(selector, SelectionKey.OP_READ);

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 创建链接
                     *  accepted = SocketChannel
                     */
                    NIOServerCnxn cnxn = createConnection(accepted, key, this);
                    key.attach(cnxn);

                    // TODO_MA 注释： 客户端注册
                    addCnxn(cnxn);
                } catch(IOException e) {
                    // register, createConnection
                    cleanupSelectionKey(key);
                    fastCloseSock(accepted);
                }
            }
        }

        /**
         * Iterate over the queue of connections ready to resume selection,
         * and restore their interest ops selection mask.
         */
        private void processInterestOpsUpdateRequests() {
            SelectionKey key;

            // TODO_MA 注释：
            while(!stopped && (key = updateQueue.poll()) != null) {
                if(!key.isValid()) {
                    cleanupSelectionKey(key);
                }

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释：
                 */
                NIOServerCnxn cnxn = (NIOServerCnxn) key.attachment();
                if(cnxn.isSelectable()) {
                    key.interestOps(cnxn.getInterestOps());
                }
            }
        }
    }

    /**
     * IOWorkRequest is a small wrapper class to allow doIO() calls to be
     * run on a connection using a WorkerService.
     */
    private class IOWorkRequest extends WorkerService.WorkRequest {

        private final SelectorThread selectorThread;
        private final SelectionKey key;

        // TODO_MA 注释： 给某一个客户端提供服务的 服务组件： NIOServerCnxn ==> ClientCnxn
        private final NIOServerCnxn cnxn;

        IOWorkRequest(SelectorThread selectorThread, SelectionKey key) {
            this.selectorThread = selectorThread;
            this.key = key;
            this.cnxn = (NIOServerCnxn) key.attachment();
        }

        public void doWork() throws InterruptedException {
            if(!key.isValid()) {
                selectorThread.cleanupSelectionKey(key);
                return;
            }

            if(key.isReadable() || key.isWritable()) {

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 服务端，执行 IO 处理
                 *  1、cnxn 就是一个唯一的服务组件
                 *  2、key 中会告诉我们，到底是 read 还是 write
                 */
                cnxn.doIO(key);

                // Check if we shutdown or doIO() closed this connection
                if(stopped) {
                    cnxn.close(ServerCnxn.DisconnectReason.SERVER_SHUTDOWN);
                    return;
                }
                if(!key.isValid()) {
                    selectorThread.cleanupSelectionKey(key);
                    return;
                }

                // TODO_MA 注释： 更新会话信息
                touchCnxn(cnxn);
            }

            // Mark this connection as once again ready for selection
            cnxn.enableSelectable();

            // Push an update request on the queue to resume selecting
            // on the current set of interest ops, which may have changed
            // as a result of the I/O operations we just performed.
            if(!selectorThread.addInterestOpsUpdateRequest(key)) {
                cnxn.close(ServerCnxn.DisconnectReason.CONNECTION_MODE_CHANGED);
            }
        }

        @Override
        public void cleanup() {
            cnxn.close(ServerCnxn.DisconnectReason.CLEAN_UP);
        }

    }

    /**
     * This thread is responsible for closing stale connections so that
     * connections on which no session is established are properly expired.
     */
    private class ConnectionExpirerThread extends ZooKeeperThread {

        ConnectionExpirerThread() {
            super("ConnnectionExpirer");
        }

        public void run() {
            try {
                while(!stopped) {

                    // TODO_MA 注释： 每隔一段时间
                    // TODO_MA 注释： 现在是一个时刻 t1, 下一个过期时刻 是t2
                    // TODO_MA 注释： 还得等： waitTime = t2 - t1
                    long waitTime = cnxnExpiryQueue.getWaitTime();
                    if(waitTime > 0) {
                        Thread.sleep(waitTime);
                        continue;
                    }

                    // TODO_MA 注释： 每次session 有新的操作的时候，超时时间会重置
                    // TODO_MA 注释： 通过一个方法来实现： touchCnxn();

                    // TODO_MA 注释： 获取该失效的客户端链接，执行会话超时处理
                    for(NIOServerCnxn conn : cnxnExpiryQueue.poll()) {
                        ServerMetrics.getMetrics().SESSIONLESS_CONNECTIONS_EXPIRED.add(1);
                        conn.close(ServerCnxn.DisconnectReason.CONNECTION_EXPIRED);
                    }
                }

            } catch(InterruptedException e) {
                LOG.info("ConnnectionExpirerThread interrupted");
            }
        }

    }

    ServerSocketChannel ss;

    /**
     * We use this buffer to do efficient socket I/O. Because I/O is handled
     * by the worker threads (or the selector threads directly, if no worker
     * thread pool is created), we can create a fixed set of these to be
     * shared by connections.
     */
    private static final ThreadLocal<ByteBuffer> directBuffer = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            return ByteBuffer.allocateDirect(directBufferBytes);
        }
    };

    public static ByteBuffer getDirectBuffer() {
        return directBufferBytes > 0 ? directBuffer.get() : null;
    }

    // ipMap is used to limit connections per IP
    private final ConcurrentHashMap<InetAddress, Set<NIOServerCnxn>> ipMap = new ConcurrentHashMap<InetAddress, Set<NIOServerCnxn>>();

    protected int maxClientCnxns = 60;
    int listenBacklog = -1;

    int sessionlessCnxnTimeout;
    private ExpiryQueue<NIOServerCnxn> cnxnExpiryQueue;

    protected WorkerService workerPool;

    private static int directBufferBytes;
    private int numSelectorThreads;
    private int numWorkerThreads;
    private long workerShutdownTimeoutMS;

    /**
     * Construct a new server connection factory which will accept an unlimited number
     * of concurrent connections from each client (up to the file descriptor
     * limits of the operating system). startup(zks) must be called subsequently.
     */
    public NIOServerCnxnFactory() {
    }

    private volatile boolean stopped = true;
    private ConnectionExpirerThread expirerThread;
    private AcceptThread acceptThread;
    private final Set<SelectorThread> selectorThreads = new HashSet<SelectorThread>();

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： NIOSeverCnxnFactory 的启动分为两个部分
     *  1、NIOSeverCnxnFactory.configure();
     *      初始化 AcceptThread 和 SelectorThread
     *  2、NIOSeverCnxnFactory.start();
     *      初始化 WorkerThread
     */
    @Override
    public void configure(InetSocketAddress addr, int maxcc, int backlog, boolean secure) throws IOException {
        if(secure) {
            throw new UnsupportedOperationException("SSL isn't supported in NIOServerCnxn");
        }
        configureSaslLogin();
        maxClientCnxns = maxcc;

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 启动会话过期线程
         *  很优秀的一种，桶管理机制
         */
        initMaxCnxns();

        // TODO_MA 注释： 管理 NIOServerCnxn 的超时
        sessionlessCnxnTimeout = Integer.getInteger(ZOOKEEPER_NIO_SESSIONLESS_CNXN_TIMEOUT, 10000);
        // We also use the sessionlessCnxnTimeout as expiring interval for
        // cnxnExpiryQueue. These don't need to be the same, but the expiring
        // interval passed into the ExpiryQueue() constructor below should be
        // less than or equal to the timeout.
        cnxnExpiryQueue = new ExpiryQueue<NIOServerCnxn>(sessionlessCnxnTimeout);
        expirerThread = new ConnectionExpirerThread();

        // TODO_MA 注释： 获取 cpu 个数
        int numCores = Runtime.getRuntime().availableProcessors();

        // TODO_MA 注释： 注意计算规则，动态的根据资源来选择合适的 selector 线程
        // TODO_MA 注释： cpucores = 32 ==> (32/2 开根号 跟1 求最大值)
        // TODO_MA 注释： 32 个  32/2 = 16 => 4
        // TODO_MA 注释： 16 个  16/2 => 8 => 2
        // TODO_MA 注释： 64 个 = 64/2 = 32 = 5个线程
		// TODO_MA 注释： 128 个 = 128 / 2 = 64 = 8 个线程
        // 32 cores sweet spot seems to be 4 selector threads
        numSelectorThreads = Integer
                .getInteger(ZOOKEEPER_NIO_NUM_SELECTOR_THREADS, Math.max((int) Math.sqrt((float) numCores / 2), 1));
        if(numSelectorThreads < 1) {
            throw new IOException("numSelectorThreads must be at least 1");
        }

        // TODO_MA 注释： worker 线程数量： 2 倍 cpu 数量
        numWorkerThreads = Integer.getInteger(ZOOKEEPER_NIO_NUM_WORKER_THREADS, 2 * numCores);

        // TODO_MA 注释： worker 线程超时时间 50s
        workerShutdownTimeoutMS = Long.getLong(ZOOKEEPER_NIO_SHUTDOWN_TIMEOUT, 5000);

        String logMsg = "Configuring NIO connection handler with " + (sessionlessCnxnTimeout / 1000) + "s sessionless connection timeout, " + numSelectorThreads + " selector thread(s), " + (numWorkerThreads > 0 ? numWorkerThreads : "no") + " worker threads, and " + (directBufferBytes == 0 ? "gathered writes." : ("" + (directBufferBytes / 1024) + " kB direct buffers."));
        LOG.info(logMsg);

        // TODO_MA 注释： 初始化 SelectorThread
        for(int i = 0; i < numSelectorThreads; ++i) {
            selectorThreads.add(new SelectorThread(i));
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 启动 NIO 服务端 ServerSocketChannel
         */
        listenBacklog = backlog;
        this.ss = ServerSocketChannel.open();
        ss.socket().setReuseAddress(true);

        // TODO_MA 马中华 注释： 绑定端口号 2181
        LOG.info("binding to port {}", addr);
        if(listenBacklog == -1) {
            ss.socket().bind(addr);
        } else {
            ss.socket().bind(addr, listenBacklog);
        }
        ss.configureBlocking(false);

        // TODO_MA 注释： 初始化一个 AcceptThread
        acceptThread = new AcceptThread(ss, addr, selectorThreads);
    }

    private void tryClose(ServerSocketChannel s) {
        try {
            s.close();
        } catch(IOException sse) {
            LOG.error("Error while closing server socket.", sse);
        }
    }

    @Override
    public void reconfigure(InetSocketAddress addr) {
        ServerSocketChannel oldSS = ss;
        try {
            acceptThread.setReconfiguring();
            tryClose(oldSS);
            acceptThread.wakeupSelector();
            try {
                acceptThread.join();
            } catch(InterruptedException e) {
                LOG.error("Error joining old acceptThread when reconfiguring client port.", e);
                Thread.currentThread().interrupt();
            }
            this.ss = ServerSocketChannel.open();
            ss.socket().setReuseAddress(true);
            LOG.info("binding to port {}", addr);
            ss.socket().bind(addr);
            ss.configureBlocking(false);
            acceptThread = new AcceptThread(ss, addr, selectorThreads);
            acceptThread.start();
        } catch(IOException e) {
            LOG.error("Error reconfiguring client port to {}", addr, e);
            tryClose(oldSS);
        }
    }

    /**
     * {@inheritDoc}
     */
    public int getMaxClientCnxnsPerHost() {
        return maxClientCnxns;
    }

    /**
     * {@inheritDoc}
     */
    public void setMaxClientCnxnsPerHost(int max) {
        maxClientCnxns = max;
    }

    /**
     * {@inheritDoc}
     */
    public int getSocketListenBacklog() {
        return listenBacklog;
    }

    @Override
    public void start() {
        stopped = false;

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 初始化工作线程
         *  初始化了一个线程池： 线程池的线程个数 = coucore * 2
         */
        if(workerPool == null) {
            workerPool = new WorkerService("NIOWorker", numWorkerThreads, false);
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 启动 SelectorThread
         */
        for(SelectorThread thread : selectorThreads) {
            if(thread.getState() == Thread.State.NEW) {
                thread.start();
            }
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 启动 AcceptThread
         */
        // ensure thread is started once and only once
        if(acceptThread.getState() == Thread.State.NEW) {
            acceptThread.start();
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 启动 ConnectionExpirerThread
         */
        if(expirerThread.getState() == Thread.State.NEW) {
            expirerThread.start();
        }
    }

    @Override
    public void startup(ZooKeeperServer zks, boolean startServer) throws IOException, InterruptedException {
        start();
        setZooKeeperServer(zks);
        if(startServer) {
            zks.startdata();
            zks.startup();
        }
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) ss.socket().getLocalSocketAddress();
    }

    @Override
    public int getLocalPort() {
        return ss.socket().getLocalPort();
    }

    /**
     * De-registers the connection from the various mappings maintained
     * by the factory.
     */
    public boolean removeCnxn(NIOServerCnxn cnxn) {
        // If the connection is not in the master list it's already been closed
        if(!cnxns.remove(cnxn)) {
            return false;
        }
        cnxnExpiryQueue.remove(cnxn);

        removeCnxnFromSessionMap(cnxn);

        InetAddress addr = cnxn.getSocketAddress();
        if(addr != null) {
            Set<NIOServerCnxn> set = ipMap.get(addr);
            if(set != null) {
                set.remove(cnxn);
                // Note that we make no effort here to remove empty mappings
                // from ipMap.
            }
        }

        // unregister from JMX
        unregisterConnection(cnxn);
        return true;
    }

    /**
     * Add or update cnxn in our cnxnExpiryQueue
     *
     * @param cnxn
     */
    public void touchCnxn(NIOServerCnxn cnxn) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 将自己更新到对应的 session bucket 中
         */
        cnxnExpiryQueue.update(cnxn, cnxn.getSessionTimeout());
    }

    private void addCnxn(NIOServerCnxn cnxn) throws IOException {
        InetAddress addr = cnxn.getSocketAddress();
        if(addr == null) {
            throw new IOException("Socket of " + cnxn + " has been closed");
        }

        // TODO_MA 注释： 根据 IP 地址，映射到一个 NIOServerCnxn Set
        Set<NIOServerCnxn> set = ipMap.get(addr);
        if(set == null) {
            // in general we will see 1 connection from each
            // host, setting the initial cap to 2 allows us
            // to minimize mem usage in the common case
            // of 1 entry --  we need to set the initial cap
            // to 2 to avoid rehash when the first entry is added
            // Construct a ConcurrentHashSet using a ConcurrentHashMap
            set = Collections.newSetFromMap(new ConcurrentHashMap<NIOServerCnxn, Boolean>(2));
            // Put the new set in the map, but only if another thread
            // hasn't beaten us to it
            Set<NIOServerCnxn> existingSet = ipMap.putIfAbsent(addr, set);
            if(existingSet != null) {
                set = existingSet;
            }
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 加入集合
         */
        set.add(cnxn);
        cnxns.add(cnxn);

        // TODO_MA 注释： 将自己加入 session expire 管理
        touchCnxn(cnxn);
    }

    protected NIOServerCnxn createConnection(SocketChannel sock, SelectionKey sk,
            SelectorThread selectorThread) throws IOException {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return new NIOServerCnxn(zkServer, sock, sk, this, selectorThread);
    }

    private int getClientCnxnCount(InetAddress cl) {
        Set<NIOServerCnxn> s = ipMap.get(cl);
        if(s == null) {
            return 0;
        }
        return s.size();
    }

    /**
     * clear all the connections in the selector
     */
    @Override
    @SuppressWarnings("unchecked")
    public void closeAll(ServerCnxn.DisconnectReason reason) {
        // clear all the connections on which we are selecting
        for(ServerCnxn cnxn : cnxns) {
            try {
                // This will remove the cnxn from cnxns
                cnxn.close(reason);
            } catch(Exception e) {
                LOG.warn("Ignoring exception closing cnxn session id 0x{}", Long.toHexString(cnxn.getSessionId()), e);
            }
        }
    }

    public void stop() {
        stopped = true;

        // Stop queuing connection attempts
        try {
            ss.close();
        } catch(IOException e) {
            LOG.warn("Error closing listen socket", e);
        }

        if(acceptThread != null) {
            if(acceptThread.isAlive()) {
                acceptThread.wakeupSelector();
            } else {
                acceptThread.closeSelector();
            }
        }
        if(expirerThread != null) {
            expirerThread.interrupt();
        }
        for(SelectorThread thread : selectorThreads) {
            if(thread.isAlive()) {
                thread.wakeupSelector();
            } else {
                thread.closeSelector();
            }
        }
        if(workerPool != null) {
            workerPool.stop();
        }
    }

    public void shutdown() {
        try {
            // close listen socket and signal selector threads to stop
            stop();

            // wait for selector and worker threads to shutdown
            join();

            // close all open connections
            closeAll(ServerCnxn.DisconnectReason.SERVER_SHUTDOWN);

            if(login != null) {
                login.shutdown();
            }
        } catch(InterruptedException e) {
            LOG.warn("Ignoring interrupted exception during shutdown", e);
        } catch(Exception e) {
            LOG.warn("Ignoring unexpected exception during shutdown", e);
        }

        if(zkServer != null) {
            zkServer.shutdown();
        }
    }

    @Override
    public void join() throws InterruptedException {
        if(acceptThread != null) {
            acceptThread.join();
        }
        for(SelectorThread thread : selectorThreads) {
            thread.join();
        }
        if(workerPool != null) {
            workerPool.join(workerShutdownTimeoutMS);
        }
    }

    @Override
    public Iterable<ServerCnxn> getConnections() {
        return cnxns;
    }

    public void dumpConnections(PrintWriter pwriter) {
        pwriter.print("Connections ");
        cnxnExpiryQueue.dump(pwriter);
    }

    @Override
    public void resetAllConnectionStats() {
        // No need to synchronize since cnxns is backed by a ConcurrentHashMap
        for(ServerCnxn c : cnxns) {
            c.resetStats();
        }
    }

    @Override
    public Iterable<Map<String, Object>> getAllConnectionInfo(boolean brief) {
        HashSet<Map<String, Object>> info = new HashSet<Map<String, Object>>();
        // No need to synchronize since cnxns is backed by a ConcurrentHashMap
        for(ServerCnxn c : cnxns) {
            info.add(c.getConnectionInfo(brief));
        }
        return info;
    }

}
