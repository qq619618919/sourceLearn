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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 3.x 新版本的方案。
 *  FSEditLogAsync 这个组件的内部，依然用的是双写缓冲！
 */
class FSEditLogAsync extends FSEditLog implements Runnable {

    static final Logger LOG = LoggerFactory.getLogger(FSEditLog.class);

    // use separate mutex to avoid possible deadlock when stopping the thread.
    private final Object syncThreadLock = new Object();

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 同步线程
     */
    private Thread syncThread;

    private static final ThreadLocal<Edit> THREAD_EDIT = new ThreadLocal<Edit>();

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 队列，存储 Edit 日志
     */
    // requires concurrent access from caller threads and syncing thread.
    private final BlockingQueue<Edit> editPendingQ = new ArrayBlockingQueue<Edit>(4096);

    // only accessed by syncing thread so no synchronization required.
    // queue is unbounded because it's effectively limited by the size
    // of the edit log buffer - ie. a sync will eventually be forced.
    // TODO_MA 马中华 注释： 从 editPendingQ 获取 日志，加入syncWaitQ队列等待 flush
    // TODO_MA 马中华 注释： 这就是条件!
    private final Deque<Edit> syncWaitQ = new ArrayDeque<Edit>();

    private long lastFull = 0;

    FSEditLogAsync(Configuration conf, NNStorage storage, List<URI> editsDirs) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        super(conf, storage, editsDirs);

        // op instances cannot be shared due to queuing for background thread.
        cache.disableCache();
    }

    private boolean isSyncThreadAlive() {
        synchronized (syncThreadLock) {
            return syncThread != null && syncThread.isAlive();
        }
    }

    private void startSyncThread() {
        synchronized (syncThreadLock) {
            if (!isSyncThreadAlive()) {
                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释：
                 */
                syncThread = new Thread(this, this.getClass().getSimpleName());
                syncThread.start();
            }
        }
    }

    private void stopSyncThread() {
        synchronized (syncThreadLock) {
            if (syncThread != null) {
                try {
                    syncThread.interrupt();
                    syncThread.join();
                } catch (InterruptedException e) {
                    // we're quitting anyway.
                } finally {
                    syncThread = null;
                }
            }
        }
    }

    @VisibleForTesting
    @Override
    public void restart() {
        stopSyncThread();
        startSyncThread();
    }

    @Override
    void openForWrite(int layoutVersion) throws IOException {
        try {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 启动异步处理消费线程
             */
            startSyncThread();

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 调用 FSEditLog 的 openForWrite
             */
            super.openForWrite(layoutVersion);
        } catch (IOException ioe) {
            stopSyncThread();
            throw ioe;
        }
    }

    @Override
    public void close() {
        super.close();
        stopSyncThread();
    }

    @Override
    void logEdit(final FSEditLogOp op) {

        // TODO_MA 马中华 注释： 将 OP 转换成Edit
        Edit edit = getEditInstance(op);

        // TODO_MA 马中华 注释： 保存线程副本
        THREAD_EDIT.set(edit);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 加入日志到队列中
         */
        enqueueEdit(edit);
    }

    @Override
    public void logSync() {
        Edit edit = THREAD_EDIT.get();
        if (edit != null) {
            // do NOT remove to avoid expunge & rehash penalties.
            THREAD_EDIT.set(null);
            if (LOG.isDebugEnabled()) {
                LOG.debug("logSync " + edit);
            }
            // TODO_MA 马中华 注释： 阻塞等待
            edit.logSyncWait();
        }
    }

    @Override
    public void logSyncAll() {
        // doesn't actually log anything, just ensures that the queues are
        // drained when it returns.
        Edit edit = new SyncEdit(this, null) {
            @Override
            public boolean logEdit() {
                return true;
            }
        };
        enqueueEdit(edit);
        edit.logSyncWait();
    }

    // draining permits is intended to provide a high priority reservation.
    // however, release of outstanding permits must be postponed until
    // drained permits are restored to avoid starvation.  logic has some races
    // but is good enough to serve its purpose.
    private Semaphore overflowMutex = new Semaphore(8) {
        private AtomicBoolean draining = new AtomicBoolean();
        private AtomicInteger pendingReleases = new AtomicInteger();

        @Override
        public int drainPermits() {
            draining.set(true);
            return super.drainPermits();
        }

        // while draining, count the releases until release(int)
        private void tryRelease(int permits) {
            pendingReleases.getAndAdd(permits);
            if (!draining.get()) {
                super.release(pendingReleases.getAndSet(0));
            }
        }

        @Override
        public void release() {
            tryRelease(1);
        }

        @Override
        public void release(int permits) {
            draining.set(false);
            tryRelease(permits);
        }
    };

    private void enqueueEdit(Edit edit) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("logEdit " + edit);
        }
        try {
            // not checking for overflow yet to avoid penalizing performance of
            // the common case.  if there is persistent overflow, a mutex will be
            // use to throttle contention on the queue.
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 将 Edit 加入到 editPendingQ 队列
             */
            if (!editPendingQ.offer(edit)) {

                Preconditions.checkState(isSyncThreadAlive(), "sync thread is not alive");
                long now = Time.monotonicNow();
                if (now - lastFull > 4000) {
                    lastFull = now;
                    LOG.info("Edit pending queue is full");
                }
                if (Thread.holdsLock(this)) {
                    // if queue is full, synchronized caller must immediately relinquish
                    // the monitor before re-offering to avoid deadlock with sync thread
                    // which needs the monitor to write transactions.
                    int permits = overflowMutex.drainPermits();
                    try {
                        do {
                            this.wait(1000); // will be notified by next logSync.
                        } while (!editPendingQ.offer(edit));
                    } finally {
                        overflowMutex.release(permits);
                    }
                } else {
                    // mutex will throttle contention during persistent overflow.
                    overflowMutex.acquire();
                    try {
                        editPendingQ.put(edit);
                    } finally {
                        overflowMutex.release();
                    }
                }
            }
        } catch (Throwable t) {
            // should never happen!  failure to enqueue an edit is fatal
            terminate(t);
        }
    }

    private Edit dequeueEdit() throws InterruptedException {
        // only block for next edit if no pending syncs.
        return syncWaitQ.isEmpty() ? editPendingQ.take() : editPendingQ.poll();
    }

    @Override
    public void run() {
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 方法的内部逻辑，分为两个部分：
         *  1、从队列中，获取 Edit 日志，加入同步队列，logEdit 是一个同步方法
         *  2、logSync 执行 flush， 内部单独的锁
         *  重点：输出日志记录 和 刷新缓冲区数据到磁盘 两个动作分开，从而提高吞吐
         */
        try {
            while (true) {
                NameNodeMetrics metrics = NameNode.getNameNodeMetrics();
                boolean doSync;

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 出队
                 */
                Edit edit = dequeueEdit();
                if (edit != null) {

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 记录日志
                     */
                    // sync if requested by edit log.
                    doSync = edit.logEdit();

                    // TODO_MA 马中华 注释： 带 sync 的 edit 加入到 syncWaitQ
                    syncWaitQ.add(edit);

                    metrics.setPendingEditsCount(editPendingQ.size() + 1);
                } else {
                    // sync when editq runs dry, but have edits pending a sync.
                    doSync = !syncWaitQ.isEmpty();
                    metrics.setPendingEditsCount(0);
                }
                if (doSync) {
                    // normally edit log exceptions cause the NN to terminate, but tests
                    // relying on ExitUtil.terminate need to see the exception.
                    RuntimeException syncEx = null;
                    try {
                        /*************************************************
                         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                         *  注释： 执行 flush，完成真正的写
                         */
                        logSync(getLastWrittenTxId());
                    } catch (RuntimeException ex) {
                        syncEx = ex;
                    }

                    // TODO_MA 马中华 注释： 唤醒
                    while ((edit = syncWaitQ.poll()) != null) {
                        edit.logSyncNotify(syncEx);
                    }
                }
            }
        } catch (InterruptedException ie) {
            LOG.info(Thread.currentThread().getName() + " was interrupted, exiting");
        } catch (Throwable t) {
            terminate(t);
        }
    }

    private void terminate(Throwable t) {
        String message = "Exception while edit logging: " + t.getMessage();
        LOG.error(message, t);
        ExitUtil.terminate(1, message);
    }

    private Edit getEditInstance(FSEditLogOp op) {
        final Edit edit;
        final Server.Call rpcCall = Server.getCurCall().get();

        // TODO_MA 马中华 注释： 只有未在日志上显式同步的 rpc 调用才会是异步的。
        // only rpc calls not explicitly sync'ed on the log will be async.
        if (rpcCall != null && !Thread.holdsLock(this)) {
            edit = new RpcEdit(this, op, rpcCall);
        } else {
            edit = new SyncEdit(this, op);
        }
        return edit;
    }

    private abstract static class Edit {
        final FSEditLog log;
        final FSEditLogOp op;

        Edit(FSEditLog log, FSEditLogOp op) {
            this.log = log;
            this.op = op;
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         *  1、关于 内存操作 和 磁盘操作，其实在一个锁里面
         *  2、关于磁盘操作，其实时候在一个 事务里面
         *      1、写本地日志
         *      2、写 journal 日志， 过半写成功
         */
        // return whether edit log wants to sync.
        boolean logEdit() {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            return log.doEditTransaction(op);
        }

        // wait for background thread to finish syncing.
        abstract void logSyncWait();

        // wake up the thread in logSyncWait.
        abstract void logSyncNotify(RuntimeException ex);
    }

    // the calling thread is synchronously waiting for the edit to complete.
    private static class SyncEdit extends Edit {
        private final Object lock;
        private boolean done = false;
        private RuntimeException syncEx;

        SyncEdit(FSEditLog log, FSEditLogOp op) {
            super(log, op);
            // if the log is already sync'ed (ex. log rolling), must wait on it to
            // avoid deadlock with sync thread.  the fsn lock protects against
            // logging during a roll.  else lock on this object to avoid sync
            // contention on edit log.
            lock = Thread.holdsLock(log) ? log : this;
        }

        @Override
        public void logSyncWait() {
            synchronized (lock) {

                // TODO_MA 马中华 注释： 阻塞等待
                // TODO_MA 马中华 注释： 等待 done 完成
                while (!done) {
                    try {
                        lock.wait(10);
                    } catch (InterruptedException e) {
                    }
                }
                // only needed by tests that rely on ExitUtil.terminate() since
                // normally exceptions terminate the NN.
                if (syncEx != null) {
                    syncEx.fillInStackTrace();
                    throw syncEx;
                }
            }
        }

        @Override
        public void logSyncNotify(RuntimeException ex) {
            synchronized (lock) {

                // TODO_MA 马中华 注释： 重要，标记完成
                done = true;
                syncEx = ex;
                lock.notifyAll();
            }
        }

        @Override
        public String toString() {
            return "[" + getClass().getSimpleName() + " op:" + op + "]";
        }
    }

    // the calling rpc thread will return immediately from logSync but the
    // rpc response will not be sent until the edit is durable.
    private static class RpcEdit extends Edit {
        private final Server.Call call;

        RpcEdit(FSEditLog log, FSEditLogOp op, Server.Call call) {
            super(log, op);
            this.call = call;
            call.postponeResponse();
        }

        @Override
        public void logSyncWait() {
            // logSync is a no-op to immediately free up rpc handlers.  the
            // response is sent when the sync thread calls syncNotify.
        }

        @Override
        public void logSyncNotify(RuntimeException syncEx) {
            try {
                if (syncEx == null) {
                    call.sendResponse();
                } else {
                    call.abortResponse(syncEx);
                }
            } catch (Exception e) {
            } // don't care if not sent.
        }

        @Override
        public String toString() {
            return "[" + getClass().getSimpleName() + " op:" + op + " call:" + call + "]";
        }
    }
}
