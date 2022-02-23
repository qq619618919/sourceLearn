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

package org.apache.flink.runtime.heartbeat;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The default implementation of {@link HeartbeatMonitor}.
 *
 * @param <O> Type of the payload being sent to the associated heartbeat target
 */
public class HeartbeatMonitorImpl<O> implements HeartbeatMonitor<O>, Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(HeartbeatMonitorImpl.class);

    /** Resource ID of the monitored heartbeat target. */
    private final ResourceID resourceID;

    /** Associated heartbeat target. */
    // TODO_MA 马中华 注释： 心跳 HeartbeatTarget
    private final HeartbeatTarget<O> heartbeatTarget;

    private final ScheduledExecutor scheduledExecutor;

    /** Listener which is notified about heartbeat timeouts. */
    // TODO_MA 马中华 注释： 心跳 HeartbeatListener
    private final HeartbeatListener<?, ?> heartbeatListener;

    /** Maximum heartbeat timeout interval. */
    private final long heartbeatTimeoutIntervalMs;

    private final int failedRpcRequestsUntilUnreachable;

    private volatile ScheduledFuture<?> futureTimeout;

    private final AtomicReference<State> state = new AtomicReference<>(State.RUNNING);

    private final AtomicInteger numberFailedRpcRequestsSinceLastSuccess = new AtomicInteger(0);

    private volatile long lastHeartbeat;

    HeartbeatMonitorImpl(ResourceID resourceID,
                         HeartbeatTarget<O> heartbeatTarget,
                         ScheduledExecutor scheduledExecutor,
                         HeartbeatListener<?, O> heartbeatListener,
                         long heartbeatTimeoutIntervalMs,
                         int failedRpcRequestsUntilUnreachable) {

        this.resourceID = Preconditions.checkNotNull(resourceID);
        this.heartbeatTarget = Preconditions.checkNotNull(heartbeatTarget);
        this.scheduledExecutor = Preconditions.checkNotNull(scheduledExecutor);
        this.heartbeatListener = Preconditions.checkNotNull(heartbeatListener);

        Preconditions.checkArgument(heartbeatTimeoutIntervalMs > 0L,
                "The heartbeat timeout interval has to be larger than 0."
        );
        this.heartbeatTimeoutIntervalMs = heartbeatTimeoutIntervalMs;

        Preconditions.checkArgument(failedRpcRequestsUntilUnreachable > 0 || failedRpcRequestsUntilUnreachable == -1,
                "The number of failed heartbeat RPC requests has to be larger than 0 or -1 (deactivated)."
        );
        this.failedRpcRequestsUntilUnreachable = failedRpcRequestsUntilUnreachable;

        lastHeartbeat = 0L;

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        resetHeartbeatTimeout(heartbeatTimeoutIntervalMs);
    }

    @Override
    public HeartbeatTarget<O> getHeartbeatTarget() {
        return heartbeatTarget;
    }

    @Override
    public ResourceID getHeartbeatTargetId() {
        return resourceID;
    }

    @Override
    public long getLastHeartbeat() {
        return lastHeartbeat;
    }

    @Override
    public void reportHeartbeatRpcFailure() {
        final int failedRpcRequestsSinceLastSuccess = numberFailedRpcRequestsSinceLastSuccess.incrementAndGet();

        if (isHeartbeatRpcFailureDetectionEnabled()
                && failedRpcRequestsSinceLastSuccess >= failedRpcRequestsUntilUnreachable) {
            if (state.compareAndSet(State.RUNNING, State.UNREACHABLE)) {
                LOG.debug("Mark heartbeat target {} as unreachable because {} consecutive heartbeat RPCs have failed.",
                        resourceID,
                        failedRpcRequestsSinceLastSuccess
                );

                cancelTimeout();
                heartbeatListener.notifyTargetUnreachable(resourceID);
            }
        }
    }

    private boolean isHeartbeatRpcFailureDetectionEnabled() {
        return failedRpcRequestsUntilUnreachable > 0;
    }

    @Override
    public void reportHeartbeatRpcSuccess() {
        numberFailedRpcRequestsSinceLastSuccess.set(0);
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： reportHeartbeat 方法的作用，是更新一次心跳。既然更新了一次心跳，则需要重置 心跳超时任务
     *  如果上一次心跳登记之后，重置了心跳之后，再也没有心跳，则这个定时任务不会被重置。
     *  如果这个定时任务经过了 50s 也没有被重置，或者取消，则该定时任务就执行了。
     *  -
     *  定时炸弹： 如果 50s 之内没有人取消，则爆炸
     */
    @Override
    public void reportHeartbeat() {

        // TODO_MA 马中华 注释： 登记刚才的这一次的心跳时间
        lastHeartbeat = System.currentTimeMillis();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 重置心跳超时任务
         */
        resetHeartbeatTimeout(heartbeatTimeoutIntervalMs);
    }

    @Override
    public void cancel() {
        // we can only cancel if we are in state running
        if (state.compareAndSet(State.RUNNING, State.CANCELED)) {
            cancelTimeout();
        }
    }

    // TODO_MA 马中华 注释： 既然没有心跳了，大概率是链接已经断开！
    // TODO_MA 马中华 注释： 那为什么还要这么做呢？
    // TODO_MA 马中华 注释： 1、既然确实是因为链接断开导致 心跳超时，早就已经发送 Task 心跳的。
    // TODO_MA 马中华 注释： 2、为了预防 心跳的丢失不是因为链接断开
    @Override
    public void run() {
        // The heartbeat has timed out if we're in state running
        if (state.compareAndSet(State.RUNNING, State.TIMEOUT)) {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 心跳超时处理
             *  Flink 中的 ResourceManager 和 TaskExecutor 和 JobMaster 的两两之间，都存在心跳
             *  -
             *  如果一个 从节点，经过 50s 了也没有给 ResourceManager 心跳回复。
             *  ResourceManager 都会关闭和这个 TaskExecutor 之间的链接
             *  1、假设这个 TaskExecutor 还运行着有没有执行完的 Task
             *      1、关闭这个 Task
             *      2、做 task 的 failover ： 转移到另外一个 TaskExecutor 去执行
             *  Task 级别的容错： FailoverStategy
             *  Job 级别的容错： RestartStrategy
             */
            heartbeatListener.notifyHeartbeatTimeout(resourceID);
        }
    }

    public boolean isCanceled() {
        return state.get() == State.CANCELED;
    }

    void resetHeartbeatTimeout(long heartbeatTimeout) {
        if (state.get() == State.RUNNING) {

            // TODO_MA 马中华 注释： 取消超时任务
            cancelTimeout();

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 启动一个定时任务
             */
            futureTimeout = scheduledExecutor.schedule(this, heartbeatTimeout, TimeUnit.MILLISECONDS);

            // Double check for concurrent accesses (e.g. a firing of the scheduled future)
            if (state.get() != State.RUNNING) {
                cancelTimeout();
            }
        }
    }

    private void cancelTimeout() {
        if (futureTimeout != null) {
            futureTimeout.cancel(true);
        }
    }

    private enum State {
        RUNNING,
        TIMEOUT,
        UNREACHABLE,
        CANCELED
    }

    /**
     * The factory that instantiates {@link HeartbeatMonitorImpl}.
     *
     * @param <O> Type of the outgoing heartbeat payload
     */
    static class Factory<O> implements HeartbeatMonitor.Factory<O> {

        @Override
        public HeartbeatMonitor<O> createHeartbeatMonitor(ResourceID resourceID,
                                                          HeartbeatTarget<O> heartbeatTarget,
                                                          ScheduledExecutor mainThreadExecutor,
                                                          HeartbeatListener<?, O> heartbeatListener,
                                                          long heartbeatTimeoutIntervalMs,
                                                          int failedRpcRequestsUntilUnreachable) {

            return new HeartbeatMonitorImpl<>(resourceID,
                    heartbeatTarget,
                    mainThreadExecutor,
                    heartbeatListener,
                    heartbeatTimeoutIntervalMs,
                    failedRpcRequestsUntilUnreachable
            );
        }
    }
}
