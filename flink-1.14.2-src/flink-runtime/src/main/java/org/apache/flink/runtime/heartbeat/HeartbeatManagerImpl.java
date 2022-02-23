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
import org.apache.flink.runtime.rpc.exceptions.RecipientUnreachableException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Heartbeat manager implementation. The heartbeat manager maintains a map of heartbeat monitors and
 * resource IDs. Each monitor will be updated when a new heartbeat of the associated machine has
 * been received. If the monitor detects that a heartbeat has timed out, it will notify the {@link
 * HeartbeatListener} about it. A heartbeat times out iff no heartbeat signal has been received
 * within a given timeout interval.
 *
 * @param <I> Type of the incoming heartbeat payload
 * @param <O> Type of the outgoing heartbeat payload
 */
@ThreadSafe
public class HeartbeatManagerImpl<I, O> implements HeartbeatManager<I, O> {

    /** Heartbeat timeout interval in milli seconds. */
    private final long heartbeatTimeoutIntervalMs;

    private final int failedRpcRequestsUntilUnreachable;

    /** Resource ID which is used to mark one own's heartbeat signals. */
    private final ResourceID ownResourceID;

    /** Heartbeat listener with which the heartbeat manager has been associated. */
    private final HeartbeatListener<I, O> heartbeatListener;

    /** Executor service used to run heartbeat timeout notifications. */
    private final ScheduledExecutor mainThreadExecutor;

    protected final Logger log;

    /** Map containing the heartbeat monitors associated with the respective resource ID. */
    private final ConcurrentHashMap<ResourceID, HeartbeatMonitor<O>> heartbeatTargets;

    private final HeartbeatMonitor.Factory<O> heartbeatMonitorFactory;

    /** Running state of the heartbeat manager. */
    protected volatile boolean stopped;

    public HeartbeatManagerImpl(long heartbeatTimeoutIntervalMs,
                                int failedRpcRequestsUntilUnreachable,
                                ResourceID ownResourceID,
                                HeartbeatListener<I, O> heartbeatListener,
                                ScheduledExecutor mainThreadExecutor,
                                Logger log) {
        this(heartbeatTimeoutIntervalMs,
                failedRpcRequestsUntilUnreachable,
                ownResourceID,
                heartbeatListener,
                mainThreadExecutor,
                log,
                new HeartbeatMonitorImpl.Factory<>()
        );
    }

    public HeartbeatManagerImpl(long heartbeatTimeoutIntervalMs,
                                int failedRpcRequestsUntilUnreachable,
                                ResourceID ownResourceID,
                                HeartbeatListener<I, O> heartbeatListener,
                                ScheduledExecutor mainThreadExecutor,
                                Logger log,
                                HeartbeatMonitor.Factory<O> heartbeatMonitorFactory) {

        Preconditions.checkArgument(heartbeatTimeoutIntervalMs > 0L, "The heartbeat timeout has to be larger than 0.");

        this.heartbeatTimeoutIntervalMs = heartbeatTimeoutIntervalMs;
        this.failedRpcRequestsUntilUnreachable = failedRpcRequestsUntilUnreachable;
        this.ownResourceID = Preconditions.checkNotNull(ownResourceID);
        this.heartbeatListener = Preconditions.checkNotNull(heartbeatListener, "heartbeatListener");
        this.mainThreadExecutor = Preconditions.checkNotNull(mainThreadExecutor);
        this.log = Preconditions.checkNotNull(log);
        this.heartbeatMonitorFactory = heartbeatMonitorFactory;
        this.heartbeatTargets = new ConcurrentHashMap<>(16);

        stopped = false;
    }

    // ----------------------------------------------------------------------------------------------
    // Getters
    // ----------------------------------------------------------------------------------------------

    ResourceID getOwnResourceID() {
        return ownResourceID;
    }

    HeartbeatListener<I, O> getHeartbeatListener() {
        return heartbeatListener;
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：
     */
    Map<ResourceID, HeartbeatMonitor<O>> getHeartbeatTargets() {
        return heartbeatTargets;
    }

    // ----------------------------------------------------------------------------------------------
    // HeartbeatManager methods
    // ----------------------------------------------------------------------------------------------

    @Override
    public void monitorTarget(ResourceID resourceID, HeartbeatTarget<O> heartbeatTarget) {
        if (!stopped) {
            if (heartbeatTargets.containsKey(resourceID)) {
                log.debug("The target with resource ID {} is already been monitored.",
                        resourceID.getStringWithMetadata()
                );
            } else {

                // TODO_MA 马中华 注释： HeartbeatTarget 被封装成了 heartbeatMonitor
                HeartbeatMonitor<O> heartbeatMonitor = heartbeatMonitorFactory.createHeartbeatMonitor(resourceID,
                        heartbeatTarget,
                        mainThreadExecutor,
                        heartbeatListener,
                        heartbeatTimeoutIntervalMs,
                        failedRpcRequestsUntilUnreachable
                );

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： heartbeatTargets = 就是心跳机制中的 心跳目标对象集合， 一个 Map
                 *  心跳集合
                 */
                heartbeatTargets.put(resourceID, heartbeatMonitor);

                // check if we have stopped in the meantime (concurrent stop operation)
                if (stopped) {
                    heartbeatMonitor.cancel();
                    heartbeatTargets.remove(resourceID);
                }
            }
        }
    }

    @Override
    public void unmonitorTarget(ResourceID resourceID) {
        if (!stopped) {
            HeartbeatMonitor<O> heartbeatMonitor = heartbeatTargets.remove(resourceID);

            if (heartbeatMonitor != null) {
                heartbeatMonitor.cancel();
            }
        }
    }

    @Override
    public void stop() {
        stopped = true;

        for (HeartbeatMonitor<O> heartbeatMonitor : heartbeatTargets.values()) {
            heartbeatMonitor.cancel();
        }

        heartbeatTargets.clear();
    }

    @Override
    public long getLastHeartbeatFrom(ResourceID resourceId) {
        HeartbeatMonitor<O> heartbeatMonitor = heartbeatTargets.get(resourceId);

        if (heartbeatMonitor != null) {
            return heartbeatMonitor.getLastHeartbeat();
        } else {
            return -1L;
        }
    }

    ScheduledExecutor getMainThreadExecutor() {
        return mainThreadExecutor;
    }

    // ----------------------------------------------------------------------------------------------
    // HeartbeatTarget methods
    // ----------------------------------------------------------------------------------------------

    @Override
    public CompletableFuture<Void> receiveHeartbeat(ResourceID heartbeatOrigin, I heartbeatPayload) {
        if (!stopped) {
            log.debug("Received heartbeat from {}.", heartbeatOrigin);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             *  1、A 向 B 发送 心跳请求， B 会记录 A 的最近一次心跳时间
             *  2、B 向 A 返回 响应请求， A 也会记录和 B 的最近一次心跳时间
             */
            reportHeartbeat(heartbeatOrigin);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： Payload 就是负载
             *  心跳请求中，有可能携带了负载汇报，如果有的话，则需要执行处理
             *  就是处理 Slot 的状态
             */
            if (heartbeatPayload != null) {
                heartbeatListener.reportPayload(heartbeatOrigin, heartbeatPayload);
            }else{

            }
        }

        return FutureUtils.completedVoidFuture();
    }

    @Override
    public CompletableFuture<Void> requestHeartbeat(final ResourceID requestOrigin, I heartbeatPayload) {
        if (!stopped) {
            log.debug("Received heartbeat request from {}.", requestOrigin);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 做心跳的处理，其实就是一个登记： 登记刚才这一次心跳的时间
             */
            final HeartbeatTarget<O> heartbeatTarget = reportHeartbeat(requestOrigin);
            // TODO_MA 马中华 注释： 心跳请求处理完了，需要干嘛：返回响应！

            if (heartbeatTarget != null) {
                if (heartbeatPayload != null) {
                    heartbeatListener.reportPayload(requestOrigin, heartbeatPayload);
                }

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： RPC 请求： 地位低的在接收到地位高的组件的心跳 RPC 请求之后完成了处理，需要给对方返回响应
                 *  发送 心跳处理的 RPC 响应请求给 心跳发送方
                 */
                heartbeatTarget
                        .receiveHeartbeat(getOwnResourceID(), heartbeatListener.retrievePayload(requestOrigin))
                        .whenCompleteAsync(handleHeartbeatRpc(requestOrigin), mainThreadExecutor);
            }
        }

        return FutureUtils.completedVoidFuture();
    }

    protected BiConsumer<Void, Throwable> handleHeartbeatRpc(ResourceID heartbeatTarget) {
        return (unused, failure) -> {
            if (failure != null) {
                handleHeartbeatRpcFailure(heartbeatTarget, failure);
            } else {
                handleHeartbeatRpcSuccess(heartbeatTarget);
            }
        };
    }

    private void handleHeartbeatRpcSuccess(ResourceID heartbeatTarget) {
        runIfHeartbeatMonitorExists(heartbeatTarget, HeartbeatMonitor::reportHeartbeatRpcSuccess);
    }

    private void handleHeartbeatRpcFailure(ResourceID heartbeatTarget, Throwable failure) {
        if (failure instanceof RecipientUnreachableException) {
            reportHeartbeatTargetUnreachable(heartbeatTarget);
        }
    }

    private void reportHeartbeatTargetUnreachable(ResourceID heartbeatTarget) {
        runIfHeartbeatMonitorExists(heartbeatTarget, HeartbeatMonitor::reportHeartbeatRpcFailure);
    }

    private void runIfHeartbeatMonitorExists(ResourceID heartbeatTarget, Consumer<? super HeartbeatMonitor<?>> action) {
        final HeartbeatMonitor<O> heartbeatMonitor = heartbeatTargets.get(heartbeatTarget);

        if (heartbeatMonitor != null) {
            action.accept(heartbeatMonitor);
        }
    }

    HeartbeatTarget<O> reportHeartbeat(ResourceID resourceID) {
        if (heartbeatTargets.containsKey(resourceID)) {
            HeartbeatMonitor<O> heartbeatMonitor = heartbeatTargets.get(resourceID);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            heartbeatMonitor.reportHeartbeat();

            return heartbeatMonitor.getHeartbeatTarget();
        } else {
            return null;
        }
    }
}
