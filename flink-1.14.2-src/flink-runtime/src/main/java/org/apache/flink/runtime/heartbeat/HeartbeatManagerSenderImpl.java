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
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * {@link HeartbeatManager} implementation which regularly requests a heartbeat response from its
 * monitored {@link HeartbeatTarget}. The heartbeat period is configurable.
 *
 * @param <I> Type of the incoming heartbeat payload
 * @param <O> Type of the outgoing heartbeat payload
 */
public class HeartbeatManagerSenderImpl<I, O> extends HeartbeatManagerImpl<I, O> implements Runnable {

    private final long heartbeatPeriod;

    HeartbeatManagerSenderImpl(long heartbeatPeriod,
                               long heartbeatTimeout,
                               int failedRpcRequestsUntilUnreachable,
                               ResourceID ownResourceID,
                               HeartbeatListener<I, O> heartbeatListener,
                               ScheduledExecutor mainThreadExecutor,
                               Logger log) {
        this(heartbeatPeriod,
                heartbeatTimeout,
                failedRpcRequestsUntilUnreachable,
                ownResourceID,
                heartbeatListener,
                mainThreadExecutor,
                log,
                new HeartbeatMonitorImpl.Factory<>()
        );
    }

    HeartbeatManagerSenderImpl(long heartbeatPeriod,
                               long heartbeatTimeout,
                               int failedRpcRequestsUntilUnreachable,
                               ResourceID ownResourceID,
                               HeartbeatListener<I, O> heartbeatListener,
                               ScheduledExecutor mainThreadExecutor,
                               Logger log,
                               HeartbeatMonitor.Factory<O> heartbeatMonitorFactory) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： HeartbeatManagerSenderImpl 是 HeartbeatManagerImpl 的子类
         *  这个 super 就是调用 HeartbeatManagerImpl 的构造方法
         */
        super(heartbeatTimeout,
                failedRpcRequestsUntilUnreachable,
                ownResourceID,
                heartbeatListener,
                mainThreadExecutor,
                log,
                heartbeatMonitorFactory
        );

        this.heartbeatPeriod = heartbeatPeriod;

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 启动了一个运行一次的定时任务： 立马执行一下 this 这个任务
         *  所以来看： HeartbeatManagerSenderImpl 的 run()
         */
        mainThreadExecutor.schedule(this, 0L, TimeUnit.MILLISECONDS);
    }

    // TODO_MA 马中华 注释： 这个 this.run() 就进入到了一个 每隔 10s 执行一次的循环
    @Override
    public void run() {

        // TODO_MA 马中华 注释： 心跳工作组件还在工作
        if (!stopped) {
            log.debug("Trigger heartbeat request.");

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 第一件事： 给所有的 心跳目标对象 发送 心跳 RPC 请求
             *  1、ResourceManager 给所有注册成功的从节点 TaskExecutor 发送 requestHeartbeat 这个 心跳 RPC 请求
             *  2、ResourceManager 给所有注册成功的 JobMaster 发送 requestHeartbeat 这个 心跳 RPC 请求
             *  3、JobMaster 给所有注册成功的 TaskExecutor 发送 requestHeartbeat 这个 心跳 RPC 请求
             */
            for (HeartbeatMonitor<O> heartbeatMonitor : getHeartbeatTargets().values()) {

                // TODO_MA 马中华 注释： 发送心跳 RPC 请求
                requestHeartbeat(heartbeatMonitor);
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 又做了一定定时调度： 延迟多少时间之后，执行 this 这个任务
             *  heartbeatPeriod = 10s
             */
            getMainThreadExecutor().schedule(this, heartbeatPeriod, TimeUnit.MILLISECONDS);
        }
    }

    // TODO_MA 马中华 注释： 1、每隔  10s 发送一次心跳
    // TODO_MA 马中华 注释： 2、上一次心跳发送完毕之后 等待 10s 发送下一次心跳
    // TODO_MA 马中华 注释： 问题1： 这两种表达有区别么？ 有区别
    // TODO_MA 马中华 注释： 问题2： flink的心跳是第一个还是第二种呢？
    // TODO_MA 马中华 注释： checkpoint 的执行时长 和 间隔时间
    // TODO_MA 马中华 注释： HDFS的心跳时间：3s， 为什么 Flink 是 10s 呢？
    // TODO_MA 马中华 注释： HDFS 的心跳负载不重，Flink的心跳负载相对重一些，还携带有负载汇报
    // TODO_MA 马中华 注释： YARN 的时间时间是 1s 呢？ MRAppMaster 会通过心跳来申请资源

    private void requestHeartbeat(HeartbeatMonitor<O> heartbeatMonitor) {

        // TODO_MA 马中华 注释： 拿到心跳目标对象
        // TODO_MA 马中华 注释： 每个角色 注册的时候，会在对应的管理组件中，生成一个 HeartbeatTarget
        // TODO_MA 马中华 注释： TaskExcutor 到 ResourceManager 注册成功，则会生成一个 HeartbeatTarget 作为 ResourceManager的细腻套目标对象
        // TODO_MA 马中华 注释： TaskExcutor 到 JobMaster 注册成功
        // TODO_MA 马中华 注释： JobMaster 到 ResourceManager 注册成功
        O payload = getHeartbeatListener().retrievePayload(heartbeatMonitor.getHeartbeatTargetId());
        final HeartbeatTarget<O> heartbeatTarget = heartbeatMonitor.getHeartbeatTarget();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 发送 心跳 RPC 请求：  地位高的，向 地位低的去发
         *  该方法，有 6 种情况，有三种情况是无效的
         */
        heartbeatTarget.requestHeartbeat(getOwnResourceID(), payload)
                .whenCompleteAsync(handleHeartbeatRpc(heartbeatMonitor.getHeartbeatTargetId()),
                        getMainThreadExecutor()
                );
    }
}
