/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.util.concurrent.FutureUtils;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： DefaultDispatcherRunner 的生命周期的包装器
 */
final class DispatcherRunnerLeaderElectionLifecycleManager<T extends DispatcherRunner & LeaderContender> implements DispatcherRunner {
    private final T dispatcherRunner;
    private final LeaderElectionService leaderElectionService;

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 其实就为了包装： leaderElectionService.start(dispatcherRunner);
     *  启动选举，已经开始进入服务了。
     */
    private DispatcherRunnerLeaderElectionLifecycleManager(T dispatcherRunner,
                                                           LeaderElectionService leaderElectionService) throws Exception {
        this.dispatcherRunner = dispatcherRunner;
        this.leaderElectionService = leaderElectionService;

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 进入到了重点了： 执行 dispatcherRunner 的选举
         *  最终，会回调 dispatcherRunner 的 grantLeadership()
         *  -
         *  主节点内部有三大组件：
         *  1、resourcemanager  有创建和启动 step3 和 step5
         *  2、dispatcher  step4
         *  3、webmonitorendpoint   有创建和启动 step1 step2
         */
        leaderElectionService.start(dispatcherRunner);
    }

    @Override
    public CompletableFuture<ApplicationStatus> getShutDownFuture() {
        return dispatcherRunner.getShutDownFuture();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        final CompletableFuture<Void> servicesTerminationFuture = stopServices();
        final CompletableFuture<Void> dispatcherRunnerTerminationFuture = dispatcherRunner.closeAsync();

        return FutureUtils.completeAll(Arrays.asList(servicesTerminationFuture, dispatcherRunnerTerminationFuture));
    }

    private CompletableFuture<Void> stopServices() {
        try {
            leaderElectionService.stop();
        } catch (Exception e) {
            return FutureUtils.completedExceptionally(e);
        }

        return FutureUtils.completedVoidFuture();
    }

    public static <T extends DispatcherRunner & LeaderContender> DispatcherRunner createFor(T dispatcherRunner,
                                                                                            LeaderElectionService leaderElectionService) throws Exception {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return new DispatcherRunnerLeaderElectionLifecycleManager<>(dispatcherRunner, leaderElectionService);
    }
}
