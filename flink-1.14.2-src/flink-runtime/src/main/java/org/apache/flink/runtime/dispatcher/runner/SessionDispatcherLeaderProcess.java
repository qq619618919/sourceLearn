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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.client.DuplicateJobSubmissionException;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.FunctionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * Process which encapsulates the job recovery logic and life cycle management of a {@link
 * Dispatcher}.
 */
public class SessionDispatcherLeaderProcess extends AbstractDispatcherLeaderProcess implements JobGraphStore.JobGraphListener {

    private final DispatcherGatewayServiceFactory dispatcherGatewayServiceFactory;

    private final JobGraphStore jobGraphStore;

    private final Executor ioExecutor;

    private CompletableFuture<Void> onGoingRecoveryOperation = FutureUtils.completedVoidFuture();

    private SessionDispatcherLeaderProcess(UUID leaderSessionId, DispatcherGatewayServiceFactory dispatcherGatewayServiceFactory,
                                           JobGraphStore jobGraphStore, Executor ioExecutor, FatalErrorHandler fatalErrorHandler) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        super(leaderSessionId, fatalErrorHandler);

        this.dispatcherGatewayServiceFactory = dispatcherGatewayServiceFactory;
        this.jobGraphStore = jobGraphStore;
        this.ioExecutor = ioExecutor;
    }

    @Override
    protected void onStart() {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 1、 启动 jobGraphStore 对 JobMaster 在 ZK 上的信息的监听
         */
        startServices();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 恢复 job 信息，但是未执行
         */
        onGoingRecoveryOperation = recoverJobsAsync()

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 两件事：
                 *  1、创建 Dispatcher
                 *  2、启动 Dispatcher
                 */.thenAccept(this::createDispatcherIfRunning)
                .handle(this::onErrorIfRunning);
    }

    private void startServices() {
        try {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 启动对 JobMaster 信息的监听
             */
            jobGraphStore.start(this);
        } catch (Exception e) {
            throw new FlinkRuntimeException(String.format("Could not start %s when trying to start the %s.",
                    jobGraphStore.getClass()
                                 .getSimpleName(), getClass().getSimpleName()), e);
        }
    }

    private void createDispatcherIfRunning(Collection<JobGraph> jobGraphs) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        runIfStateIs(State.RUNNING, () -> createDispatcher(jobGraphs));
    }

    private void createDispatcher(Collection<JobGraph> jobGraphs) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        final DispatcherGatewayService dispatcherService = dispatcherGatewayServiceFactory.create(
                DispatcherId.fromUuid(getLeaderSessionId()), jobGraphs, jobGraphStore);

        completeDispatcherSetup(dispatcherService);
    }

    private CompletableFuture<Collection<JobGraph>> recoverJobsAsync() {
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return CompletableFuture.supplyAsync(this::recoverJobsIfRunning, ioExecutor);
    }

    private Collection<JobGraph> recoverJobsIfRunning() {
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 从 ZK 和 HDFS 中，恢复得到 需要恢复执行的 Job 的 JObGraph
         */
        return supplyUnsynchronizedIfRunning(this::recoverJobs).orElse(Collections.emptyList());
    }

    private Collection<JobGraph> recoverJobs() {
        log.info("Recover all persisted job graphs.");

        // TODO_MA 马中华 注释： 拿到一堆 jobID ， 集群停止之前未运行完毕的 job 的 ID
        // TODO_MA 马中华 注释： 这是通过 JobGraphStore 从 ZK 拿到的一堆 JobID 信息
        final Collection<JobID> jobIds = getJobIds();

        final Collection<JobGraph> recoveredJobGraphs = new ArrayList<>();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 只是恢复这个 job 的信息，不是恢复执行
         */
        for (JobID jobId : jobIds) {
            recoveredJobGraphs.add(recoverJob(jobId));
        }

        log.info("Successfully recovered {} persisted job graphs.", recoveredJobGraphs.size());

        // TODO_MA 马中华 注释： 返回这个集合
        return recoveredJobGraphs;
    }

    private Collection<JobID> getJobIds() {
        try {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 从 ZK 上获取 job 信息
             */
            return jobGraphStore.getJobIds();
        } catch (Exception e) {
            throw new FlinkRuntimeException("Could not retrieve job ids of persisted jobs.", e);
        }
    }

    private JobGraph recoverJob(JobID jobId) {
        log.info("Trying to recover job with job id {}.", jobId);
        try {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 是从 jobGraph store 中恢复
             *  这个 jobGraphStore 的实现， 在 HA 集群中，是基于 ZK 实现的
             *  意味着这个 job 的信息，其实存储在 ZK 中
             */
            return jobGraphStore.recoverJobGraph(jobId);
        } catch (Exception e) {
            throw new FlinkRuntimeException(String.format("Could not recover job with job id %s.", jobId), e);
        }
    }

    @Override
    protected CompletableFuture<Void> onClose() {
        return CompletableFuture.runAsync(this::stopServices, ioExecutor);
    }

    private void stopServices() {
        try {
            jobGraphStore.stop();
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }
    }

    // ------------------------------------------------------------
    // JobGraphListener
    // ------------------------------------------------------------

    @Override
    public void onAddedJobGraph(JobID jobId) {
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        runIfStateIs(State.RUNNING, () -> handleAddedJobGraph(jobId));
    }

    private void handleAddedJobGraph(JobID jobId) {
        log.debug("Job {} has been added to the {} by another process.", jobId, jobGraphStore.getClass()
                                                                                             .getSimpleName());

        // serialize all ongoing recovery operations
        onGoingRecoveryOperation = onGoingRecoveryOperation
                // TODO_MA 马中华 注释：
                .thenApplyAsync(ignored -> recoverJobIfRunning(jobId), ioExecutor)
                // TODO_MA 马中华 注释：
                .thenCompose(optionalJobGraph -> optionalJobGraph.flatMap(this::submitAddedJobIfRunning)
                                                                 .orElse(FutureUtils.completedVoidFuture()))
                .handle(this::onErrorIfRunning);
    }

    private Optional<CompletableFuture<Void>> submitAddedJobIfRunning(JobGraph jobGraph) {
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return supplyIfRunning(() -> submitAddedJob(jobGraph));
    }

    private CompletableFuture<Void> submitAddedJob(JobGraph jobGraph) {

        // TODO_MA 马中华 注释：
        final DispatcherGateway dispatcherGateway = getDispatcherGatewayInternal();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return dispatcherGateway.submitJob(jobGraph, RpcUtils.INF_TIMEOUT)
                                .thenApply(FunctionUtils.nullFn())
                                .exceptionally(this::filterOutDuplicateJobSubmissionException);
    }

    private Void filterOutDuplicateJobSubmissionException(Throwable throwable) {
        final Throwable strippedException = ExceptionUtils.stripCompletionException(throwable);
        if (strippedException instanceof DuplicateJobSubmissionException) {
            final DuplicateJobSubmissionException duplicateJobSubmissionException = (DuplicateJobSubmissionException) strippedException;

            log.debug("Ignore recovered job {} because the job is currently being executed.",
                    duplicateJobSubmissionException.getJobID(), duplicateJobSubmissionException);

            return null;
        } else {
            throw new CompletionException(throwable);
        }
    }

    private DispatcherGateway getDispatcherGatewayInternal() {
        return Preconditions.checkNotNull(getDispatcherGateway().getNow(null));
    }

    private Optional<JobGraph> recoverJobIfRunning(JobID jobId) {
        return supplyUnsynchronizedIfRunning(() -> recoverJob(jobId));
    }

    @Override
    public void onRemovedJobGraph(JobID jobId) {
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        runIfStateIs(State.RUNNING, () -> handleRemovedJobGraph(jobId));
    }

    private void handleRemovedJobGraph(JobID jobId) {
        log.debug("Job {} has been removed from the {} by another process.", jobId, jobGraphStore.getClass()
                                                                                                 .getSimpleName());

        onGoingRecoveryOperation = onGoingRecoveryOperation
                // TODO_MA 马中华 注释：
                .thenCompose(ignored -> removeJobGraphIfRunning(jobId).orElse(FutureUtils.completedVoidFuture()))
                .handle(this::onErrorIfRunning);
    }

    private Optional<CompletableFuture<Void>> removeJobGraphIfRunning(JobID jobId) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return supplyIfRunning(() -> removeJobGraph(jobId));
    }

    private CompletableFuture<Void> removeJobGraph(JobID jobId) {
        return getDispatcherService()

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释：
                 */.map(dispatcherService -> dispatcherService.onRemovedJobGraph(jobId))
                .orElseGet(FutureUtils::completedVoidFuture);
    }

    // ---------------------------------------------------------------
    // Factory methods
    // ---------------------------------------------------------------

    public static SessionDispatcherLeaderProcess create(UUID leaderSessionId, DispatcherGatewayServiceFactory dispatcherFactory,
                                                        JobGraphStore jobGraphStore, Executor ioExecutor,
                                                        FatalErrorHandler fatalErrorHandler) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return new SessionDispatcherLeaderProcess(leaderSessionId, dispatcherFactory, jobGraphStore, ioExecutor, fatalErrorHandler);
    }
}
