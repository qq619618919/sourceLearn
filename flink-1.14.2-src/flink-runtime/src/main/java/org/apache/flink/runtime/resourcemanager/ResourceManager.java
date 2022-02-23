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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatListener;
import org.apache.flink.runtime.heartbeat.HeartbeatManager;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.HeartbeatTarget;
import org.apache.flink.runtime.heartbeat.NoOpHeartbeatManager;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.network.partition.DataSetMetaInfo;
import org.apache.flink.runtime.io.network.partition.ResourceManagerPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResourceManagerPartitionTrackerFactory;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.JobMasterRegistrationSuccess;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.exceptions.UnknownTaskExecutorException;
import org.apache.flink.runtime.resourcemanager.registration.JobManagerRegistration;
import org.apache.flink.runtime.resourcemanager.registration.WorkerRegistration;
import org.apache.flink.runtime.resourcemanager.slotmanager.ResourceActions;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rest.messages.LogInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.ThreadDumpInfo;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcServiceUtils;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.FileType;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorHeartbeatPayload;
import org.apache.flink.runtime.taskexecutor.TaskExecutorRegistrationRejection;
import org.apache.flink.runtime.taskexecutor.TaskExecutorRegistrationSuccess;
import org.apache.flink.runtime.taskexecutor.TaskExecutorThreadInfoGateway;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.FutureUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * ResourceManager implementation. The resource manager is responsible for resource de-/allocation
 * and bookkeeping.
 *
 * <p>It offers the following methods as part of its rpc interface to interact with him remotely:
 *
 * <ul>
 *   <li>{@link #registerJobManager(JobMasterId, ResourceID, String, JobID, Time)} registers a
 *       {@link JobMaster} at the resource manager
 * </ul>
 * // TODO_MA 马中华 注释： StandaloneResourcemanager
 * // TODO_MA 马中华 注释： ActiveResourceManager
 * // TODO_MA 马中华 注释： on YARN 的从节点的数量，不是固定不变的。
 * // TODO_MA 马中华 注释： 如果少了，可以继续问 YARN 去申请 Contianer 然后启动 从节点
 * // TODO_MA 马中华 注释： 如果从节点闲置了，则启动该 从节点的 Container 会被回收
 *
 * // TODO_MA 马中华 注释： Resourcemanager 是一个 RpcEndpoint, 关注 onStart 方法
 */
public abstract class ResourceManager<WorkerType extends ResourceIDRetrievable> extends FencedRpcEndpoint<ResourceManagerId> implements ResourceManagerGateway {

    public static final String RESOURCE_MANAGER_NAME = "resourcemanager";

    /** Unique id of the resource manager. */
    private final ResourceID resourceId;

    /** All currently registered JobMasterGateways scoped by JobID. */
    // TODO_MA 马中华 注释： JobMaster 的注册集合
    private final Map<JobID, JobManagerRegistration> jobManagerRegistrations;

    /** All currently registered JobMasterGateways scoped by ResourceID. */
    private final Map<ResourceID, JobManagerRegistration> jmResourceIdRegistrations;

    /** Service to retrieve the job leader ids. */
    private final JobLeaderIdService jobLeaderIdService;

    /** All currently registered TaskExecutors with there framework specific worker information. */
    // TODO_MA 马中华 注释： 从节点 TaskExecutor 的注册集合
    private final Map<ResourceID, WorkerRegistration<WorkerType>> taskExecutors;

    /** Ongoing registration of TaskExecutors per resource ID. */
    private final Map<ResourceID, CompletableFuture<TaskExecutorGateway>> taskExecutorGatewayFutures;

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： HeartbeatServices 心跳服务， 内部两个方法：
     *  1、 心跳主动方
     *  2、 心跳被动方
     */
    private final HeartbeatServices heartbeatServices;

    /** Fatal error handler. */
    private final FatalErrorHandler fatalErrorHandler;

    /** The slot manager maintains the available slots. */
    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： ResourceManager 是资源管理者
     *  1、管理 slot, SlotManager 存在于 ResourceManager中用来管理整个集群的资源的
     *  2、管理 TaskExecutor， 一个注册集合 + 一个心跳机制
     *  3、管理 JobMaster， 一个注册集合 + 一个心跳机制
     *  -
     *  在一个比较复杂的工作组件里面，内部组成的一个通用套路：
     *  1、一系列工作组件： 给当前这个复杂组件提供一些功能实现
     *  2、一系列数据结构： 存储一些关键信息
     *  -
     *  门面模式！组合模式！
     *  ZooKeeper：
     *      ZKDatabase： 管理 zk 的所有数据
     *          DataTree  管理内存数据
     *          FileSnapShot  管理磁盘数据
     *      ServerCnxnFactory： 管理客户端的
     *      ...
     *  一个相对复杂的组件的内部，必然包含了很多的 成员变量（提供行为支持的， 提供存储数据支持的）
     *  你只看你关心的。
     */
    private final SlotManager slotManager;

    private final ResourceManagerPartitionTracker clusterPartitionTracker;

    private final ClusterInformation clusterInformation;

    protected final ResourceManagerMetricGroup resourceManagerMetricGroup;

    protected final Executor ioExecutor;

    private final CompletableFuture<Void> startedFuture;
    /** The heartbeat manager with task managers. */
    private HeartbeatManager<TaskExecutorHeartbeatPayload, Void> taskManagerHeartbeatManager;

    /** The heartbeat manager with job managers. */
    private HeartbeatManager<Void, Void> jobManagerHeartbeatManager;

    public ResourceManager(RpcService rpcService,
                           UUID leaderSessionId,
                           ResourceID resourceId,
                           HeartbeatServices heartbeatServices,
                           SlotManager slotManager,
                           ResourceManagerPartitionTrackerFactory clusterPartitionTrackerFactory,
                           JobLeaderIdService jobLeaderIdService,
                           ClusterInformation clusterInformation,
                           FatalErrorHandler fatalErrorHandler,
                           ResourceManagerMetricGroup resourceManagerMetricGroup,
                           Time rpcTimeout,
                           Executor ioExecutor) {

        super(rpcService, RpcServiceUtils.createRandomName(RESOURCE_MANAGER_NAME), ResourceManagerId.fromUuid(leaderSessionId));

        this.resourceId = checkNotNull(resourceId);
        this.heartbeatServices = checkNotNull(heartbeatServices);
        this.slotManager = checkNotNull(slotManager);
        this.jobLeaderIdService = checkNotNull(jobLeaderIdService);
        this.clusterInformation = checkNotNull(clusterInformation);
        this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
        this.resourceManagerMetricGroup = checkNotNull(resourceManagerMetricGroup);

        this.jobManagerRegistrations = new HashMap<>(4);
        this.jmResourceIdRegistrations = new HashMap<>(4);
        this.taskExecutors = new HashMap<>(8);
        this.taskExecutorGatewayFutures = new HashMap<>(8);

        this.jobManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();
        this.taskManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();

        this.clusterPartitionTracker = checkNotNull(clusterPartitionTrackerFactory).get(
                (taskExecutorResourceId, dataSetIds) -> taskExecutors.get(taskExecutorResourceId).getTaskExecutorGateway()
                        .releaseClusterPartitions(dataSetIds, rpcTimeout).exceptionally(throwable -> {
                            log.debug("Request for release of cluster partitions belonging to data sets {} was not successful.",
                                    dataSetIds, throwable);
                            throw new CompletionException(throwable);
                        }));
        this.ioExecutor = ioExecutor;

        this.startedFuture = new CompletableFuture<>();
    }

    // ------------------------------------------------------------------------
    //  RPC lifecycle methods
    // ------------------------------------------------------------------------

    @Override
    public final void onStart() throws Exception {
        try {
            log.info("Starting the resource manager.");

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 启动 ResourceManager 中的一些服务
             *  1、在创建 ResourceManager 的时候已经创建了连个重要服务
             *      SlotManager
             *      JobLeaderIdService
             */
            startResourceManagerServices();

            startedFuture.complete(null);
        } catch (Throwable t) {
            final ResourceManagerException exception = new ResourceManagerException(
                    String.format("Could not start the ResourceManager %s", getAddress()), t);
            onFatalError(exception);
            throw exception;
        }
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 四件事
     */
    private void startResourceManagerServices() throws Exception {
        try {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 1、jobLeaderIdService 启动
             *  JobMaster 如果出一些问题了，则回调 JobLeaderIdActionsImpl 组件的对应的方法
             */
            jobLeaderIdService.start(new JobLeaderIdActionsImpl());

            registerMetrics();

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 2、启动心跳服务
             *  要想完整了解的心跳的全流程实现，需要了解三个地方：
             *  1、ResourceManager 中的 HeartBeatServices 的初始化
             *  2、ResourceManager 中的 HeartBeatServices 的启动
             *  3、从节点 TaskExecutor 启动和注册
             */
            startHeartbeatServices();

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 3、slotManager 启动
             *  1、启动了两个定时任务，现在改成了一个
             *  关于 slotManager 有两种实现：
             *  1、声明式的， 默认的
             *  2、细粒度的， 需要通过开关来启用
             */
            slotManager.start(getFencingToken(), getMainThreadExecutor(), new ResourceActionsImpl());

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 4、初始化。
             *  1、standalone 模式中，什么也没做
             *  2、on YARN 模式中做了一些事：
             */
            initialize();
        } catch (Exception e) {
            handleStartResourceManagerServicesException(e);
        }
    }

    private void handleStartResourceManagerServicesException(Exception e) throws Exception {
        try {
            stopResourceManagerServices();
        } catch (Exception inner) {
            e.addSuppressed(inner);
        }

        throw e;
    }

    /**
     * Completion of this future indicates that the resource manager is fully started and is ready
     * to serve.
     */
    public CompletableFuture<Void> getStartedFuture() {
        return startedFuture;
    }

    @Override
    public final CompletableFuture<Void> onStop() {
        try {
            stopResourceManagerServices();
        } catch (Exception exception) {
            return FutureUtils.completedExceptionally(
                    new FlinkException("Could not properly shut down the ResourceManager.", exception));
        }

        return CompletableFuture.completedFuture(null);
    }

    private void stopResourceManagerServices() throws Exception {
        Exception exception = null;

        try {
            terminate();
        } catch (Exception e) {
            exception = new ResourceManagerException("Error while shutting down resource manager", e);
        }

        stopHeartbeatServices();

        try {
            slotManager.close();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            jobLeaderIdService.stop();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        resourceManagerMetricGroup.close();

        clearStateInternal();

        ExceptionUtils.tryRethrowException(exception);
    }

    // ------------------------------------------------------------------------
    //  RPC methods
    // ------------------------------------------------------------------------

    // TODO_MA 马中华 注释： RPC 请求接收到了
    @Override
    public CompletableFuture<RegistrationResponse> registerJobManager(final JobMasterId jobMasterId,
                                                                      final ResourceID jobManagerResourceId,
                                                                      final String jobManagerAddress,
                                                                      final JobID jobId,
                                                                      final Time timeout) {
        checkNotNull(jobMasterId);
        checkNotNull(jobManagerResourceId);
        checkNotNull(jobManagerAddress);
        checkNotNull(jobId);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 判断是否已经注册过
         */
        if (!jobLeaderIdService.containsJob(jobId)) {
            try {
                // TODO_MA 马中华 注释： 注册 jobID
                // TODO_MA 马中华 注释： 当没有注册的时候，先注册 JObID
                jobLeaderIdService.addJob(jobId);
            } catch (Exception e) {
                ResourceManagerException exception = new ResourceManagerException(
                        "Could not add the job " + jobId + " to the job id leader service.", e);

                // TODO_MA 马中华 注释： 如果注册失败，则异常退出
                // TODO_MA 马中华 注释： ResourceManager 解析 JobMaster 的注册如果失败，则 ResourceManager所在的 JVM 就强制退出了
                onFatalError(exception);

                log.error("Could not add job {} to job leader id service.", jobId, e);
                return FutureUtils.completedExceptionally(exception);
            }
        }

        log.info("Registering job manager {}@{} for job {}.", jobMasterId, jobManagerAddress, jobId);

        // TODO_MA 马中华 注释： 获取 JobMaster ID
        CompletableFuture<JobMasterId> jobMasterIdFuture;
        try {
            jobMasterIdFuture = jobLeaderIdService.getLeaderId(jobId);
        } catch (Exception e) {
            // we cannot check the job leader id so let's fail
            // TODO: Maybe it's also ok to skip this check in case that we cannot check the leader
            // id
            ResourceManagerException exception = new ResourceManagerException(
                    "Cannot obtain the " + "job leader id future to verify the correct job leader.", e);

            onFatalError(exception);

            log.debug("Could not obtain the job leader id future to verify the correct job leader.");
            return FutureUtils.completedExceptionally(exception);
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 链接 JobMaster
         */
        CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = getRpcService().connect(jobManagerAddress, jobMasterId,
                JobMasterGateway.class);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 完成 Job 注册
         */
        CompletableFuture<RegistrationResponse> registrationResponseFuture = jobMasterGatewayFuture.thenCombineAsync(
                jobMasterIdFuture, (JobMasterGateway jobMasterGateway, JobMasterId leadingJobMasterId) -> {
                    if (Objects.equals(leadingJobMasterId, jobMasterId)) {

                        /*************************************************
                         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                         *  注释： 注册的内部具体实现
                         */
                        return registerJobMasterInternal(jobMasterGateway, jobId, jobManagerAddress, jobManagerResourceId);
                    } else {
                        final String declineMessage = String.format(
                                "The leading JobMaster id %s did not match the received JobMaster id %s. " + "This indicates that a JobMaster leader change has happened.",
                                leadingJobMasterId, jobMasterId);
                        log.debug(declineMessage);
                        return new RegistrationResponse.Failure(new FlinkException(declineMessage));
                    }
                }, getMainThreadExecutor());

        // TODO_MA 马中华 注释： 返回注册响应
        // handle exceptions which might have occurred in one of the futures inputs of combine
        return registrationResponseFuture.handleAsync((RegistrationResponse registrationResponse, Throwable throwable) -> {
            if (throwable != null) {
                if (log.isDebugEnabled()) {
                    log.debug("Registration of job manager {}@{} failed.", jobMasterId, jobManagerAddress, throwable);
                } else {
                    log.info("Registration of job manager {}@{} failed.", jobMasterId, jobManagerAddress);
                }
                return new RegistrationResponse.Failure(throwable);
            } else {
                return registrationResponse;
            }
        }, ioExecutor);
    }

    // TODO_MA 马中华 注释： 主节点处理从节点的注册请求
    // TODO_MA 马中华 注释： 1、先判断链接是否正确
    // TODO_MA 马中华 注释： 2、判断是否已经注册过
    // TODO_MA 马中华 注释： 3、完成注册
    // TODO_MA 马中华 注释： 4、生成响应，返回给对方
    @Override
    public CompletableFuture<RegistrationResponse> registerTaskExecutor(final TaskExecutorRegistration taskExecutorRegistration,
                                                                        final Time timeout) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 完成链接的校验
         */
        CompletableFuture<TaskExecutorGateway> taskExecutorGatewayFuture = getRpcService().connect(
                taskExecutorRegistration.getTaskExecutorAddress(), TaskExecutorGateway.class);
        taskExecutorGatewayFutures.put(taskExecutorRegistration.getResourceId(), taskExecutorGatewayFuture);

        return taskExecutorGatewayFuture.handleAsync((TaskExecutorGateway taskExecutorGateway, Throwable throwable) -> {
            final ResourceID resourceId = taskExecutorRegistration.getResourceId();
            if (taskExecutorGatewayFuture == taskExecutorGatewayFutures.get(resourceId)) {
                taskExecutorGatewayFutures.remove(resourceId);
                if (throwable != null) {
                    return new RegistrationResponse.Failure(throwable);
                } else {
                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 完成 注册的具体的逻辑处理
                     */
                    return registerTaskExecutorInternal(taskExecutorGateway, taskExecutorRegistration);
                }
            } else {
                log.debug("Ignoring outdated TaskExecutorGateway connection for {}.", resourceId.getStringWithMetadata());
                return new RegistrationResponse.Failure(new FlinkException("Decline outdated task executor registration."));
            }
        }, getMainThreadExecutor());
    }

    @Override
    public CompletableFuture<Acknowledge> sendSlotReport(ResourceID taskManagerResourceId,
                                                         InstanceID taskManagerRegistrationId,
                                                         SlotReport slotReport,
                                                         Time timeout) {
        final WorkerRegistration<WorkerType> workerTypeWorkerRegistration = taskExecutors.get(taskManagerResourceId);

        if (workerTypeWorkerRegistration.getInstanceID().equals(taskManagerRegistrationId)) {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            if (slotManager.registerTaskManager(workerTypeWorkerRegistration, slotReport,
                    workerTypeWorkerRegistration.getTotalResourceProfile(),
                    workerTypeWorkerRegistration.getDefaultSlotResourceProfile())) {
                onWorkerRegistered(workerTypeWorkerRegistration.getWorker());
            }
            return CompletableFuture.completedFuture(Acknowledge.get());
        } else {
            return FutureUtils.completedExceptionally(new ResourceManagerException(
                    String.format("Unknown TaskManager registration id %s.", taskManagerRegistrationId)));
        }
    }

    protected void onWorkerRegistered(WorkerType worker) {
        // noop
    }

    // TODO_MA 马中华 注释： 这个代码在主节点中执行的
    @Override
    public CompletableFuture<Void> heartbeatFromTaskManager(final ResourceID resourceID,
                                                            final TaskExecutorHeartbeatPayload heartbeatPayload) {
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： ResourceManager 处理 TaskExecutor 处理完心跳请求之后发回的响应 RPC 请求
         */
        return taskManagerHeartbeatManager.receiveHeartbeat(resourceID, heartbeatPayload);
    }

    @Override
    public CompletableFuture<Void> heartbeatFromJobManager(final ResourceID resourceID) {
        return jobManagerHeartbeatManager.receiveHeartbeat(resourceID, null);
    }

    @Override
    public void disconnectTaskManager(final ResourceID resourceId,
                                      final Exception cause) {
        closeTaskManagerConnection(resourceId, cause);
    }

    @Override
    public void disconnectJobManager(final JobID jobId,
                                     JobStatus jobStatus,
                                     final Exception cause) {
        if (jobStatus.isGloballyTerminalState()) {
            removeJob(jobId, cause);
        } else {
            closeJobManagerConnection(jobId, ResourceRequirementHandling.RETAIN, cause);
        }
    }

    @Override
    public CompletableFuture<Acknowledge> declareRequiredResources(JobMasterId jobMasterId,
                                                                   ResourceRequirements resourceRequirements,
                                                                   Time timeout) {
        final JobID jobId = resourceRequirements.getJobId();
        final JobManagerRegistration jobManagerRegistration = jobManagerRegistrations.get(jobId);

        if (null != jobManagerRegistration) {
            if (Objects.equals(jobMasterId, jobManagerRegistration.getJobMasterId())) {

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释：
                 */
                slotManager.processResourceRequirements(resourceRequirements);

                return CompletableFuture.completedFuture(Acknowledge.get());
            } else {
                return FutureUtils.completedExceptionally(new ResourceManagerException(
                        "The job leader's id " + jobManagerRegistration.getJobMasterId() + " does not match the received id " + jobMasterId + '.'));
            }
        } else {
            return FutureUtils.completedExceptionally(
                    new ResourceManagerException("Could not find registered job manager for job " + jobId + '.'));
        }
    }

    @Override
    public void notifySlotAvailable(final InstanceID instanceID,
                                    final SlotID slotId,
                                    final AllocationID allocationId) {

        final ResourceID resourceId = slotId.getResourceID();
        WorkerRegistration<WorkerType> registration = taskExecutors.get(resourceId);

        if (registration != null) {
            InstanceID registrationId = registration.getInstanceID();

            if (Objects.equals(registrationId, instanceID)) {
                slotManager.freeSlot(slotId, allocationId);
            } else {
                log.debug("Invalid registration id for slot available message. This indicates an" + " outdated request.");
            }
        } else {
            log.debug("Could not find registration for resource id {}. Discarding the slot available" + "message {}.",
                    resourceId.getStringWithMetadata(), slotId);
        }
    }

    /**
     * Cleanup application and shut down cluster.
     *
     * @param finalStatus of the Flink application
     * @param diagnostics diagnostics message for the Flink application or {@code null}
     */
    @Override
    public CompletableFuture<Acknowledge> deregisterApplication(final ApplicationStatus finalStatus,
                                                                @Nullable final String diagnostics) {
        log.info("Shut down cluster because application is in {}, diagnostics {}.", finalStatus, diagnostics);

        try {
            internalDeregisterApplication(finalStatus, diagnostics);
        } catch (ResourceManagerException e) {
            log.warn("Could not properly shutdown the application.", e);
        }

        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<Integer> getNumberOfRegisteredTaskManagers() {
        return CompletableFuture.completedFuture(taskExecutors.size());
    }

    @Override
    public CompletableFuture<Collection<TaskManagerInfo>> requestTaskManagerInfo(Time timeout) {

        final ArrayList<TaskManagerInfo> taskManagerInfos = new ArrayList<>(taskExecutors.size());

        for (Map.Entry<ResourceID, WorkerRegistration<WorkerType>> taskExecutorEntry : taskExecutors.entrySet()) {
            final ResourceID resourceId = taskExecutorEntry.getKey();
            final WorkerRegistration<WorkerType> taskExecutor = taskExecutorEntry.getValue();

            taskManagerInfos.add(
                    new TaskManagerInfo(resourceId, taskExecutor.getTaskExecutorGateway().getAddress(), taskExecutor.getDataPort(),
                            taskExecutor.getJmxPort(), taskManagerHeartbeatManager.getLastHeartbeatFrom(resourceId),
                            slotManager.getNumberRegisteredSlotsOf(taskExecutor.getInstanceID()),
                            slotManager.getNumberFreeSlotsOf(taskExecutor.getInstanceID()),
                            slotManager.getRegisteredResourceOf(taskExecutor.getInstanceID()),
                            slotManager.getFreeResourceOf(taskExecutor.getInstanceID()), taskExecutor.getHardwareDescription(),
                            taskExecutor.getMemoryConfiguration()));
        }

        return CompletableFuture.completedFuture(taskManagerInfos);
    }

    @Override
    public CompletableFuture<TaskManagerInfoWithSlots> requestTaskManagerDetailsInfo(ResourceID resourceId,
                                                                                     Time timeout) {

        final WorkerRegistration<WorkerType> taskExecutor = taskExecutors.get(resourceId);

        if (taskExecutor == null) {
            return FutureUtils.completedExceptionally(new UnknownTaskExecutorException(resourceId));
        } else {
            final InstanceID instanceId = taskExecutor.getInstanceID();
            final TaskManagerInfoWithSlots taskManagerInfoWithSlots = new TaskManagerInfoWithSlots(
                    new TaskManagerInfo(resourceId, taskExecutor.getTaskExecutorGateway().getAddress(), taskExecutor.getDataPort(),
                            taskExecutor.getJmxPort(), taskManagerHeartbeatManager.getLastHeartbeatFrom(resourceId),
                            slotManager.getNumberRegisteredSlotsOf(instanceId), slotManager.getNumberFreeSlotsOf(instanceId),
                            slotManager.getRegisteredResourceOf(instanceId), slotManager.getFreeResourceOf(instanceId),
                            taskExecutor.getHardwareDescription(), taskExecutor.getMemoryConfiguration()),
                    slotManager.getAllocatedSlotsOf(instanceId));

            return CompletableFuture.completedFuture(taskManagerInfoWithSlots);
        }
    }

    @Override
    public CompletableFuture<ResourceOverview> requestResourceOverview(Time timeout) {
        final int numberSlots = slotManager.getNumberRegisteredSlots();
        final int numberFreeSlots = slotManager.getNumberFreeSlots();
        final ResourceProfile totalResource = slotManager.getRegisteredResource();
        final ResourceProfile freeResource = slotManager.getFreeResource();

        return CompletableFuture.completedFuture(
                new ResourceOverview(taskExecutors.size(), numberSlots, numberFreeSlots, totalResource, freeResource));
    }

    @Override
    public CompletableFuture<Collection<Tuple2<ResourceID, String>>> requestTaskManagerMetricQueryServiceAddresses(Time timeout) {
        final ArrayList<CompletableFuture<Optional<Tuple2<ResourceID, String>>>> metricQueryServiceAddressFutures = new ArrayList<>(
                taskExecutors.size());

        for (Map.Entry<ResourceID, WorkerRegistration<WorkerType>> workerRegistrationEntry : taskExecutors.entrySet()) {
            final ResourceID tmResourceId = workerRegistrationEntry.getKey();
            final WorkerRegistration<WorkerType> workerRegistration = workerRegistrationEntry.getValue();
            final TaskExecutorGateway taskExecutorGateway = workerRegistration.getTaskExecutorGateway();

            final CompletableFuture<Optional<Tuple2<ResourceID, String>>> metricQueryServiceAddressFuture = taskExecutorGateway.requestMetricQueryServiceAddress(
                    timeout).thenApply(o -> o.toOptional().map(address -> Tuple2.of(tmResourceId, address)));

            metricQueryServiceAddressFutures.add(metricQueryServiceAddressFuture);
        }

        return FutureUtils.combineAll(metricQueryServiceAddressFutures).thenApply(
                collection -> collection.stream().filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<TransientBlobKey> requestTaskManagerFileUploadByType(ResourceID taskManagerId,
                                                                                  FileType fileType,
                                                                                  Time timeout) {
        log.debug("Request {} file upload from TaskExecutor {}.", fileType, taskManagerId.getStringWithMetadata());

        final WorkerRegistration<WorkerType> taskExecutor = taskExecutors.get(taskManagerId);

        if (taskExecutor == null) {
            log.debug("Request upload of file {} from unregistered TaskExecutor {}.", fileType,
                    taskManagerId.getStringWithMetadata());
            return FutureUtils.completedExceptionally(new UnknownTaskExecutorException(taskManagerId));
        } else {
            return taskExecutor.getTaskExecutorGateway().requestFileUploadByType(fileType, timeout);
        }
    }

    @Override
    public CompletableFuture<TransientBlobKey> requestTaskManagerFileUploadByName(ResourceID taskManagerId,
                                                                                  String fileName,
                                                                                  Time timeout) {
        log.debug("Request upload of file {} from TaskExecutor {}.", fileName, taskManagerId.getStringWithMetadata());

        final WorkerRegistration<WorkerType> taskExecutor = taskExecutors.get(taskManagerId);

        if (taskExecutor == null) {
            log.debug("Request upload of file {} from unregistered TaskExecutor {}.", fileName,
                    taskManagerId.getStringWithMetadata());
            return FutureUtils.completedExceptionally(new UnknownTaskExecutorException(taskManagerId));
        } else {
            return taskExecutor.getTaskExecutorGateway().requestFileUploadByName(fileName, timeout);
        }
    }

    @Override
    public CompletableFuture<Collection<LogInfo>> requestTaskManagerLogList(ResourceID taskManagerId,
                                                                            Time timeout) {
        final WorkerRegistration<WorkerType> taskExecutor = taskExecutors.get(taskManagerId);
        if (taskExecutor == null) {
            log.debug("Requested log list from unregistered TaskExecutor {}.", taskManagerId.getStringWithMetadata());
            return FutureUtils.completedExceptionally(new UnknownTaskExecutorException(taskManagerId));
        } else {
            return taskExecutor.getTaskExecutorGateway().requestLogList(timeout);
        }
    }

    @Override
    public CompletableFuture<Void> releaseClusterPartitions(IntermediateDataSetID dataSetId) {
        return clusterPartitionTracker.releaseClusterPartitions(dataSetId);
    }

    @Override
    public CompletableFuture<Map<IntermediateDataSetID, DataSetMetaInfo>> listDataSets() {
        return CompletableFuture.completedFuture(clusterPartitionTracker.listDataSets());
    }

    @Override
    public CompletableFuture<ThreadDumpInfo> requestThreadDump(ResourceID taskManagerId,
                                                               Time timeout) {
        final WorkerRegistration<WorkerType> taskExecutor = taskExecutors.get(taskManagerId);

        if (taskExecutor == null) {
            log.debug("Requested thread dump from unregistered TaskExecutor {}.", taskManagerId.getStringWithMetadata());
            return FutureUtils.completedExceptionally(new UnknownTaskExecutorException(taskManagerId));
        } else {
            return taskExecutor.getTaskExecutorGateway().requestThreadDump(timeout);
        }
    }

    @Override
    public CompletableFuture<TaskExecutorThreadInfoGateway> requestTaskExecutorThreadInfoGateway(ResourceID taskManagerId,
                                                                                                 Time timeout) {

        final WorkerRegistration<WorkerType> taskExecutor = taskExecutors.get(taskManagerId);

        if (taskExecutor == null) {
            return FutureUtils.completedExceptionally(new UnknownTaskExecutorException(taskManagerId));
        } else {
            return CompletableFuture.completedFuture(taskExecutor.getTaskExecutorGateway());
        }
    }

    // ------------------------------------------------------------------------
    //  Internal methods
    // ------------------------------------------------------------------------

    /**
     * Registers a new JobMaster.
     *
     * @param jobMasterGateway to communicate with the registering JobMaster
     * @param jobId of the job for which the JobMaster is responsible
     * @param jobManagerAddress address of the JobMaster
     * @param jobManagerResourceId ResourceID of the JobMaster
     *
     * @return RegistrationResponse
     */
    private RegistrationResponse registerJobMasterInternal(final JobMasterGateway jobMasterGateway,
                                                           JobID jobId,
                                                           String jobManagerAddress,
                                                           ResourceID jobManagerResourceId) {
        // TODO_MA 马中华 注释： 判断是否注册过
        if (jobManagerRegistrations.containsKey(jobId)) {

            JobManagerRegistration oldJobManagerRegistration = jobManagerRegistrations.get(jobId);

            if (Objects.equals(oldJobManagerRegistration.getJobMasterId(), jobMasterGateway.getFencingToken())) {
                // same registration
                log.debug("Job manager {}@{} was already registered.", jobMasterGateway.getFencingToken(), jobManagerAddress);
            } else {
                // tell old job manager that he is no longer the job leader
                closeJobManagerConnection(oldJobManagerRegistration.getJobID(), ResourceRequirementHandling.RETAIN,
                        new Exception("New job leader for job " + jobId + " found."));

                JobManagerRegistration jobManagerRegistration = new JobManagerRegistration(jobId, jobManagerResourceId,
                        jobMasterGateway);
                jobManagerRegistrations.put(jobId, jobManagerRegistration);
                jmResourceIdRegistrations.put(jobManagerResourceId, jobManagerRegistration);
            }
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 完成注册
         *  一个通用的注册机制； 生成一个 注册信息对象，然后登记到一个内存数据结构（Map，）
         */
        else {
            // new registration for the job
            JobManagerRegistration jobManagerRegistration = new JobManagerRegistration(jobId, jobManagerResourceId,
                    jobMasterGateway);

            // TODO_MA 马中华 注释： 插入一个map
            jobManagerRegistrations.put(jobId, jobManagerRegistration);

            jmResourceIdRegistrations.put(jobManagerResourceId, jobManagerRegistration);
        }

        log.info("Registered job manager {}@{} for job {}.", jobMasterGateway.getFencingToken(), jobManagerAddress, jobId);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 上述代码完成了 JObMaster 的注册，接下来，开始： 维持心跳
         *  此时，ResourceManager 就把当前注册成功的 这个 JobMaster 抽象成一个 HeartbeatTarget 维护在一个数据集合中
         *  ResourceManager 中其实有两个心跳定时任务：
         *  1、维护和 TaskExecutor 之间的
         *  2、维护和 JobMaster 之间的
         */
        jobManagerHeartbeatManager.monitorTarget(jobManagerResourceId, new JobMasterHeartbeatTarget(jobMasterGateway));

        // TODO_MA 马中华 注释：
        return new JobMasterRegistrationSuccess(getFencingToken(), resourceId);
    }

    /**
     * Registers a new TaskExecutor.
     *
     * @param taskExecutorRegistration task executor registration parameters
     *
     * @return RegistrationResponse
     */
    private RegistrationResponse registerTaskExecutorInternal(TaskExecutorGateway taskExecutorGateway,
                                                              TaskExecutorRegistration taskExecutorRegistration) {

        ResourceID taskExecutorResourceId = taskExecutorRegistration.getResourceId();

        // TODO_MA 马中华 注释： 先获取 注册集合中，是否有这个 从节点的注册对象
        // TODO_MA 马中华 注释： 如果有，证明之前注册过，此时的这次注册是重复注册
        WorkerRegistration<WorkerType> oldRegistration = taskExecutors.remove(taskExecutorResourceId);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 如果 oldRegistration 不等于 null，之前注册过，移除
         */
        if (oldRegistration != null) {
            // TODO :: suggest old taskExecutor to stop itself
            log.debug("Replacing old registration of TaskExecutor {}.", taskExecutorResourceId.getStringWithMetadata());

            // remove old task manager registration from slot manager
            // TODO_MA 马中华 注释： 注销
            slotManager.unregisterTaskManager(oldRegistration.getInstanceID(), new ResourceManagerException(
                    String.format("TaskExecutor %s re-connected to the ResourceManager.",
                            taskExecutorResourceId.getStringWithMetadata())));
        }else{

        }

        // TODO_MA 马中华 注释： 接下来无论如何，对方都是没有注册的

        final WorkerType newWorker = workerStarted(taskExecutorResourceId);

        String taskExecutorAddress = taskExecutorRegistration.getTaskExecutorAddress();
        if (newWorker == null) {
            log.warn("Discard registration from TaskExecutor {} at ({}) because the framework did " + "not recognize it",
                    taskExecutorResourceId.getStringWithMetadata(), taskExecutorAddress);
            return new TaskExecutorRegistrationRejection("The ResourceManager does not recognize this TaskExecutor.");
        } else {

            // TODO_MA 马中华 注释： 先生成一个注册信息对象
            // TODO_MA 马中华 注释： 关于一个从节点的注册信息的抽象，有两个：
            // TODO_MA 马中华 注释： 1、从节点： TaskExecutorRegistion
            // TODO_MA 马中华 注释： 2、主节点： WorkerRegistration
            // TODO_MA 马中华 注释： 他们的作用是一样的，就是为了避免歧义
            // TODO_MA 马中华 注释： 中文： 打    打水，打饭，打架，
            WorkerRegistration<WorkerType> registration = new WorkerRegistration<>(taskExecutorGateway, newWorker,
                    taskExecutorRegistration.getDataPort(), taskExecutorRegistration.getJmxPort(),
                    taskExecutorRegistration.getHardwareDescription(), taskExecutorRegistration.getMemoryConfiguration(),
                    taskExecutorRegistration.getTotalResourceProfile(), taskExecutorRegistration.getDefaultSlotResourceProfile());

            log.info("Registering TaskManager with ResourceID {} ({}) at ResourceManager",
                    taskExecutorResourceId.getStringWithMetadata(), taskExecutorAddress);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 完成注册！
             *  taskExecutors 内存数据结构： Map
             */
            taskExecutors.put(taskExecutorResourceId, registration);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 既然刚才来注册的这个 从节点，已经注册成功了，那么这个从节点，就作为主节点的一个心跳目标对象
             *  所以：将刚才这个过来注册的从节点，包装生成一个 HeartbeatTarget 对象，放入到心跳目标对象集合中
             *  心跳机制； 每隔一段时间，遍历这个心跳目标对象，给心跳目标发送心跳请求
             */
            taskManagerHeartbeatManager.monitorTarget(taskExecutorResourceId, new TaskExecutorHeartbeatTarget(taskExecutorGateway));

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 构造一个注册成功的响应结果，返回给从节点
             */
            return new TaskExecutorRegistrationSuccess(registration.getInstanceID(), resourceId, clusterInformation);
        }
    }

    protected void registerMetrics() {
        resourceManagerMetricGroup.gauge(MetricNames.NUM_REGISTERED_TASK_MANAGERS, () -> (long) taskExecutors.size());
    }

    private void clearStateInternal() {
        jobManagerRegistrations.clear();
        jmResourceIdRegistrations.clear();
        taskExecutors.clear();

        try {
            jobLeaderIdService.clear();
        } catch (Exception e) {
            onFatalError(new ResourceManagerException("Could not properly clear the job leader id service.", e));
        }
    }

    /**
     * This method should be called by the framework once it detects that a currently registered job
     * manager has failed.
     *
     * @param jobId identifying the job whose leader shall be disconnected.
     * @param resourceRequirementHandling indicating how existing resource requirements for the
     *         corresponding job should be handled
     * @param cause The exception which cause the JobManager failed.
     */
    protected void closeJobManagerConnection(JobID jobId,
                                             ResourceRequirementHandling resourceRequirementHandling,
                                             Exception cause) {

        // TODO_MA 马中华 注释： 首先，从注册集合中移除
        JobManagerRegistration jobManagerRegistration = jobManagerRegistrations.remove(jobId);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        if (jobManagerRegistration != null) {
            final ResourceID jobManagerResourceId = jobManagerRegistration.getJobManagerResourceID();
            final JobMasterGateway jobMasterGateway = jobManagerRegistration.getJobManagerGateway();
            final JobMasterId jobMasterId = jobManagerRegistration.getJobMasterId();

            log.info("Disconnect job manager {}@{} for job {} from the resource manager.", jobMasterId,
                    jobMasterGateway.getAddress(), jobId);

            // TODO_MA 马中华 注释： 注销心跳
            jobManagerHeartbeatManager.unmonitorTarget(jobManagerResourceId);

            // TODO_MA 马中华 注释： 取消 JobMasterGateways 注册
            jmResourceIdRegistrations.remove(jobManagerResourceId);

            if (resourceRequirementHandling == ResourceRequirementHandling.CLEAR) {
                slotManager.clearResourceRequirements(jobId);
            }

            // tell the job manager about the disconnect
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 断开链接
             */
            jobMasterGateway.disconnectResourceManager(getFencingToken(), cause);
        } else {
            log.debug("There was no registered job manager for job {}.", jobId);
        }
    }

    /**
     * This method should be called by the framework once it detects that a currently registered
     * task executor has failed.
     *
     * @param resourceID Id of the TaskManager that has failed.
     * @param cause The exception which cause the TaskManager failed.
     */
    protected void closeTaskManagerConnection(final ResourceID resourceID,
                                              final Exception cause) {
        taskManagerHeartbeatManager.unmonitorTarget(resourceID);

        WorkerRegistration<WorkerType> workerRegistration = taskExecutors.remove(resourceID);

        if (workerRegistration != null) {
            log.info("Closing TaskExecutor connection {} because: {}", resourceID.getStringWithMetadata(), cause.getMessage());

            // TODO :: suggest failed task executor to stop itself
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            slotManager.unregisterTaskManager(workerRegistration.getInstanceID(), cause);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            clusterPartitionTracker.processTaskExecutorShutdown(resourceID);

            workerRegistration.getTaskExecutorGateway().disconnectResourceManager(cause);
        } else {
            log.debug("No open TaskExecutor connection {}. Ignoring close TaskExecutor connection. Closing reason was: {}",
                    resourceID.getStringWithMetadata(), cause.getMessage());
        }
    }

    protected void removeJob(JobID jobId,
                             Exception cause) {
        try {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 取消注册
             */
            jobLeaderIdService.removeJob(jobId);
        } catch (Exception e) {
            log.warn("Could not properly remove the job {} from the job leader id service.", jobId, e);
        }

        if (jobManagerRegistrations.containsKey(jobId)) {
            closeJobManagerConnection(jobId, ResourceRequirementHandling.CLEAR, cause);
        }
    }

    protected void jobLeaderLostLeadership(JobID jobId,
                                           JobMasterId oldJobMasterId) {
        if (jobManagerRegistrations.containsKey(jobId)) {
            JobManagerRegistration jobManagerRegistration = jobManagerRegistrations.get(jobId);

            if (Objects.equals(jobManagerRegistration.getJobMasterId(), oldJobMasterId)) {

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 关闭链接
                 */
                closeJobManagerConnection(jobId, ResourceRequirementHandling.RETAIN, new Exception("Job leader lost leadership."));
            } else {
                log.debug("Discarding job leader lost leadership, because a new job leader was found for job {}. ", jobId);
            }
        } else {
            log.debug("Discard job leader lost leadership for outdated leader {} for job {}.", oldJobMasterId, jobId);
        }
    }

    protected void releaseResource(InstanceID instanceId,
                                   Exception cause) {
        WorkerType worker = null;

        // TODO: Improve performance by having an index on the instanceId
        for (Map.Entry<ResourceID, WorkerRegistration<WorkerType>> entry : taskExecutors.entrySet()) {
            if (entry.getValue().getInstanceID().equals(instanceId)) {
                worker = entry.getValue().getWorker();
                break;
            }
        }

        if (worker != null) {
            if (stopWorker(worker)) {
                closeTaskManagerConnection(worker.getResourceID(), cause);
            } else {
                log.debug("Worker {} could not be stopped.", worker.getResourceID().getStringWithMetadata());
            }
        } else {
            // unregister in order to clean up potential left over state
            slotManager.unregisterTaskManager(instanceId, cause);
        }
    }

    private enum ResourceRequirementHandling {
        RETAIN,
        CLEAR
    }

    // ------------------------------------------------------------------------
    //  Error Handling
    // ------------------------------------------------------------------------

    /**
     * Notifies the ResourceManager that a fatal error has occurred and it cannot proceed.
     *
     * @param t The exception describing the fatal error
     */
    protected void onFatalError(Throwable t) {
        try {
            log.error("Fatal error occurred in ResourceManager.", t);
        } catch (Throwable ignored) {
        }

        // The fatal error handler implementation should make sure that this call is non-blocking
        // TODO_MA 马中华 注释： fatalErrorHandler = ClusterEntryPoint
        fatalErrorHandler.onFatalError(t);
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 执行这个代码的是 ResourceManager
     *  相当于 JobMaster 和 TaskExecutor 都地位更高
     */
    private void startHeartbeatServices() {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        taskManagerHeartbeatManager = heartbeatServices.createHeartbeatManagerSender(resourceId, new TaskManagerHeartbeatListener(),
                getMainThreadExecutor(), log);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        jobManagerHeartbeatManager = heartbeatServices.createHeartbeatManagerSender(resourceId, new JobManagerHeartbeatListener(),
                getMainThreadExecutor(), log);
    }

    private void stopHeartbeatServices() {
        taskManagerHeartbeatManager.stop();
        jobManagerHeartbeatManager.stop();
    }

    // ------------------------------------------------------------------------
    //  Framework specific behavior
    // ------------------------------------------------------------------------

    /**
     * Initializes the framework specific components.
     *
     * @throws ResourceManagerException which occurs during initialization and causes the resource
     *         manager to fail.
     */
    protected abstract void initialize() throws ResourceManagerException;

    /**
     * Terminates the framework specific components.
     *
     * @throws Exception which occurs during termination.
     */
    protected abstract void terminate() throws Exception;

    /**
     * The framework specific code to deregister the application. This should report the
     * application's final status and shut down the resource manager cleanly.
     *
     * <p>This method also needs to make sure all pending containers that are not registered yet are
     * returned.
     *
     * @param finalStatus The application status to report.
     * @param optionalDiagnostics A diagnostics message or {@code null}.
     *
     * @throws ResourceManagerException if the application could not be shut down.
     */
    protected abstract void internalDeregisterApplication(ApplicationStatus finalStatus,
                                                          @Nullable String optionalDiagnostics) throws ResourceManagerException;

    /**
     * Allocates a resource using the worker resource specification.
     *
     * @param workerResourceSpec workerResourceSpec specifies the size of the to be allocated
     *         resource
     *
     * @return whether the resource can be allocated
     */
    @VisibleForTesting
    public abstract boolean startNewWorker(WorkerResourceSpec workerResourceSpec);

    /**
     * Callback when a worker was started.
     *
     * @param resourceID The worker resource id
     */
    protected abstract WorkerType workerStarted(ResourceID resourceID);

    /**
     * Stops the given worker.
     *
     * @param worker The worker.
     *
     * @return True if the worker was stopped, otherwise false
     */
    public abstract boolean stopWorker(WorkerType worker);

    /**
     * Set {@link SlotManager} whether to fail unfulfillable slot requests.
     *
     * @param failUnfulfillableRequest whether to fail unfulfillable requests
     */
    protected void setFailUnfulfillableRequest(boolean failUnfulfillableRequest) {
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        slotManager.setFailUnfulfillableRequest(failUnfulfillableRequest);
    }

    // ------------------------------------------------------------------------
    //  Static utility classes
    // ------------------------------------------------------------------------

    private static final class JobMasterHeartbeatTarget implements HeartbeatTarget<Void> {
        private final JobMasterGateway jobMasterGateway;

        private JobMasterHeartbeatTarget(JobMasterGateway jobMasterGateway) {
            this.jobMasterGateway = jobMasterGateway;
        }

        @Override
        public CompletableFuture<Void> receiveHeartbeat(ResourceID resourceID,
                                                        Void payload) {
            // the ResourceManager will always send heartbeat requests to the JobManager
            return FutureUtils.unsupportedOperationFuture();
        }

        @Override
        public CompletableFuture<Void> requestHeartbeat(ResourceID resourceID,
                                                        Void payload) {
            return jobMasterGateway.heartbeatFromResourceManager(resourceID);
        }
    }

    private static final class TaskExecutorHeartbeatTarget implements HeartbeatTarget<Void> {

        private final TaskExecutorGateway taskExecutorGateway;

        private TaskExecutorHeartbeatTarget(TaskExecutorGateway taskExecutorGateway) {
            this.taskExecutorGateway = taskExecutorGateway;
        }

        @Override
        public CompletableFuture<Void> receiveHeartbeat(ResourceID resourceID,
                                                        Void payload) {
            // the ResourceManager will always send heartbeat requests to the TaskManager
            return FutureUtils.unsupportedOperationFuture();
        }

        @Override
        public CompletableFuture<Void> requestHeartbeat(ResourceID resourceID,
                                                        Void payload) {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 这个代码是主节点执行的
             */
            return taskExecutorGateway.heartbeatFromResourceManager(resourceID);
        }
    }

    private class ResourceActionsImpl implements ResourceActions {

        @Override
        public void releaseResource(InstanceID instanceId,
                                    Exception cause) {
            validateRunsInMainThread();
            ResourceManager.this.releaseResource(instanceId, cause);
        }

        @Override
        public boolean allocateResource(WorkerResourceSpec workerResourceSpec) {
            validateRunsInMainThread();
            return startNewWorker(workerResourceSpec);
        }

        @Override
        public void notifyAllocationFailure(JobID jobId,
                                            AllocationID allocationId,
                                            Exception cause) {
            validateRunsInMainThread();

            JobManagerRegistration jobManagerRegistration = jobManagerRegistrations.get(jobId);
            if (jobManagerRegistration != null) {
                jobManagerRegistration.getJobManagerGateway().notifyAllocationFailure(allocationId, cause);
            }
        }

        @Override
        public void notifyNotEnoughResourcesAvailable(JobID jobId,
                                                      Collection<ResourceRequirement> acquiredResources) {
            validateRunsInMainThread();

            JobManagerRegistration jobManagerRegistration = jobManagerRegistrations.get(jobId);
            if (jobManagerRegistration != null) {
                jobManagerRegistration.getJobManagerGateway().notifyNotEnoughResourcesAvailable(acquiredResources);
            }
        }
    }

    private class JobLeaderIdActionsImpl implements JobLeaderIdActions {

        // TODO_MA 马中华 注释： jobMaster 地址发生迁移了
        @Override
        public void jobLeaderLostLeadership(final JobID jobId,
                                            final JobMasterId oldJobMasterId) {
            runAsync(new Runnable() {
                @Override
                public void run() {
                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释：
                     */
                    ResourceManager.this.jobLeaderLostLeadership(jobId, oldJobMasterId);
                }
            });
        }

        // TODO_MA 马中华 注释： jobMaster 的启动超时了
        @Override
        public void notifyJobTimeout(final JobID jobId,
                                     final UUID timeoutId) {

            runAsync(new Runnable() {
                @Override
                public void run() {
                    if (jobLeaderIdService.isValidTimeout(jobId, timeoutId)) {

                        /*************************************************
                         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                         *  注释：
                         */
                        removeJob(jobId, new Exception("Job " + jobId + "was removed because of timeout"));
                    }
                }
            });
        }

        // TODO_MA 马中华 注释： 启动过程中发生了致命的异常了
        @Override
        public void handleError(Throwable error) {
            onFatalError(error);
        }
    }

    private class TaskManagerHeartbeatListener implements HeartbeatListener<TaskExecutorHeartbeatPayload, Void> {

        @Override
        public void notifyHeartbeatTimeout(final ResourceID resourceID) {
            final String message = String.format("The heartbeat of TaskManager with id %s timed out.",
                    resourceID.getStringWithMetadata());
            log.info(message);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            handleTaskManagerConnectionLoss(resourceID, new TimeoutException(message));
        }

        private void handleTaskManagerConnectionLoss(ResourceID resourceID,
                                                     Exception cause) {
            validateRunsInMainThread();

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            closeTaskManagerConnection(resourceID, cause);
        }

        @Override
        public void notifyTargetUnreachable(ResourceID resourceID) {
            final String message = String.format("TaskManager with id %s is no longer reachable.",
                    resourceID.getStringWithMetadata());
            log.info(message);

            handleTaskManagerConnectionLoss(resourceID, new ResourceManagerException(message));
        }

        @Override
        public void reportPayload(final ResourceID resourceID,
                                  final TaskExecutorHeartbeatPayload payload) {
            validateRunsInMainThread();
            final WorkerRegistration<WorkerType> workerRegistration = taskExecutors.get(resourceID);

            if (workerRegistration == null) {
                log.debug("Received slot report from TaskManager {} which is no longer registered.",
                        resourceID.getStringWithMetadata());
            } else {
                InstanceID instanceId = workerRegistration.getInstanceID();

                slotManager.reportSlotStatus(instanceId, payload.getSlotReport());
                clusterPartitionTracker.processTaskExecutorClusterPartitionReport(resourceID, payload.getClusterPartitionReport());
            }
        }

        @Override
        public Void retrievePayload(ResourceID resourceID) {
            return null;
        }
    }

    private class JobManagerHeartbeatListener implements HeartbeatListener<Void, Void> {

        @Override
        public void notifyHeartbeatTimeout(final ResourceID resourceID) {
            final String message = String.format("The heartbeat of JobManager with id %s timed out.",
                    resourceID.getStringWithMetadata());
            log.info(message);

            handleJobManagerConnectionLoss(resourceID, new TimeoutException(message));
        }

        private void handleJobManagerConnectionLoss(ResourceID resourceID,
                                                    Exception cause) {
            validateRunsInMainThread();
            if (jmResourceIdRegistrations.containsKey(resourceID)) {
                JobManagerRegistration jobManagerRegistration = jmResourceIdRegistrations.get(resourceID);

                if (jobManagerRegistration != null) {
                    closeJobManagerConnection(jobManagerRegistration.getJobID(), ResourceRequirementHandling.RETAIN, cause);
                }
            }
        }

        @Override
        public void notifyTargetUnreachable(ResourceID resourceID) {
            final String message = String.format("JobManager with id %s is no longer reachable.",
                    resourceID.getStringWithMetadata());
            log.info(message);

            handleJobManagerConnectionLoss(resourceID, new ResourceManagerException(message));
        }

        @Override
        public void reportPayload(ResourceID resourceID,
                                  Void payload) {
            // nothing to do since there is no payload
        }

        @Override
        public Void retrievePayload(ResourceID resourceID) {
            return null;
        }
    }

    // ------------------------------------------------------------------------
    //  Resource Management
    // ------------------------------------------------------------------------

    protected int getNumberRequiredTaskManagers() {
        return getRequiredResources().values().stream().reduce(0, Integer::sum);
    }

    protected Map<WorkerResourceSpec, Integer> getRequiredResources() {
        return slotManager.getRequiredResources();
    }
}
