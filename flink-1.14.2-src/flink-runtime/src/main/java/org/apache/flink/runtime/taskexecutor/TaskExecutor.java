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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.management.jmx.JMXService;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.blob.PermanentBlobCache;
import org.apache.flink.runtime.blob.TransientBlobCache;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.CheckpointType.PostCheckpointAction;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.heartbeat.HeartbeatListener;
import org.apache.flink.runtime.heartbeat.HeartbeatManager;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.HeartbeatTarget;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionInfo;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionTracker;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.jobmaster.AllocatedSlotInfo;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationRejection;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.ResourceManagerAddress;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.TaskThreadInfoResponse;
import org.apache.flink.runtime.messages.ThreadInfoSample;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.TaskNotRunningException;
import org.apache.flink.runtime.query.KvStateClientProxy;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.KvStateServer;
import org.apache.flink.runtime.registration.RegistrationConnectionListener;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.TaskExecutorRegistration;
import org.apache.flink.runtime.rest.messages.LogInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.ThreadDumpInfo;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcServiceUtils;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.state.TaskExecutorLocalStateStoresManager;
import org.apache.flink.runtime.state.TaskExecutorStateChangelogStoragesManager;
import org.apache.flink.runtime.state.TaskLocalStateStore;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.TaskStateManagerImpl;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.runtime.taskexecutor.exceptions.RegistrationTimeoutException;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotAllocationException;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotOccupiedException;
import org.apache.flink.runtime.taskexecutor.exceptions.TaskException;
import org.apache.flink.runtime.taskexecutor.exceptions.TaskManagerException;
import org.apache.flink.runtime.taskexecutor.exceptions.TaskSubmissionException;
import org.apache.flink.runtime.taskexecutor.rpc.RpcCheckpointResponder;
import org.apache.flink.runtime.taskexecutor.rpc.RpcGlobalAggregateManager;
import org.apache.flink.runtime.taskexecutor.rpc.RpcInputSplitProvider;
import org.apache.flink.runtime.taskexecutor.rpc.RpcKvStateRegistryListener;
import org.apache.flink.runtime.taskexecutor.rpc.RpcPartitionStateChecker;
import org.apache.flink.runtime.taskexecutor.rpc.RpcResultPartitionConsumableNotifier;
import org.apache.flink.runtime.taskexecutor.rpc.RpcTaskOperatorEventGateway;
import org.apache.flink.runtime.taskexecutor.slot.SlotActions;
import org.apache.flink.runtime.taskexecutor.slot.SlotNotActiveException;
import org.apache.flink.runtime.taskexecutor.slot.SlotNotFoundException;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlot;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;
import org.apache.flink.runtime.util.JvmUtils;
import org.apache.flink.runtime.webmonitor.threadinfo.ThreadInfoSamplesRequest;
import org.apache.flink.types.SerializableOptional;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OptionalConsumer;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava30.com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ThreadInfo;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * TaskExecutor implementation. The task executor is responsible for the execution of multiple
 * {@link Task}.
 * // TODO_MA 马中华 注释： TaskExecutor 集群的节点： 提供资源，接收Task执行。 猜想一下
 * // TODO_MA 马中华 注释： 1、管理 Slot 和 Task 的角色： TaskSlotTable
 * // TODO_MA 马中华 注释： 2、管理 Job 的角色： JobTable
 * // TODO_MA 马中华 注释： 3、心跳服务
 */
public class TaskExecutor extends RpcEndpoint implements TaskExecutorGateway {

    public static final String TASK_MANAGER_NAME = "taskmanager";

    /** The access to the leader election and retrieval services. */
    private final HighAvailabilityServices haServices;

    private final TaskManagerServices taskExecutorServices;

    /** The task manager configuration. */
    private final TaskManagerConfiguration taskManagerConfiguration;

    /** The fatal error handler to use in case of a fatal error. */
    private final FatalErrorHandler fatalErrorHandler;

    private final BlobCacheService blobCacheService;

    private final LibraryCacheManager libraryCacheManager;

    /** The address to metric query service on this Task Manager. */
    @Nullable
    private final String metricQueryServiceAddress;

    // --------- TaskManager services --------

    /** The connection information of this task manager. */
    private final UnresolvedTaskManagerLocation unresolvedTaskManagerLocation;

    private final TaskManagerMetricGroup taskManagerMetricGroup;

    /** The state manager for this task, providing state managers per slot. */
    private final TaskExecutorLocalStateStoresManager localStateStoresManager;

    /** The changelog manager for this task, providing changelog storage per job. */
    private final TaskExecutorStateChangelogStoragesManager changelogStoragesManager;

    /** Information provider for external resources. */
    private final ExternalResourceInfoProvider externalResourceInfoProvider;

    /** The network component in the task manager. */
    private final ShuffleEnvironment<?, ?> shuffleEnvironment;

    /** The kvState registration service in the task manager. */
    private final KvStateService kvStateService;

    private final Executor ioExecutor;

    // --------- task slot allocation table -----------

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 管理 slot 和 task 的
     */
    private final TaskSlotTable<Task> taskSlotTable;

    private final Map<JobID, UUID> currentSlotOfferPerJob = new HashMap<>();

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 管理 job 的: JobOrConnection
     */
    private final JobTable jobTable;

    private final JobLeaderService jobLeaderService;

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： LeaderRetrievalService 用来监控 ResourceManager 是谁
     */
    private final LeaderRetrievalService resourceManagerLeaderRetriever;

    // ------------------------------------------------------------------------

    private final HardwareDescription hardwareDescription;

    private final TaskExecutorMemoryConfiguration memoryConfiguration;

    private FileCache fileCache;

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：  两个心跳组件
     */
    /** The heartbeat manager for job manager in the task manager. */
    private final HeartbeatManager<AllocatedSlotReport, TaskExecutorToJobManagerHeartbeatPayload> jobManagerHeartbeatManager;

    /** The heartbeat manager for resource manager in the task manager. */
    private final HeartbeatManager<Void, TaskExecutorHeartbeatPayload> resourceManagerHeartbeatManager;

    private final TaskExecutorPartitionTracker partitionTracker;

    // --------- resource manager --------

    @Nullable
    private ResourceManagerAddress resourceManagerAddress;

    @Nullable
    private EstablishedResourceManagerConnection establishedResourceManagerConnection;

    @Nullable
    private TaskExecutorToResourceManagerConnection resourceManagerConnection;

    @Nullable
    private UUID currentRegistrationTimeoutId;

    private Map<JobID, Collection<CompletableFuture<ExecutionState>>> taskResultPartitionCleanupFuturesPerJob = new HashMap<>(8);

    private final ThreadInfoSampleService threadInfoSampleService;

    public TaskExecutor(RpcService rpcService,
                        TaskManagerConfiguration taskManagerConfiguration,
                        HighAvailabilityServices haServices,
                        TaskManagerServices taskExecutorServices,
                        ExternalResourceInfoProvider externalResourceInfoProvider,
                        HeartbeatServices heartbeatServices,
                        TaskManagerMetricGroup taskManagerMetricGroup,
                        @Nullable String metricQueryServiceAddress,
                        BlobCacheService blobCacheService,
                        FatalErrorHandler fatalErrorHandler,
                        TaskExecutorPartitionTracker partitionTracker) {

        super(rpcService, RpcServiceUtils.createRandomName(TASK_MANAGER_NAME));

        checkArgument(taskManagerConfiguration.getNumberSlots() > 0, "The number of slots has to be larger than 0.");

        this.taskManagerConfiguration = checkNotNull(taskManagerConfiguration);
        this.taskExecutorServices = checkNotNull(taskExecutorServices);
        this.haServices = checkNotNull(haServices);
        this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
        this.partitionTracker = partitionTracker;
        this.taskManagerMetricGroup = checkNotNull(taskManagerMetricGroup);
        this.blobCacheService = checkNotNull(blobCacheService);
        this.metricQueryServiceAddress = metricQueryServiceAddress;
        this.externalResourceInfoProvider = checkNotNull(externalResourceInfoProvider);

        this.libraryCacheManager = taskExecutorServices.getLibraryCacheManager();
        this.taskSlotTable = taskExecutorServices.getTaskSlotTable();
        this.jobTable = taskExecutorServices.getJobTable();
        this.jobLeaderService = taskExecutorServices.getJobLeaderService();
        this.unresolvedTaskManagerLocation = taskExecutorServices.getUnresolvedTaskManagerLocation();
        this.localStateStoresManager = taskExecutorServices.getTaskManagerStateStore();
        this.changelogStoragesManager = taskExecutorServices.getTaskManagerChangelogManager();
        this.shuffleEnvironment = taskExecutorServices.getShuffleEnvironment();
        this.kvStateService = taskExecutorServices.getKvStateService();
        this.ioExecutor = taskExecutorServices.getIOExecutor();

        // TODO_MA 马中华 注释： RM 的信息检索器
        this.resourceManagerLeaderRetriever = haServices.getResourceManagerLeaderRetriever();

        // TODO_MA 马中华 注释： 资源信息
        this.hardwareDescription = HardwareDescription.extractFromSystem(taskExecutorServices.getManagedMemorySize());
        this.memoryConfiguration = TaskExecutorMemoryConfiguration.create(taskManagerConfiguration.getConfiguration());

        this.resourceManagerAddress = null;
        this.resourceManagerConnection = null;
        this.currentRegistrationTimeoutId = null;

        final ResourceID resourceId = taskExecutorServices.getUnresolvedTaskManagerLocation().getResourceID();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 创建两个心跳服务
         */
        this.jobManagerHeartbeatManager = createJobManagerHeartbeatManager(heartbeatServices, resourceId);
        this.resourceManagerHeartbeatManager = createResourceManagerHeartbeatManager(heartbeatServices, resourceId);

        ExecutorThreadFactory sampleThreadFactory = new ExecutorThreadFactory.Builder().setPoolName("flink-thread-info-sampler")
                .build();
        ScheduledExecutorService sampleExecutor = Executors.newSingleThreadScheduledExecutor(sampleThreadFactory);
        this.threadInfoSampleService = new ThreadInfoSampleService(sampleExecutor);
    }

    private HeartbeatManager<Void, TaskExecutorHeartbeatPayload> createResourceManagerHeartbeatManager(
            HeartbeatServices heartbeatServices,
            ResourceID resourceId) {
        return heartbeatServices.createHeartbeatManager(resourceId, new ResourceManagerHeartbeatListener(), getMainThreadExecutor(),
                log);
    }

    private HeartbeatManager<AllocatedSlotReport, TaskExecutorToJobManagerHeartbeatPayload> createJobManagerHeartbeatManager(
            HeartbeatServices heartbeatServices,
            ResourceID resourceId) {
        return heartbeatServices.createHeartbeatManager(resourceId, new JobManagerHeartbeatListener(), getMainThreadExecutor(),
                log);
    }

    @Override
    public CompletableFuture<Boolean> canBeReleased() {
        return CompletableFuture.completedFuture(shuffleEnvironment.getPartitionsOccupyingLocalResources().isEmpty());
    }

    @Override
    public CompletableFuture<Collection<LogInfo>> requestLogList(Time timeout) {
        return CompletableFuture.supplyAsync(() -> {
            final String logDir = taskManagerConfiguration.getTaskManagerLogDir();
            if (logDir != null) {
                final File[] logFiles = new File(logDir).listFiles();

                if (logFiles == null) {
                    throw new CompletionException(
                            new FlinkException(String.format("There isn't a log file in TaskExecutor’s log dir %s.", logDir)));
                }

                return Arrays.stream(logFiles).filter(File::isFile).map(logFile -> new LogInfo(logFile.getName(), logFile.length()))
                        .collect(Collectors.toList());
            }
            return Collections.emptyList();
        }, ioExecutor);
    }

    // ------------------------------------------------------------------------
    //  Life cycle
    // ------------------------------------------------------------------------

    @Override
    public void onStart() throws Exception {
        try {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 启动 TaskExecutor 上的服务
             */
            startTaskExecutorServices();
        } catch (Throwable t) {
            final TaskManagerException exception = new TaskManagerException(
                    String.format("Could not start the TaskExecutor %s", getAddress()), t);
            onFatalError(exception);
            throw exception;
        }
        startRegistrationTimeout();
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 里面四件事，你可以理解成是 启动四个服务
     *  1、 监听 ResourceManager 的地址，建立连接，完成注册
     *  2、 taskSlotTable 是存在于 TaskExecutor中用来管理 slot 的组件， 启动过程中，启动两个定时服务
     *      TaskManager 的超时检测服务（废弃了）
     *      SlotRequest 的超时检测服务（我TaskExecutor提供slot给某个JobMaster资源使用者，结果在一段时间里面并没有用，则超时回收）
     *  3、 启动 jobLeaderService
     *  4、 创建一个 FileCache 文件缓存服务
     */
    private void startTaskExecutorServices() throws Exception {
        try {
            // start by connecting to the ResourceManager
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 从节点启动了一个 监听 主节点地址的 监听器
             *  当监听响应，则回调： ResourceManagerLeaderListener 的 notifyLeaderAddresss()
             *  LeaderRetriever 是一个逻辑概念，是一个检索组件。通过这个检索组件调用 start() 启动检索/监听
             *  当监听响应的时候，则回调： ResourceManagerLeaderListener 的 notifyLeaderAddresss()
             */
            resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());

            // tell the task slot table who's responsible for the task slot actions
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 启动一个用来检测slot超时未使用的定时服务
             */
            taskSlotTable.start(new SlotActionsImpl(), getMainThreadExecutor());

            // start the job leader service
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： jobLeaderService = 管理 Jobmaster
             *  就是管理当前 TaskExecutor 和 JobMaster 之间的链接
             */
            jobLeaderService.start(getAddress(), getRpcService(), haServices, new JobLeaderListenerImpl());

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 文件缓存服务
             *  blobCacheService = 文件服务（C/S架构：BlobServer  BlobClient）
             */
            fileCache = new FileCache(taskManagerConfiguration.getTmpDirectories(), blobCacheService.getPermanentBlobService());
        } catch (Exception e) {
            handleStartTaskExecutorServicesException(e);
        }
    }

    private void handleStartTaskExecutorServicesException(Exception e) throws Exception {
        try {
            stopTaskExecutorServices();
        } catch (Exception inner) {
            e.addSuppressed(inner);
        }

        throw e;
    }

    /** Called to shut down the TaskManager. The method closes all TaskManager services. */
    @Override
    public CompletableFuture<Void> onStop() {
        log.info("Stopping TaskExecutor {}.", getAddress());

        Throwable jobManagerDisconnectThrowable = null;

        FlinkException cause = new FlinkException("The TaskExecutor is shutting down.");

        closeResourceManagerConnection(cause);

        for (JobTable.Job job : jobTable.getJobs()) {
            try {
                closeJob(job, cause);
            } catch (Throwable t) {
                jobManagerDisconnectThrowable = ExceptionUtils.firstOrSuppressed(t, jobManagerDisconnectThrowable);
            }
        }

        changelogStoragesManager.shutdown();

        Preconditions.checkState(jobTable.isEmpty());

        final Throwable throwableBeforeTasksCompletion = jobManagerDisconnectThrowable;

        return FutureUtils.runAfterwards(taskSlotTable.closeAsync(), this::stopTaskExecutorServices)
                .handle((ignored, throwable) -> {
                    handleOnStopException(throwableBeforeTasksCompletion, throwable);
                    return null;
                });
    }

    private void handleOnStopException(Throwable throwableBeforeTasksCompletion,
                                       Throwable throwableAfterTasksCompletion) {
        final Throwable throwable;

        if (throwableBeforeTasksCompletion != null) {
            throwable = ExceptionUtils.firstOrSuppressed(throwableBeforeTasksCompletion, throwableAfterTasksCompletion);
        } else {
            throwable = throwableAfterTasksCompletion;
        }

        if (throwable != null) {
            throw new CompletionException(new FlinkException("Error while shutting the TaskExecutor down.", throwable));
        } else {
            log.info("Stopped TaskExecutor {}.", getAddress());
        }
    }

    private void stopTaskExecutorServices() throws Exception {
        Exception exception = null;

        try {
            threadInfoSampleService.close();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            jobLeaderService.stop();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            resourceManagerLeaderRetriever.stop();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            taskExecutorServices.shutDown();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            fileCache.shutdown();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        // it will call close() recursively from the parent to children
        taskManagerMetricGroup.close();

        ExceptionUtils.tryRethrowException(exception);
    }

    // ======================================================================
    //  RPC methods
    // ======================================================================

    @Override
    public CompletableFuture<TaskThreadInfoResponse> requestThreadInfoSamples(final ExecutionAttemptID taskExecutionAttemptId,
                                                                              final ThreadInfoSamplesRequest requestParams,
                                                                              final Time timeout) {

        final Task task = taskSlotTable.getTask(taskExecutionAttemptId);
        if (task == null) {
            return FutureUtils.completedExceptionally(new IllegalStateException(
                    String.format("Cannot sample task %s. " + "Task is not known to the task manager.", taskExecutionAttemptId)));
        }

        final CompletableFuture<List<ThreadInfoSample>> stackTracesFuture = threadInfoSampleService.requestThreadInfoSamples(
                SampleableTaskAdapter.fromTask(task), requestParams);

        return stackTracesFuture.thenApply(TaskThreadInfoResponse::new);
    }

    // ----------------------------------------------------------------------
    // Task lifecycle RPCs
    // ----------------------------------------------------------------------

    // TODO_MA 马中华 注释： 下一节课的入口！
    // TODO_MA 马中华 注释： 从节点 TaskExecutor 接收到 Task 开始初始化和启动执行
    @Override
    public CompletableFuture<Acknowledge> submitTask(TaskDeploymentDescriptor tdd,
                                                     JobMasterId jobMasterId,
                                                     Time timeout) {

        try {
            final JobID jobId = tdd.getJobId();
            final ExecutionAttemptID executionAttemptID = tdd.getExecutionAttemptId();

            // TODO_MA 马中华 注释： 获取 Connection = JobOrConnection
            final JobTable.Connection jobManagerConnection = jobTable.getConnection(jobId).orElseThrow(() -> {
                final String message = "Could not submit task because there is no JobManager " + "associated for the job " + jobId + '.';
                log.debug(message);
                return new TaskSubmissionException(message);
            });

            if (!Objects.equals(jobManagerConnection.getJobMasterId(), jobMasterId)) {
                final String message = "Rejecting the task submission because the job manager leader id " + jobMasterId + " does not match the expected job manager leader id " + jobManagerConnection.getJobMasterId() + '.';
                log.debug(message);
                throw new TaskSubmissionException(message);
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 第一件事： 首先标记 Slot 为 active
             *  JobID, TaskID  SlotID  AllocationID  ExecutionID
             *  1、 搞清这些 ID 之间的关系（SlotID 和 AllocationID 是一对多的关系，可以通过 AllocationID 拿到 SlotID）
             *  2、 传入 jobID 的原因，是为了标记该 slot 给那个 job 服务
             */
            if (!taskSlotTable.tryMarkSlotActive(jobId, tdd.getAllocationId())) {
                final String message = "No task slot allocated for job ID " + jobId + " and allocation ID " + tdd.getAllocationId() + '.';
                log.debug(message);
                throw new TaskSubmissionException(message);
            }

            // re-integrate offloaded data:
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 下载 JobInformation 和 TaskInformation 等信息文件
             *  1、TDD（部署 Task 的时候所需要的资料的 元数据）
             *  2、loadBigData（真正的去下载，这个 Task 启动运行时所需要的生产资料，jar包，依赖jar，配置文件等等）
             *      TaskInfomation
             *      JobInformation
             *   BlobService（BlobServer + BlobClient） 文件服务 / 大对象服务
             */
            try {
                tdd.loadBigData(blobCacheService.getPermanentBlobService());
            } catch (IOException | ClassNotFoundException e) {
                throw new TaskSubmissionException("Could not re-integrate offloaded TaskDeploymentDescriptor data.", e);
            }

            // deserialize the pre-serialized information
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 进行反序列化 构建 JobInformation 和 TaskInformation
             */
            final JobInformation jobInformation;
            final TaskInformation taskInformation;
            try {
                jobInformation = tdd.getSerializedJobInformation().deserializeValue(getClass().getClassLoader());
                taskInformation = tdd.getSerializedTaskInformation().deserializeValue(getClass().getClassLoader());
            } catch (IOException | ClassNotFoundException e) {
                throw new TaskSubmissionException("Could not deserialize the job or task information.", e);
            }

            if (!jobId.equals(jobInformation.getJobId())) {
                throw new TaskSubmissionException(
                        "Inconsistent job ID information inside TaskDeploymentDescriptor (" + tdd.getJobId() + " vs. " + jobInformation.getJobId() + ")");
            }

            TaskMetricGroup taskMetricGroup = taskManagerMetricGroup.addTaskForJob(jobInformation.getJobId(),
                    jobInformation.getJobName(), taskInformation.getJobVertexId(), tdd.getExecutionAttemptId(),
                    taskInformation.getTaskName(), tdd.getSubtaskIndex(), tdd.getAttemptNumber());

            InputSplitProvider inputSplitProvider = new RpcInputSplitProvider(jobManagerConnection.getJobManagerGateway(),
                    taskInformation.getJobVertexId(), tdd.getExecutionAttemptId(), taskManagerConfiguration.getRpcTimeout());

            final TaskOperatorEventGateway taskOperatorEventGateway = new RpcTaskOperatorEventGateway(
                    jobManagerConnection.getJobManagerGateway(), executionAttemptID,
                    (t) -> runAsync(() -> failTask(executionAttemptID, t)));

            TaskManagerActions taskManagerActions = jobManagerConnection.getTaskManagerActions();
            CheckpointResponder checkpointResponder = jobManagerConnection.getCheckpointResponder();
            GlobalAggregateManager aggregateManager = jobManagerConnection.getGlobalAggregateManager();

            LibraryCacheManager.ClassLoaderHandle classLoaderHandle = jobManagerConnection.getClassLoaderHandle();
            ResultPartitionConsumableNotifier resultPartitionConsumableNotifier = jobManagerConnection.getResultPartitionConsumableNotifier();
            PartitionProducerStateChecker partitionStateChecker = jobManagerConnection.getPartitionStateChecker();

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： TaskLocalStateStoreImpl
             */
            final TaskLocalStateStore localStateStore = localStateStoresManager.localStateStoreForSubtask(jobId,
                    tdd.getAllocationId(), taskInformation.getJobVertexId(), tdd.getSubtaskIndex());

            // TODO: Pass config value from user program and do overriding here.
            final StateChangelogStorage<?> changelogStorage;
            try {
                changelogStorage = changelogStoragesManager.stateChangelogStorageForJob(jobId,
                        taskManagerConfiguration.getConfiguration());
            } catch (IOException e) {
                throw new TaskSubmissionException(e);
            }

            final JobManagerTaskRestore taskRestore = tdd.getTaskRestore();

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            final TaskStateManager taskStateManager = new TaskStateManagerImpl(jobId, tdd.getExecutionAttemptId(), localStateStore,
                    changelogStorage, taskRestore, checkpointResponder);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： MemoryManager
             */
            MemoryManager memoryManager;
            try {
                memoryManager = taskSlotTable.getTaskMemoryManager(tdd.getAllocationId());
            } catch (SlotNotFoundException e) {
                throw new TaskSubmissionException("Could not submit task.", e);
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 4 件重要的事情
             *  1、创建 TaskInfo， Task 的元数据
             *  2、创建 ResultPartition 和 ResultSubPartition， 输出组件
             *  3、创建 InputGate 和 InputChannel， 输入组件
             *  4、创建 ExecutingThread， 负责执行 Task
             *  -
             *  创建的是一个 逻辑 Slot， FLink 任何应用程序中，任何一个 逻辑执行单元，都叫做 Task
             *  包含了，启动这个 Task 所需要的各种生产资料， 启动任何物理 Task 的一个统一入口
             *  不管是下述的那种 物理Task， 最终，通过一个线程来启动的， 逻辑 Task 对象的内部，有一个成员变量： 工作线程
             *  物理Task:  StreamTask， BatchTask， OneInputStreamTask , TwoInputStreamTask
             *  -
             *  Task 的初始化： Task内部的线程，开始执行了。
             *  -
             *  该 Task 部署的时候，必须要搞定：该 Task 的输入和输出相关工作组件
             */
            Task task = new Task(jobInformation, taskInformation,
                    tdd.getExecutionAttemptId(),
                    tdd.getAllocationId(),
                    tdd.getSubtaskIndex(), tdd.getAttemptNumber(),
                    // TODO_MA 马中华 注释： DD = DeploymentDescriptor
                    // TODO_MA 马中华 注释： 部署该 Task 对应的 ResultPartition 和 ResultSubPartition 的 DD 对象
                    tdd.getProducedPartitions(),
                    // TODO_MA 马中华 注释： 部署该 Task 对应的 InputGate 和 InputChannel 的 DD 对象
                    tdd.getInputGates(),
                    memoryManager,
                    // TODO_MA 马中华 注释： 这一堆参数，都是从节点提供的
                    // TODO_MA 马中华 注释： taskExecutorServices = TaskManagerServices
                    taskExecutorServices.getIOManager(), taskExecutorServices.getShuffleEnvironment(),
                    taskExecutorServices.getKvStateService(), taskExecutorServices.getBroadcastVariableManager(),
                    taskExecutorServices.getTaskEventDispatcher(),
                    // TODO_MA 马中华 注释：
                    externalResourceInfoProvider, taskStateManager,
                    taskManagerActions, inputSplitProvider, checkpointResponder, taskOperatorEventGateway, aggregateManager,
                    classLoaderHandle, fileCache, taskManagerConfiguration, taskMetricGroup, resultPartitionConsumableNotifier,
                    partitionStateChecker, getRpcService().getScheduledExecutor());

            taskMetricGroup.gauge(MetricNames.IS_BACK_PRESSURED, task::isBackPressured);

            log.info("Received task {} ({}), deploy into slot with allocation id {}.", task.getTaskInfo().getTaskNameWithSubtasks(),
                    tdd.getExecutionAttemptId(), tdd.getAllocationId());

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： Task 登记注册， 完成 Task 和 Slot 的映射
             */
            boolean taskAdded;
            try {
                taskAdded = taskSlotTable.addTask(task);
            } catch (SlotNotFoundException | SlotNotActiveException e) {
                throw new TaskSubmissionException("Could not submit task.", e);
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 通过一个线程来启动 Task
             */
            if (taskAdded) {
                task.startTaskThread();

                setupResultPartitionBookkeeping(tdd.getJobId(), tdd.getProducedPartitions(), task.getTerminationFuture());
                return CompletableFuture.completedFuture(Acknowledge.get());
            } else {
                final String message = "TaskManager already contains a task for id " + task.getExecutionId() + '.';

                log.debug(message);
                throw new TaskSubmissionException(message);
            }
        } catch (TaskSubmissionException e) {
            return FutureUtils.completedExceptionally(e);
        }
    }

    private void setupResultPartitionBookkeeping(JobID jobId,
                                                 Collection<ResultPartitionDeploymentDescriptor> producedResultPartitions,
                                                 CompletableFuture<ExecutionState> terminationFuture) {

        final Set<ResultPartitionID> partitionsRequiringRelease = filterPartitionsRequiringRelease(producedResultPartitions)
                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释：
                 */.peek(rpdd -> partitionTracker.startTrackingPartition(jobId, TaskExecutorPartitionInfo.from(rpdd)))
                .map(ResultPartitionDeploymentDescriptor::getShuffleDescriptor).map(ShuffleDescriptor::getResultPartitionID)
                .collect(Collectors.toSet());

        final CompletableFuture<ExecutionState> taskTerminationWithResourceCleanupFuture = terminationFuture.thenApplyAsync(
                executionState -> {
                    if (executionState != ExecutionState.FINISHED) {
                        partitionTracker.stopTrackingPartitions(partitionsRequiringRelease);
                    }
                    return executionState;
                }, getMainThreadExecutor());

        taskResultPartitionCleanupFuturesPerJob.compute(jobId, (ignored, completableFutures) -> {
            if (completableFutures == null) {
                completableFutures = new ArrayList<>(4);
            }

            completableFutures.add(taskTerminationWithResourceCleanupFuture);
            return completableFutures;
        });
    }

    private Stream<ResultPartitionDeploymentDescriptor> filterPartitionsRequiringRelease(
            Collection<ResultPartitionDeploymentDescriptor> producedResultPartitions) {
        return producedResultPartitions.stream()
                // only blocking partitions require explicit release call
                .filter(d -> d.getPartitionType().isBlocking())
                // partitions without local resources don't store anything on the TaskExecutor
                .filter(d -> d.getShuffleDescriptor().storesLocalResourcesOn().isPresent());
    }

    @Override
    public CompletableFuture<Acknowledge> cancelTask(ExecutionAttemptID executionAttemptID,
                                                     Time timeout) {
        final Task task = taskSlotTable.getTask(executionAttemptID);

        if (task != null) {
            try {
                task.cancelExecution();
                return CompletableFuture.completedFuture(Acknowledge.get());
            } catch (Throwable t) {
                return FutureUtils.completedExceptionally(
                        new TaskException("Cannot cancel task for execution " + executionAttemptID + '.', t));
            }
        } else {
            final String message = "Cannot find task to stop for execution " + executionAttemptID + '.';

            log.debug(message);
            return FutureUtils.completedExceptionally(new TaskException(message));
        }
    }

    // ----------------------------------------------------------------------
    // Partition lifecycle RPCs
    // ----------------------------------------------------------------------

    @Override
    public CompletableFuture<Acknowledge> updatePartitions(final ExecutionAttemptID executionAttemptID,
                                                           Iterable<PartitionInfo> partitionInfos,
                                                           Time timeout) {
        final Task task = taskSlotTable.getTask(executionAttemptID);

        if (task != null) {
            for (final PartitionInfo partitionInfo : partitionInfos) {
                // Run asynchronously because it might be blocking
                FutureUtils.assertNoException(CompletableFuture.runAsync(() -> {
                    try {
                        if (!shuffleEnvironment.updatePartitionInfo(executionAttemptID, partitionInfo)) {
                            log.debug(
                                    "Discard update for input gate partition {} of result {} in task {}. " + "The partition is no longer available.",
                                    partitionInfo.getShuffleDescriptor().getResultPartitionID(),
                                    partitionInfo.getIntermediateDataSetID(), executionAttemptID);
                        }
                    } catch (IOException | InterruptedException e) {
                        log.error("Could not update input data location for task {}. Trying to fail task.",
                                task.getTaskInfo().getTaskName(), e);
                        task.failExternally(e);
                    }
                }, getRpcService().getScheduledExecutor()));
            }
            return CompletableFuture.completedFuture(Acknowledge.get());
        } else {
            log.debug("Discard update for input partitions of task {}. Task is no longer running.", executionAttemptID);
            return CompletableFuture.completedFuture(Acknowledge.get());
        }
    }

    @Override
    public void releaseOrPromotePartitions(JobID jobId,
                                           Set<ResultPartitionID> partitionToRelease,
                                           Set<ResultPartitionID> partitionsToPromote) {
        try {
            partitionTracker.stopTrackingAndReleaseJobPartitions(partitionToRelease);
            partitionTracker.promoteJobPartitions(partitionsToPromote);

            closeJobManagerConnectionIfNoAllocatedResources(jobId);
        } catch (Throwable t) {
            // TODO: Do we still need this catch branch?
            onFatalError(t);
        }

        // TODO: Maybe it's better to return an Acknowledge here to notify the JM about the
        // success/failure with an Exception
    }

    @Override
    public CompletableFuture<Acknowledge> releaseClusterPartitions(Collection<IntermediateDataSetID> dataSetsToRelease,
                                                                   Time timeout) {
        partitionTracker.stopTrackingAndReleaseClusterPartitions(dataSetsToRelease);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    // ----------------------------------------------------------------------
    // Heartbeat RPC
    // ----------------------------------------------------------------------

    @Override
    public CompletableFuture<Void> heartbeatFromJobManager(ResourceID resourceID,
                                                           AllocatedSlotReport allocatedSlotReport) {
        return jobManagerHeartbeatManager.requestHeartbeat(resourceID, allocatedSlotReport);
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 这个代码是从节点执行的
     */
    @Override
    public CompletableFuture<Void> heartbeatFromResourceManager(ResourceID resourceID) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return resourceManagerHeartbeatManager.requestHeartbeat(resourceID, null);
    }

    // ----------------------------------------------------------------------
    // Checkpointing RPCs
    // ----------------------------------------------------------------------

    @Override
    public CompletableFuture<Acknowledge> triggerCheckpoint(ExecutionAttemptID executionAttemptID,
                                                            long checkpointId,
                                                            long checkpointTimestamp,
                                                            CheckpointOptions checkpointOptions) {
        log.debug("Trigger checkpoint {}@{} for {}.", checkpointId, checkpointTimestamp, executionAttemptID);

        final CheckpointType checkpointType = checkpointOptions.getCheckpointType();
        if (checkpointType.getPostCheckpointAction() == PostCheckpointAction.TERMINATE && !(checkpointType.isSynchronous() && checkpointType.isSavepoint()
        )) {
            throw new IllegalArgumentException("Only synchronous savepoints are allowed to advance the watermark to MAX.");
        }

        // TODO_MA 马中华 注释： 根据 execution 的 AttemptID 拿到 taskSlotTable 登记的 Task
        final Task task = taskSlotTable.getTask(executionAttemptID);

        if (task != null) {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            task.triggerCheckpointBarrier(checkpointId, checkpointTimestamp, checkpointOptions);

            return CompletableFuture.completedFuture(Acknowledge.get());
        } else {
            final String message = "TaskManager received a checkpoint request for unknown task " + executionAttemptID + '.';

            log.debug(message);
            return FutureUtils.completedExceptionally(
                    new CheckpointException(message, CheckpointFailureReason.TASK_CHECKPOINT_FAILURE));
        }
    }

    @Override
    public CompletableFuture<Acknowledge> confirmCheckpoint(ExecutionAttemptID executionAttemptID,
                                                            long checkpointId,
                                                            long checkpointTimestamp) {
        log.debug("Confirm checkpoint {}@{} for {}.", checkpointId, checkpointTimestamp, executionAttemptID);

        final Task task = taskSlotTable.getTask(executionAttemptID);

        if (task != null) {
            task.notifyCheckpointComplete(checkpointId);

            return CompletableFuture.completedFuture(Acknowledge.get());
        } else {
            final String message = "TaskManager received a checkpoint confirmation for unknown task " + executionAttemptID + '.';

            log.debug(message);
            return FutureUtils.completedExceptionally(
                    new CheckpointException(message, CheckpointFailureReason.UNKNOWN_TASK_CHECKPOINT_NOTIFICATION_FAILURE));
        }
    }

    @Override
    public CompletableFuture<Acknowledge> abortCheckpoint(ExecutionAttemptID executionAttemptID,
                                                          long checkpointId,
                                                          long latestCompletedCheckpointId,
                                                          long checkpointTimestamp) {
        log.debug("Abort checkpoint {}@{} for {}.", checkpointId, checkpointTimestamp, executionAttemptID);

        final Task task = taskSlotTable.getTask(executionAttemptID);

        if (task != null) {
            task.notifyCheckpointAborted(checkpointId, latestCompletedCheckpointId);

            return CompletableFuture.completedFuture(Acknowledge.get());
        } else {
            final String message = "TaskManager received an aborted checkpoint for unknown task " + executionAttemptID + '.';

            log.debug(message);
            return FutureUtils.completedExceptionally(
                    new CheckpointException(message, CheckpointFailureReason.UNKNOWN_TASK_CHECKPOINT_NOTIFICATION_FAILURE));
        }
    }

    // ----------------------------------------------------------------------
    // Slot allocation RPCs
    // ----------------------------------------------------------------------

    @Override
    public CompletableFuture<Acknowledge> requestSlot(final SlotID slotId,
                                                      final JobID jobId,
                                                      final AllocationID allocationId,
                                                      final ResourceProfile resourceProfile,
                                                      final String targetAddress,
                                                      final ResourceManagerId resourceManagerId,
                                                      final Time timeout) {
        // TODO: Filter invalid requests from the resource manager by using the
        // instance/registration Id

        log.info("Receive slot request {} for job {} from resource manager with leader id {}.", allocationId, jobId,
                resourceManagerId);

        if (!isConnectedToResourceManager(resourceManagerId)) {
            final String message = String.format("TaskManager is not connected to the resource manager %s.", resourceManagerId);
            log.debug(message);
            return FutureUtils.completedExceptionally(new TaskManagerException(message));
        }

        try {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 第一 ： 完成物理分配
             *  1、构造一个 TaskSlot 对象
             *  2、登记到 TaslSlotTable ： 表示这个 slot 已经被分配给某个 job 使用了
             *  TaskSlot 和 TaskSlotTable 之间的登记
             */
            allocateSlot(slotId, jobId, allocationId, resourceProfile);
        } catch (SlotAllocationException sae) {
            return FutureUtils.completedExceptionally(sae);
        }

        final JobTable.Job job;

        try {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 第二： 登记， JobOrConnection： 维护 TaskExecutor 和 JobMaster 之间的链接
             *  Job 和 JobTable
             */
            job = jobTable.getOrCreateJob(jobId, () -> registerNewJobAndCreateServices(jobId, targetAddress));
        } catch (Exception e) {
            // free the allocated slot
            try {
                taskSlotTable.freeSlot(allocationId);
            } catch (SlotNotFoundException slotNotFoundException) {
                // slot no longer existent, this should actually never happen, because we've
                // just allocated the slot. So let's fail hard in this case!
                onFatalError(slotNotFoundException);
            }

            // release local state under the allocation id.
            localStateStoresManager.releaseLocalStateForAllocationId(allocationId);

            // sanity check
            if (!taskSlotTable.isSlotFree(slotId.getSlotNumber())) {
                onFatalError(new Exception("Could not free slot " + slotId));
            }

            return FutureUtils.completedExceptionally(new SlotAllocationException("Could not create new job.", e));
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 第三： 汇报给 JobMaster
         */
        if (job.isConnected()) {
            offerSlotsToJobManager(jobId);
        }

        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    private TaskExecutorJobServices registerNewJobAndCreateServices(JobID jobId,
                                                                    String targetAddress) throws Exception {
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        jobLeaderService.addJob(jobId, targetAddress);
        final PermanentBlobCache permanentBlobService = blobCacheService.getPermanentBlobService();
        permanentBlobService.registerJob(jobId);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return TaskExecutorJobServices.create(libraryCacheManager.registerClassLoaderLease(jobId),
                () -> permanentBlobService.releaseJob(jobId));
    }

    // TODO_MA 马中华 注释： SlotID 是 ResourceManager 在做逻辑分配的指定的
    private void allocateSlot(SlotID slotId,
                              JobID jobId,
                              AllocationID allocationId,
                              ResourceProfile resourceProfile) throws SlotAllocationException {

        // TODO_MA 马中华 注释： 既然 ResourceManager 说这个 slot 需要分配给这个 JobID 对应的job
        if (taskSlotTable.isSlotFree(slotId.getSlotNumber())) {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 完成物理分配
             */
            if (taskSlotTable.allocateSlot(slotId.getSlotNumber(), jobId, allocationId, resourceProfile,
                    taskManagerConfiguration.getSlotTimeout())) {
                log.info("Allocated slot for {}.", allocationId);
            } else {
                log.info("Could not allocate slot for {}.", allocationId);
                throw new SlotAllocationException("Could not allocate slot.");
            }
        } else if (!taskSlotTable.isAllocated(slotId.getSlotNumber(), jobId, allocationId)) {
            final String message = "The slot " + slotId + " has already been allocated for a different job.";

            log.info(message);

            final AllocationID allocationID = taskSlotTable.getCurrentAllocation(slotId.getSlotNumber());
            throw new SlotOccupiedException(message, allocationID, taskSlotTable.getOwningJob(allocationID));
        }
    }

    @Override
    public CompletableFuture<Acknowledge> freeSlot(AllocationID allocationId,
                                                   Throwable cause,
                                                   Time timeout) {
        freeSlotInternal(allocationId, cause);

        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public void freeInactiveSlots(JobID jobId,
                                  Time timeout) {
        log.debug("Freeing inactive slots for job {}.", jobId);

        // need a copy to prevent ConcurrentModificationExceptions
        final ImmutableList<TaskSlot<Task>> inactiveSlots = ImmutableList.copyOf(taskSlotTable.getAllocatedSlots(jobId));
        for (TaskSlot<Task> slot : inactiveSlots) {
            freeSlotInternal(slot.getAllocationId(), new FlinkException("Slot was re-claimed by resource manager."));
        }
    }

    @Override
    public CompletableFuture<TransientBlobKey> requestFileUploadByType(FileType fileType,
                                                                       Time timeout) {
        final String filePath;
        switch (fileType) {
            case LOG:
                filePath = taskManagerConfiguration.getTaskManagerLogPath();
                break;
            case STDOUT:
                filePath = taskManagerConfiguration.getTaskManagerStdoutPath();
                break;
            default:
                filePath = null;
        }
        return requestFileUploadByFilePath(filePath, fileType.toString());
    }

    @Override
    public CompletableFuture<TransientBlobKey> requestFileUploadByName(String fileName,
                                                                       Time timeout) {
        final String filePath;
        final String logDir = taskManagerConfiguration.getTaskManagerLogDir();
        if (StringUtils.isNullOrWhitespaceOnly(logDir) || StringUtils.isNullOrWhitespaceOnly(fileName)) {
            filePath = null;
        } else {
            filePath = new File(logDir, new File(fileName).getName()).getPath();
        }
        return requestFileUploadByFilePath(filePath, fileName);
    }

    @Override
    public CompletableFuture<SerializableOptional<String>> requestMetricQueryServiceAddress(Time timeout) {
        return CompletableFuture.completedFuture(SerializableOptional.ofNullable(metricQueryServiceAddress));
    }

    // ----------------------------------------------------------------------
    // Disconnection RPCs
    // ----------------------------------------------------------------------

    @Override
    public void disconnectJobManager(JobID jobId,
                                     Exception cause) {
        jobTable.getConnection(jobId)
                .ifPresent(jobManagerConnection -> disconnectAndTryReconnectToJobManager(jobManagerConnection, cause));
    }

    private void disconnectAndTryReconnectToJobManager(JobTable.Connection jobManagerConnection,
                                                       Exception cause) {
        disconnectJobManagerConnection(jobManagerConnection, cause);
        jobLeaderService.reconnect(jobManagerConnection.getJobId());
    }

    @Override
    public void disconnectResourceManager(Exception cause) {
        if (isRunning()) {
            reconnectToResourceManager(cause);
        }
    }

    // ----------------------------------------------------------------------
    // Other RPCs
    // ----------------------------------------------------------------------

    @Override
    public CompletableFuture<Acknowledge> sendOperatorEventToTask(ExecutionAttemptID executionAttemptID,
                                                                  OperatorID operatorId,
                                                                  SerializedValue<OperatorEvent> evt) {

        log.debug("Operator event for {} - {}", executionAttemptID, operatorId);

        final Task task = taskSlotTable.getTask(executionAttemptID);
        if (task == null) {
            return FutureUtils.completedExceptionally(
                    new TaskNotRunningException("Task " + executionAttemptID + " not running on TaskManager"));
        }

        try {
            task.deliverOperatorEvent(operatorId, evt);
            return CompletableFuture.completedFuture(Acknowledge.get());
        } catch (Throwable t) {
            ExceptionUtils.rethrowIfFatalError(t);
            return FutureUtils.completedExceptionally(t);
        }
    }

    @Override
    public CompletableFuture<ThreadDumpInfo> requestThreadDump(Time timeout) {
        final Collection<ThreadInfo> threadDump = JvmUtils.createThreadDump();

        final Collection<ThreadDumpInfo.ThreadInfo> threadInfos = threadDump.stream()
                .map(threadInfo -> ThreadDumpInfo.ThreadInfo.create(threadInfo.getThreadName(), threadInfo.toString()))
                .collect(Collectors.toList());

        return CompletableFuture.completedFuture(ThreadDumpInfo.create(threadInfos));
    }

    // ------------------------------------------------------------------------
    //  Internal resource manager connection methods
    // ------------------------------------------------------------------------

    private void notifyOfNewResourceManagerLeader(String newLeaderAddress,
                                                  ResourceManagerId newResourceManagerId) {

        // TODO_MA 马中华 注释： 获取地址
        resourceManagerAddress = createResourceManagerAddress(newLeaderAddress, newResourceManagerId);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 但是为了复用和防止歧义：所以用 reconnect()
         */
        reconnectToResourceManager(
                new FlinkException(String.format("ResourceManager leader changed to new address %s", resourceManagerAddress)));
    }

    @Nullable
    private ResourceManagerAddress createResourceManagerAddress(@Nullable String newLeaderAddress,
                                                                @Nullable ResourceManagerId newResourceManagerId) {
        if (newLeaderAddress == null) {
            return null;
        } else {
            assert (newResourceManagerId != null);
            return new ResourceManagerAddress(newLeaderAddress, newResourceManagerId);
        }
    }

    // TODO_MA 马中华 注释： 既然获取到了 ResourceManager 的地址，也就意味着接下来要干的事情：
    // TODO_MA 马中华 注释： 1、建立连接
    // TODO_MA 马中华 注释： 2、完成注册（注册超时）
    private void reconnectToResourceManager(Exception cause) {

        // TODO_MA 马中华 注释： 万一连接还存在，先关闭，先确保没有链接
        closeResourceManagerConnection(cause);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 启动注册超时任务
         *  在 Flink 中，所有的超时任务，其实都可以理解成是 定时炸弹。反正到点了不取消，就会执行
         *  当工作完成之后，就会取消这个 超时任务  start() 启动， cancel() 取消
         *  cancelXXXXTimeOut() 取消
         *  startXXXXTimeOut() 启动
         */
        startRegistrationTimeout();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 开启连接，进行注册
         */
        tryConnectToResourceManager();
    }

    private void tryConnectToResourceManager() {
        if (resourceManagerAddress != null) {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            connectToResourceManager();
        }
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 注册：
     *  1、 先生成一个注册对象
     *  2、 建立连接
     *  3、 发送 RPC 请求，将注册对象发给对方（注册）
     */
    private void connectToResourceManager() {
        assert (resourceManagerAddress != null);
        assert (establishedResourceManagerConnection == null);
        assert (resourceManagerConnection == null);

        log.info("Connecting to ResourceManager {}.", resourceManagerAddress);

        // TODO_MA 马中华 注释： 生成一个注册对象（把当前从节点的各种必要信息，汇报给 主节点）
        final TaskExecutorRegistration taskExecutorRegistration = new TaskExecutorRegistration(getAddress(), getResourceID(),
                unresolvedTaskManagerLocation.getDataPort(), JMXService.getPort().orElse(-1), hardwareDescription,
                memoryConfiguration, taskManagerConfiguration.getDefaultSlotResourceProfile(),
                taskManagerConfiguration.getTotalResourceProfile());

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 2、建立连接，这个连接是一个 逻辑概念，并没有真正完成链接，只是包含了建立连接所需要的各种信息
         */
        resourceManagerConnection = new TaskExecutorToResourceManagerConnection(log, getRpcService(),
                taskManagerConfiguration.getRetryingRegistrationConfiguration(), resourceManagerAddress.getAddress(),
                resourceManagerAddress.getResourceManagerId(), getMainThreadExecutor(), new ResourceManagerRegistrationListener(),
                taskExecutorRegistration);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 2、链接启动
         */
        resourceManagerConnection.start();
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 关于从节点的启动，其实干三件事：
     *  当 TaskExecutor 创建好了之后，回去检索 ResourceManager 的地址信息，然后：
     *  1、 注册
     *  2、 维持心跳
     *  3、 做 slot 汇报
     *  -
     *  涉及到 slot 资源管理，这节课，不涉及资源管理
     *  A B D C 这个顺序， 是 java8 的异步编程 API 来控制的
     */
    private void establishResourceManagerConnection(ResourceManagerGateway resourceManagerGateway,
                                                    ResourceID resourceManagerResourceId,
                                                    InstanceID taskExecutorRegistrationId,
                                                    ClusterInformation clusterInformation) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： RPC 请求： 发送 slot 汇报
         */
        final CompletableFuture<Acknowledge> slotReportResponseFuture = resourceManagerGateway.sendSlotReport(getResourceID(),
                taskExecutorRegistrationId, taskSlotTable.createSlotReport(getResourceID()),
                taskManagerConfiguration.getRpcTimeout());

        // TODO_MA 马中华 注释： 如果发送报告有问题，则重连
        slotReportResponseFuture.whenCompleteAsync((acknowledge, throwable) -> {
            if (throwable != null) {
                reconnectToResourceManager(
                        new TaskManagerException("Failed to send initial slot report to ResourceManager.", throwable));
            }
        }, getMainThreadExecutor());

        // monitor the resource manager as heartbeat target
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 加入心跳管理
         */
        resourceManagerHeartbeatManager.monitorTarget(resourceManagerResourceId,
                new ResourceManagerHeartbeatTarget(resourceManagerGateway));

        // set the propagated blob server address
        final InetSocketAddress blobServerAddress = new InetSocketAddress(clusterInformation.getBlobServerHostname(),
                clusterInformation.getBlobServerPort());

        blobCacheService.setBlobServerAddress(blobServerAddress);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 生成一个链接管理器
         */
        establishedResourceManagerConnection = new EstablishedResourceManagerConnection(resourceManagerGateway,
                resourceManagerResourceId, taskExecutorRegistrationId);

        stopRegistrationTimeout();
    }

    private void closeResourceManagerConnection(Exception cause) {
        if (establishedResourceManagerConnection != null) {
            final ResourceID resourceManagerResourceId = establishedResourceManagerConnection.getResourceManagerResourceId();

            if (log.isDebugEnabled()) {
                log.debug("Close ResourceManager connection {}.", resourceManagerResourceId, cause);
            } else {
                log.info("Close ResourceManager connection {}.", resourceManagerResourceId);
            }
            resourceManagerHeartbeatManager.unmonitorTarget(resourceManagerResourceId);

            ResourceManagerGateway resourceManagerGateway = establishedResourceManagerConnection.getResourceManagerGateway();
            resourceManagerGateway.disconnectTaskManager(getResourceID(), cause);

            establishedResourceManagerConnection = null;

            partitionTracker.stopTrackingAndReleaseAllClusterPartitions();
        }

        if (resourceManagerConnection != null) {
            if (!resourceManagerConnection.isConnected()) {
                if (log.isDebugEnabled()) {
                    log.debug("Terminating registration attempts towards ResourceManager {}.",
                            resourceManagerConnection.getTargetAddress(), cause);
                } else {
                    log.info("Terminating registration attempts towards ResourceManager {}.",
                            resourceManagerConnection.getTargetAddress());
                }
            }

            resourceManagerConnection.close();
            resourceManagerConnection = null;
        }
    }

    private void startRegistrationTimeout() {

        final Time maxRegistrationDuration = taskManagerConfiguration.getMaxRegistrationDuration();

        if (maxRegistrationDuration != null) {
            final UUID newRegistrationTimeoutId = UUID.randomUUID();
            currentRegistrationTimeoutId = newRegistrationTimeoutId;

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            scheduleRunAsync(() -> registrationTimeout(newRegistrationTimeoutId), maxRegistrationDuration);
        }
    }

    private void stopRegistrationTimeout() {
        currentRegistrationTimeoutId = null;
    }

    private void registrationTimeout(@Nonnull UUID registrationTimeoutId) {

        if (registrationTimeoutId.equals(currentRegistrationTimeoutId)) {
            final Time maxRegistrationDuration = taskManagerConfiguration.getMaxRegistrationDuration();

            onFatalError(new RegistrationTimeoutException(String.format(
                    "Could not register at the ResourceManager within the specified maximum " + "registration duration %s. This indicates a problem with this instance. Terminating now.",
                    maxRegistrationDuration)));
        }
    }

    // ------------------------------------------------------------------------
    //  Internal job manager connection methods
    // ------------------------------------------------------------------------

    private void offerSlotsToJobManager(final JobID jobId) {
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        jobTable.getConnection(jobId).ifPresent(this::internalOfferSlotsToJobManager);
    }

    private void internalOfferSlotsToJobManager(JobTable.Connection jobManagerConnection) {
        final JobID jobId = jobManagerConnection.getJobId();

        if (taskSlotTable.hasAllocatedSlots(jobId)) {
            log.info("Offer reserved slots to the leader of job {}.", jobId);

            // TODO_MA 马中华 注释：
            final JobMasterGateway jobMasterGateway = jobManagerConnection.getJobManagerGateway();

            final Iterator<TaskSlot<Task>> reservedSlotsIterator = taskSlotTable.getAllocatedSlots(jobId);
            final JobMasterId jobMasterId = jobManagerConnection.getJobMasterId();

            final Collection<SlotOffer> reservedSlots = new HashSet<>(2);

            while (reservedSlotsIterator.hasNext()) {
                SlotOffer offer = reservedSlotsIterator.next().generateSlotOffer();
                reservedSlots.add(offer);
            }

            final UUID slotOfferId = UUID.randomUUID();
            currentSlotOfferPerJob.put(jobId, slotOfferId);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 发送 RPC 请求
             */
            CompletableFuture<Collection<SlotOffer>> acceptedSlotsFuture = jobMasterGateway.offerSlots(getResourceID(),
                    reservedSlots, taskManagerConfiguration.getRpcTimeout());

            acceptedSlotsFuture.whenCompleteAsync(
                    handleAcceptedSlotOffers(jobId, jobMasterGateway, jobMasterId, reservedSlots, slotOfferId),
                    getMainThreadExecutor());
        } else {
            log.debug("There are no unassigned slots for the job {}.", jobId);
        }
    }

    @Nonnull
    private BiConsumer<Iterable<SlotOffer>, Throwable> handleAcceptedSlotOffers(JobID jobId,
                                                                                JobMasterGateway jobMasterGateway,
                                                                                JobMasterId jobMasterId,
                                                                                Collection<SlotOffer> offeredSlots,
                                                                                UUID offerId) {
        return (Iterable<SlotOffer> acceptedSlots, Throwable throwable) -> {
            // check if this is the latest offer
            if (!offerId.equals(currentSlotOfferPerJob.get(jobId))) {
                // If this offer is outdated then it can be safely ignored.
                // If the response for a given slot is identical in both offers (accepted/rejected),
                // then this is naturally the case since the end-result is the same.
                // If the responses differ, then there are 2 cases to consider:
                // 1) initially rejected, later accepted
                //   This can happen when the resource requirements of a job increases between
                //   offers.
                //   In this case the first response MUST be ignored, so that
                //   the the slot can be properly activated when the second response arrives.
                // 2) initially accepted, later rejected
                //   This can happen when the resource requirements of a job decrease between
                //   offers.
                //   In this case the first response MAY be ignored, because the job no longer
                //   requires the slot (and already has initiated steps to free it) and we can thus
                //   assume that any in-flight task submissions are no longer relevant for the job
                //   execution.

                log.debug("Discard slot offer response since there is a newer offer for the job {}.", jobId);
                return;
            }

            if (throwable != null) {
                if (throwable instanceof TimeoutException) {
                    log.info("Slot offering to JobManager did not finish in time. Retrying the slot offering.");
                    // We ran into a timeout. Try again.
                    offerSlotsToJobManager(jobId);
                } else {
                    log.warn(
                            "Slot offering to JobManager failed. Freeing the slots " + "and returning them to the ResourceManager.",
                            throwable);

                    // We encountered an exception. Free the slots and return them to the RM.
                    for (SlotOffer reservedSlot : offeredSlots) {
                        freeSlotInternal(reservedSlot.getAllocationId(), throwable);
                    }
                }
            } else {
                // check if the response is still valid
                if (isJobManagerConnectionValid(jobId, jobMasterId)) {
                    // mark accepted slots active
                    for (SlotOffer acceptedSlot : acceptedSlots) {
                        final AllocationID allocationId = acceptedSlot.getAllocationId();
                        try {
                            if (!taskSlotTable.markSlotActive(allocationId)) {
                                // the slot is either free or releasing at the moment
                                final String message = "Could not mark slot " + allocationId + " active.";
                                log.debug(message);
                                jobMasterGateway.failSlot(getResourceID(), allocationId, new FlinkException(message));
                            }
                        } catch (SlotNotFoundException e) {
                            final String message = "Could not mark slot " + allocationId + " active.";
                            jobMasterGateway.failSlot(getResourceID(), allocationId, new FlinkException(message));
                        }

                        offeredSlots.remove(acceptedSlot);
                    }

                    final Exception e = new Exception("The slot was rejected by the JobManager.");

                    for (SlotOffer rejectedSlot : offeredSlots) {
                        freeSlotInternal(rejectedSlot.getAllocationId(), e);
                    }
                } else {
                    // discard the response since there is a new leader for the job
                    log.debug("Discard slot offer response since there is a new leader " + "for the job {}.", jobId);
                }
            }
        };
    }

    private void establishJobManagerConnection(JobTable.Job job,
                                               final JobMasterGateway jobMasterGateway,
                                               JMTMRegistrationSuccess registrationSuccess) {

        final JobID jobId = job.getJobId();
        final Optional<JobTable.Connection> connection = job.asConnection();

        if (connection.isPresent()) {
            JobTable.Connection oldJobManagerConnection = connection.get();

            if (Objects.equals(oldJobManagerConnection.getJobMasterId(), jobMasterGateway.getFencingToken())) {
                // we already are connected to the given job manager
                log.debug("Ignore JobManager gained leadership message for {} because we are already connected to it.",
                        jobMasterGateway.getFencingToken());
                return;
            } else {
                disconnectJobManagerConnection(oldJobManagerConnection,
                        new Exception("Found new job leader for job id " + jobId + '.'));
            }
        }

        log.info("Establish JobManager connection for job {}.", jobId);

        ResourceID jobManagerResourceID = registrationSuccess.getResourceID();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        final JobTable.Connection establishedConnection = associateWithJobManager(job, jobManagerResourceID, jobMasterGateway);

        // monitor the job manager as heartbeat target
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        jobManagerHeartbeatManager.monitorTarget(jobManagerResourceID, new JobManagerHeartbeatTarget(jobMasterGateway));

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        internalOfferSlotsToJobManager(establishedConnection);
    }

    private void closeJob(JobTable.Job job,
                          Exception cause) {
        job.asConnection().ifPresent(jobManagerConnection -> disconnectJobManagerConnection(jobManagerConnection, cause));

        job.close();
    }

    private void disconnectJobManagerConnection(JobTable.Connection jobManagerConnection,
                                                Exception cause) {
        final JobID jobId = jobManagerConnection.getJobId();
        if (log.isDebugEnabled()) {
            log.debug("Close JobManager connection for job {}.", jobId, cause);
        } else {
            log.info("Close JobManager connection for job {}.", jobId);
        }

        // 1. fail tasks running under this JobID
        Iterator<Task> tasks = taskSlotTable.getTasks(jobId);

        final FlinkException failureCause = new FlinkException(
                String.format("Disconnect from JobManager responsible for %s.", jobId), cause);

        while (tasks.hasNext()) {
            tasks.next().failExternally(failureCause);
        }

        // 2. Move the active slots to state allocated (possible to time out again)
        Set<AllocationID> activeSlotAllocationIDs = taskSlotTable.getActiveTaskSlotAllocationIdsPerJob(jobId);

        final FlinkException freeingCause = new FlinkException("Slot could not be marked inactive.");

        for (AllocationID activeSlotAllocationID : activeSlotAllocationIDs) {
            try {
                if (!taskSlotTable.markSlotInactive(activeSlotAllocationID, taskManagerConfiguration.getSlotTimeout())) {
                    freeSlotInternal(activeSlotAllocationID, freeingCause);
                }
            } catch (SlotNotFoundException e) {
                log.debug("Could not mark the slot {} inactive.", activeSlotAllocationID, e);
            }
        }

        // 3. Disassociate from the JobManager
        try {
            jobManagerHeartbeatManager.unmonitorTarget(jobManagerConnection.getResourceId());
            disassociateFromJobManager(jobManagerConnection, cause);
        } catch (IOException e) {
            log.warn("Could not properly disassociate from JobManager {}.",
                    jobManagerConnection.getJobManagerGateway().getAddress(), e);
        }

        jobManagerConnection.disconnect();
    }

    private JobTable.Connection associateWithJobManager(JobTable.Job job,
                                                        ResourceID resourceID,
                                                        JobMasterGateway jobMasterGateway) {
        checkNotNull(resourceID);
        checkNotNull(jobMasterGateway);

        TaskManagerActions taskManagerActions = new TaskManagerActionsImpl(jobMasterGateway);

        CheckpointResponder checkpointResponder = new RpcCheckpointResponder(jobMasterGateway);
        GlobalAggregateManager aggregateManager = new RpcGlobalAggregateManager(jobMasterGateway);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        ResultPartitionConsumableNotifier resultPartitionConsumableNotifier = new RpcResultPartitionConsumableNotifier(
                jobMasterGateway, getRpcService().getScheduledExecutor(), taskManagerConfiguration.getRpcTimeout());

        PartitionProducerStateChecker partitionStateChecker = new RpcPartitionStateChecker(jobMasterGateway);

        registerQueryableState(job.getJobId(), jobMasterGateway);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return job.connect(resourceID, jobMasterGateway, taskManagerActions, checkpointResponder, aggregateManager,
                resultPartitionConsumableNotifier, partitionStateChecker);
    }

    private void disassociateFromJobManager(JobTable.Connection jobManagerConnection,
                                            Exception cause) throws IOException {
        checkNotNull(jobManagerConnection);

        final JobID jobId = jobManagerConnection.getJobId();

        // cleanup remaining partitions once all tasks for this job have completed
        scheduleResultPartitionCleanup(jobId);

        final KvStateRegistry kvStateRegistry = kvStateService.getKvStateRegistry();

        if (kvStateRegistry != null) {
            kvStateRegistry.unregisterListener(jobId);
        }

        final KvStateClientProxy kvStateClientProxy = kvStateService.getKvStateClientProxy();

        if (kvStateClientProxy != null) {
            kvStateClientProxy.updateKvStateLocationOracle(jobManagerConnection.getJobId(), null);
        }

        JobMasterGateway jobManagerGateway = jobManagerConnection.getJobManagerGateway();
        jobManagerGateway.disconnectTaskManager(getResourceID(), cause);
    }

    private void handleRejectedJobManagerConnection(JobID jobId,
                                                    String targetAddress,
                                                    JMTMRegistrationRejection rejection) {
        log.info("The JobManager under {} rejected the registration for job {}: {}. Releasing all job related resources.",
                targetAddress, jobId, rejection.getReason());

        releaseJobResources(jobId, new FlinkException(String.format("JobManager %s has rejected the registration.", jobId)));
    }

    private void releaseJobResources(JobID jobId,
                                     Exception cause) {
        log.debug("Releasing job resources for job {}.", jobId, cause);

        if (partitionTracker.isTrackingPartitionsFor(jobId)) {
            // stop tracking job partitions
            partitionTracker.stopTrackingAndReleaseJobPartitionsFor(jobId);
        }

        // free slots
        final Set<AllocationID> allocationIds = taskSlotTable.getAllocationIdsPerJob(jobId);

        if (!allocationIds.isEmpty()) {
            for (AllocationID allocationId : allocationIds) {
                freeSlotInternal(allocationId, cause);
            }
        }

        jobLeaderService.removeJob(jobId);
        jobTable.getJob(jobId).ifPresent(job -> {
            closeJob(job, cause);
        });
        changelogStoragesManager.releaseStateChangelogStorageForJob(jobId);
        currentSlotOfferPerJob.remove(jobId);
    }

    private void scheduleResultPartitionCleanup(JobID jobId) {
        final Collection<CompletableFuture<ExecutionState>> taskTerminationFutures = taskResultPartitionCleanupFuturesPerJob.remove(
                jobId);
        if (taskTerminationFutures != null) {
            FutureUtils.waitForAll(taskTerminationFutures).thenRunAsync(() -> {
                partitionTracker.stopTrackingAndReleaseJobPartitionsFor(jobId);
            }, getMainThreadExecutor());
        }
    }

    private void registerQueryableState(JobID jobId,
                                        JobMasterGateway jobMasterGateway) {
        final KvStateServer kvStateServer = kvStateService.getKvStateServer();
        final KvStateRegistry kvStateRegistry = kvStateService.getKvStateRegistry();

        if (kvStateServer != null && kvStateRegistry != null) {
            kvStateRegistry.registerListener(jobId,
                    new RpcKvStateRegistryListener(jobMasterGateway, kvStateServer.getServerAddress()));
        }

        final KvStateClientProxy kvStateProxy = kvStateService.getKvStateClientProxy();

        if (kvStateProxy != null) {
            kvStateProxy.updateKvStateLocationOracle(jobId, jobMasterGateway);
        }
    }

    // ------------------------------------------------------------------------
    //  Internal task methods
    // ------------------------------------------------------------------------

    private void failTask(final ExecutionAttemptID executionAttemptID, final Throwable cause) {

        // TODO_MA 马中华 注释：
        final Task task = taskSlotTable.getTask(executionAttemptID);

        if (task != null) {
            try {
                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释：
                 */
                task.failExternally(cause);
            } catch (Throwable t) {
                log.error("Could not fail task {}.", executionAttemptID, t);
            }
        } else {
            log.info("Cannot find task to fail for execution {} with exception:", executionAttemptID, cause);
        }
    }

    private void updateTaskExecutionState(final JobMasterGateway jobMasterGateway,
                                          final TaskExecutionState taskExecutionState) {
        final ExecutionAttemptID executionAttemptID = taskExecutionState.getID();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 发送 RPC 请求
         */
        CompletableFuture<Acknowledge> futureAcknowledge = jobMasterGateway.updateTaskExecutionState(taskExecutionState);

        futureAcknowledge.whenCompleteAsync((ack, throwable) -> {
            if (throwable != null) {
                failTask(executionAttemptID, throwable);
            }
        }, getMainThreadExecutor());
    }

    private void unregisterTaskAndNotifyFinalState(final JobMasterGateway jobMasterGateway,
                                                   final ExecutionAttemptID executionAttemptID) {

        Task task = taskSlotTable.removeTask(executionAttemptID);
        if (task != null) {
            if (!task.getExecutionState().isTerminal()) {
                try {
                    task.failExternally(new IllegalStateException("Task is being remove from TaskManager."));
                } catch (Exception e) {
                    log.error("Could not properly fail task.", e);
                }
            }

            log.info("Un-registering task and sending final execution state {} to JobManager for task {} {}.",
                    task.getExecutionState(), task.getTaskInfo().getTaskNameWithSubtasks(), task.getExecutionId());

            AccumulatorSnapshot accumulatorSnapshot = task.getAccumulatorRegistry().getSnapshot();

            updateTaskExecutionState(jobMasterGateway,
                    new TaskExecutionState(task.getExecutionId(), task.getExecutionState(), task.getFailureCause(),
                            accumulatorSnapshot, task.getMetricGroup().getIOMetricGroup().createSnapshot()));
        } else {
            log.error("Cannot find task with ID {} to unregister.", executionAttemptID);
        }
    }

    private void freeSlotInternal(AllocationID allocationId,
                                  Throwable cause) {
        checkNotNull(allocationId);

        log.debug("Free slot with allocation id {} because: {}", allocationId, cause.getMessage());

        try {
            final JobID jobId = taskSlotTable.getOwningJob(allocationId);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            final int slotIndex = taskSlotTable.freeSlot(allocationId, cause);

            if (slotIndex != -1) {

                if (isConnectedToResourceManager()) {
                    // the slot was freed. Tell the RM about it
                    ResourceManagerGateway resourceManagerGateway = establishedResourceManagerConnection.getResourceManagerGateway();

                    resourceManagerGateway.notifySlotAvailable(establishedResourceManagerConnection.getTaskExecutorRegistrationId(),
                            new SlotID(getResourceID(), slotIndex), allocationId);
                }

                if (jobId != null) {
                    closeJobManagerConnectionIfNoAllocatedResources(jobId);
                }
            }
        } catch (SlotNotFoundException e) {
            log.debug("Could not free slot for allocation id {}.", allocationId, e);
        }

        localStateStoresManager.releaseLocalStateForAllocationId(allocationId);
    }

    private void closeJobManagerConnectionIfNoAllocatedResources(JobID jobId) {
        // check whether we still have allocated slots for the same job
        if (taskSlotTable.getAllocationIdsPerJob(jobId).isEmpty() && !partitionTracker.isTrackingPartitionsFor(jobId)) {
            // we can remove the job from the job leader service

            final FlinkException cause = new FlinkException(
                    "TaskExecutor " + getAddress() + " has no more allocated slots for job " + jobId + '.');

            releaseJobResources(jobId, cause);
        }
    }

    private void timeoutSlot(AllocationID allocationId,
                             UUID ticket) {
        checkNotNull(allocationId);
        checkNotNull(ticket);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        if (taskSlotTable.isValidTimeout(allocationId, ticket)) {
            freeSlotInternal(allocationId, new Exception("The slot " + allocationId + " has timed out."));
        } else {
            log.debug("Received an invalid timeout for allocation id {} with ticket {}.", allocationId, ticket);
        }
    }

    /**
     * Syncs the TaskExecutor's view on its allocated slots with the JobMaster's view. Slots which
     * are no longer reported by the JobMaster are being freed. Slots which the JobMaster thinks it
     * still owns but which are no longer allocated to it will be failed via {@link
     * JobMasterGateway#failSlot}.
     *
     * @param jobMasterGateway jobMasterGateway to talk to the connected job master
     * @param allocatedSlotReport represents the JobMaster's view on the current slot allocation
     *         state
     */
    private void syncSlotsWithSnapshotFromJobMaster(JobMasterGateway jobMasterGateway,
                                                    AllocatedSlotReport allocatedSlotReport) {
        failNoLongerAllocatedSlots(allocatedSlotReport, jobMasterGateway);
        freeNoLongerUsedSlots(allocatedSlotReport);
    }

    private void failNoLongerAllocatedSlots(AllocatedSlotReport allocatedSlotReport,
                                            JobMasterGateway jobMasterGateway) {
        for (AllocatedSlotInfo allocatedSlotInfo : allocatedSlotReport.getAllocatedSlotInfos()) {
            final AllocationID allocationId = allocatedSlotInfo.getAllocationId();
            if (!taskSlotTable.isAllocated(allocatedSlotInfo.getSlotIndex(), allocatedSlotReport.getJobId(), allocationId)) {
                jobMasterGateway.failSlot(getResourceID(), allocationId, new FlinkException(
                        String.format("Slot %s on TaskExecutor %s is not allocated by job %s.", allocatedSlotInfo.getSlotIndex(),
                                getResourceID().getStringWithMetadata(), allocatedSlotReport.getJobId())));
            }
        }
    }

    private void freeNoLongerUsedSlots(AllocatedSlotReport allocatedSlotReport) {
        final Set<AllocationID> activeSlots = taskSlotTable.getActiveTaskSlotAllocationIdsPerJob(allocatedSlotReport.getJobId());
        final Set<AllocationID> reportedSlots = allocatedSlotReport.getAllocatedSlotInfos().stream()
                .map(AllocatedSlotInfo::getAllocationId).collect(Collectors.toSet());

        final Sets.SetView<AllocationID> difference = Sets.difference(activeSlots, reportedSlots);

        for (AllocationID allocationID : difference) {
            freeSlotInternal(allocationID, new FlinkException(
                    String.format("%s is no longer allocated by job %s.", allocationID, allocatedSlotReport.getJobId())));
        }
    }

    // ------------------------------------------------------------------------
    //  Internal utility methods
    // ------------------------------------------------------------------------

    private boolean isConnectedToResourceManager() {
        return establishedResourceManagerConnection != null;
    }

    private boolean isConnectedToResourceManager(ResourceManagerId resourceManagerId) {
        return establishedResourceManagerConnection != null && resourceManagerAddress != null && resourceManagerAddress.getResourceManagerId()
                .equals(resourceManagerId);
    }

    private boolean isJobManagerConnectionValid(JobID jobId,
                                                JobMasterId jobMasterId) {
        return jobTable.getConnection(jobId).map(jmConnection -> Objects.equals(jmConnection.getJobMasterId(), jobMasterId))
                .orElse(false);
    }

    private CompletableFuture<TransientBlobKey> requestFileUploadByFilePath(String filePath,
                                                                            String fileTag) {
        log.debug("Received file upload request for file {}", fileTag);
        if (!StringUtils.isNullOrWhitespaceOnly(filePath)) {
            return CompletableFuture.supplyAsync(() -> {
                final File file = new File(filePath);
                if (file.exists()) {
                    try {
                        return putTransientBlobStream(new FileInputStream(file), fileTag).get();
                    } catch (Exception e) {
                        log.debug("Could not upload file {}.", fileTag, e);
                        throw new CompletionException(new FlinkException("Could not upload file " + fileTag + '.', e));
                    }
                } else {
                    log.debug("The file {} does not exist on the TaskExecutor {}.", fileTag,
                            getResourceID().getStringWithMetadata());
                    throw new CompletionException(
                            new FlinkException("The file " + fileTag + " does not exist on the TaskExecutor."));
                }
            }, ioExecutor);
        } else {
            log.debug("The file {} is unavailable on the TaskExecutor {}.", fileTag, getResourceID().getStringWithMetadata());
            return FutureUtils.completedExceptionally(
                    new FlinkException("The file " + fileTag + " is not available on the TaskExecutor."));
        }
    }

    private CompletableFuture<TransientBlobKey> putTransientBlobStream(InputStream inputStream,
                                                                       String fileTag) {
        final TransientBlobCache transientBlobService = blobCacheService.getTransientBlobService();
        final TransientBlobKey transientBlobKey;

        try {
            transientBlobKey = transientBlobService.putTransient(inputStream);
        } catch (IOException e) {
            log.debug("Could not upload file {}.", fileTag, e);
            return FutureUtils.completedExceptionally(new FlinkException("Could not upload file " + fileTag + '.', e));
        }
        return CompletableFuture.completedFuture(transientBlobKey);
    }

    // ------------------------------------------------------------------------
    //  Properties
    // ------------------------------------------------------------------------

    public ResourceID getResourceID() {
        return unresolvedTaskManagerLocation.getResourceID();
    }

    // ------------------------------------------------------------------------
    //  Error Handling
    // ------------------------------------------------------------------------

    /**
     * Notifies the TaskExecutor that a fatal error has occurred and it cannot proceed.
     *
     * @param t The exception describing the fatal error
     */
    void onFatalError(final Throwable t) {
        try {
            log.error("Fatal error occurred in TaskExecutor {}.", getAddress(), t);
        } catch (Throwable ignored) {
        }

        // The fatal error handler implementation should make sure that this call is non-blocking
        fatalErrorHandler.onFatalError(t);
    }

    // ------------------------------------------------------------------------
    //  Access to fields for testing
    // ------------------------------------------------------------------------

    @VisibleForTesting
    TaskExecutorToResourceManagerConnection getResourceManagerConnection() {
        return resourceManagerConnection;
    }

    @VisibleForTesting
    HeartbeatManager<Void, TaskExecutorHeartbeatPayload> getResourceManagerHeartbeatManager() {
        return resourceManagerHeartbeatManager;
    }

    // ------------------------------------------------------------------------
    //  Utility classes
    // ------------------------------------------------------------------------

    private static final class JobManagerHeartbeatTarget implements HeartbeatTarget<TaskExecutorToJobManagerHeartbeatPayload> {
        private final JobMasterGateway jobMasterGateway;

        private JobManagerHeartbeatTarget(JobMasterGateway jobMasterGateway) {
            this.jobMasterGateway = jobMasterGateway;
        }

        @Override
        public CompletableFuture<Void> receiveHeartbeat(ResourceID resourceID,
                                                        TaskExecutorToJobManagerHeartbeatPayload payload) {
            return jobMasterGateway.heartbeatFromTaskManager(resourceID, payload);
        }

        @Override
        public CompletableFuture<Void> requestHeartbeat(ResourceID resourceID,
                                                        TaskExecutorToJobManagerHeartbeatPayload payload) {
            // request heartbeat will never be called on the task manager side
            return FutureUtils.unsupportedOperationFuture();
        }
    }

    private static final class ResourceManagerHeartbeatTarget implements HeartbeatTarget<TaskExecutorHeartbeatPayload> {
        private final ResourceManagerGateway resourceManagerGateway;

        private ResourceManagerHeartbeatTarget(ResourceManagerGateway resourceManagerGateway) {
            this.resourceManagerGateway = resourceManagerGateway;
        }

        @Override
        public CompletableFuture<Void> receiveHeartbeat(ResourceID resourceID,
                                                        TaskExecutorHeartbeatPayload heartbeatPayload) {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 真正发送 RPC 请求
             *  这个代码在从节点执行的
             */
            return resourceManagerGateway.heartbeatFromTaskManager(resourceID, heartbeatPayload);
        }

        @Override
        public CompletableFuture<Void> requestHeartbeat(ResourceID resourceID,
                                                        TaskExecutorHeartbeatPayload heartbeatPayload) {
            // the TaskManager won't send heartbeat requests to the ResourceManager
            return FutureUtils.unsupportedOperationFuture();
        }
    }

    /** The listener for leader changes of the resource manager. */
    private final class ResourceManagerLeaderListener implements LeaderRetrievalListener {

        @Override
        public void notifyLeaderAddress(final String leaderAddress,
                                        final UUID leaderSessionID) {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：  1111111111111111111111111111111111111
             *  我们已经拿到了 ResourceManager的地址信息， 然后建立连接，完成注册
             */
            runAsync(() -> notifyOfNewResourceManagerLeader(leaderAddress, ResourceManagerId.fromUuidOrNull(leaderSessionID)));
        }

        @Override
        public void handleError(Exception exception) {
            onFatalError(exception);
        }
    }

    private final class JobLeaderListenerImpl implements JobLeaderListener {

        @Override
        public void jobManagerGainedLeadership(final JobID jobId,
                                               final JobMasterGateway jobManagerGateway,
                                               final JMTMRegistrationSuccess registrationMessage) {
            runAsync(() -> jobTable.getJob(jobId).ifPresent(
                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释：
                     */
                    job -> establishJobManagerConnection(job, jobManagerGateway, registrationMessage)));
        }

        @Override
        public void jobManagerLostLeadership(final JobID jobId,
                                             final JobMasterId jobMasterId) {
            log.info("JobManager for job {} with leader id {} lost leadership.", jobId, jobMasterId);

            runAsync(() -> jobTable.getConnection(jobId).ifPresent(
                    jobManagerConnection -> disconnectJobManagerConnection(jobManagerConnection,
                            new Exception("Job leader for job id " + jobId + " lost leadership."))));
        }

        @Override
        public void handleError(Throwable throwable) {
            onFatalError(throwable);
        }

        @Override
        public void jobManagerRejectedRegistration(JobID jobId,
                                                   String targetAddress,
                                                   JMTMRegistrationRejection rejection) {
            runAsync(() -> handleRejectedJobManagerConnection(jobId, targetAddress, rejection));
        }
    }

    private final class ResourceManagerRegistrationListener implements RegistrationConnectionListener<TaskExecutorToResourceManagerConnection, TaskExecutorRegistrationSuccess, TaskExecutorRegistrationRejection> {

        @Override
        public void onRegistrationSuccess(TaskExecutorToResourceManagerConnection connection,
                                          TaskExecutorRegistrationSuccess success) {
            final ResourceID resourceManagerId = success.getResourceManagerId();
            final InstanceID taskExecutorRegistrationId = success.getRegistrationId();
            final ClusterInformation clusterInformation = success.getClusterInformation();
            final ResourceManagerGateway resourceManagerGateway = connection.getTargetGateway();

            runAsync(() -> {
                // filter out outdated connections
                //noinspection ObjectEquality
                if (resourceManagerConnection == connection) {
                    try {
                        /*************************************************
                         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                         *  注释： 
                         */
                        establishResourceManagerConnection(resourceManagerGateway, resourceManagerId, taskExecutorRegistrationId,
                                clusterInformation);
                    } catch (Throwable t) {
                        log.error("Establishing Resource Manager connection in Task Executor failed", t);
                    }
                }
            });
        }

        @Override
        public void onRegistrationFailure(Throwable failure) {
            onFatalError(failure);
        }

        @Override
        public void onRegistrationRejection(String targetAddress,
                                            TaskExecutorRegistrationRejection rejection) {
            onFatalError(new FlinkException(
                    String.format("The TaskExecutor's registration at the ResourceManager %s has been rejected: %s", targetAddress,
                            rejection)));
        }
    }

    private final class TaskManagerActionsImpl implements TaskManagerActions {
        private final JobMasterGateway jobMasterGateway;

        private TaskManagerActionsImpl(JobMasterGateway jobMasterGateway) {
            this.jobMasterGateway = checkNotNull(jobMasterGateway);
        }

        @Override
        public void notifyFatalError(String message,
                                     Throwable cause) {
            try {
                log.error(message, cause);
            } catch (Throwable ignored) {
            }

            // The fatal error handler implementation should make sure that this call is
            // non-blocking
            fatalErrorHandler.onFatalError(cause);
        }

        @Override
        public void failTask(final ExecutionAttemptID executionAttemptID,
                             final Throwable cause) {
            runAsync(() -> TaskExecutor.this.failTask(executionAttemptID, cause));
        }

        @Override
        public void updateTaskExecutionState(final TaskExecutionState taskExecutionState) {
            if (taskExecutionState.getExecutionState().isTerminal()) {
                runAsync(() -> unregisterTaskAndNotifyFinalState(jobMasterGateway, taskExecutionState.getID()));
            } else {
                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释：
                 */
                TaskExecutor.this.updateTaskExecutionState(jobMasterGateway, taskExecutionState);
            }
        }
    }

    private class SlotActionsImpl implements SlotActions {

        @Override
        public void freeSlot(final AllocationID allocationId) {
            runAsync(() -> freeSlotInternal(allocationId,
                    new FlinkException("TaskSlotTable requested freeing the TaskSlot " + allocationId + '.')));
        }

        @Override
        public void timeoutSlot(final AllocationID allocationId,
                                final UUID ticket) {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            runAsync(() -> TaskExecutor.this.timeoutSlot(allocationId, ticket));
        }
    }

    private class JobManagerHeartbeatListener implements HeartbeatListener<AllocatedSlotReport, TaskExecutorToJobManagerHeartbeatPayload> {

        @Override
        public void notifyHeartbeatTimeout(final ResourceID resourceID) {
            final String message = String.format("The heartbeat of JobManager with id %s timed out.",
                    resourceID.getStringWithMetadata());

            log.info(message);
            handleJobManagerConnectionLoss(resourceID, new TimeoutException(message));
        }

        @Override
        public void notifyTargetUnreachable(ResourceID resourceID) {
            final String message = String.format("JobManager with id %s is no longer reachable.",
                    resourceID.getStringWithMetadata());
            log.info(message);

            handleJobManagerConnectionLoss(resourceID, new TaskManagerException(message));
        }

        private void handleJobManagerConnectionLoss(ResourceID resourceID,
                                                    Exception cause) {
            validateRunsInMainThread();
            jobTable.getConnection(resourceID)
                    .ifPresent(jobManagerConnection -> disconnectAndTryReconnectToJobManager(jobManagerConnection, cause));
        }

        @Override
        public void reportPayload(ResourceID resourceID,
                                  AllocatedSlotReport allocatedSlotReport) {
            validateRunsInMainThread();
            OptionalConsumer.of(jobTable.getConnection(allocatedSlotReport.getJobId())).ifPresent(jobManagerConnection -> {
                syncSlotsWithSnapshotFromJobMaster(jobManagerConnection.getJobManagerGateway(), allocatedSlotReport);
            }).ifNotPresent(() -> log.debug("Ignoring allocated slot report from job {} because there is no active leader.",
                    allocatedSlotReport.getJobId()));
        }

        @Override
        public TaskExecutorToJobManagerHeartbeatPayload retrievePayload(ResourceID resourceID) {
            validateRunsInMainThread();
            return jobTable.getConnection(resourceID).map(jobManagerConnection -> {
                JobID jobId = jobManagerConnection.getJobId();

                Set<ExecutionAttemptID> deployedExecutions = new HashSet<>();
                List<AccumulatorSnapshot> accumulatorSnapshots = new ArrayList<>(16);
                Iterator<Task> allTasks = taskSlotTable.getTasks(jobId);

                while (allTasks.hasNext()) {
                    Task task = allTasks.next();
                    deployedExecutions.add(task.getExecutionId());
                    accumulatorSnapshots.add(task.getAccumulatorRegistry().getSnapshot());
                }
                return new TaskExecutorToJobManagerHeartbeatPayload(new AccumulatorReport(accumulatorSnapshots),
                        new ExecutionDeploymentReport(deployedExecutions));
            }).orElseGet(TaskExecutorToJobManagerHeartbeatPayload::empty);
        }
    }

    private class ResourceManagerHeartbeatListener implements HeartbeatListener<Void, TaskExecutorHeartbeatPayload> {

        @Override
        public void notifyHeartbeatTimeout(final ResourceID resourceId) {
            final String message = String.format("The heartbeat of ResourceManager with id %s timed out.",
                    resourceId.getStringWithMetadata());

            log.info(message);
            handleResourceManagerConnectionLoss(resourceId, new TaskManagerException(message));
        }

        @Override
        public void notifyTargetUnreachable(ResourceID resourceID) {
            final String message = String.format("ResourceManager with id %s is no longer reachable.",
                    resourceID.getStringWithMetadata());
            log.info(message);

            handleResourceManagerConnectionLoss(resourceID, new TaskManagerException(message));
        }

        private void handleResourceManagerConnectionLoss(ResourceID resourceId,
                                                         TaskManagerException cause) {
            validateRunsInMainThread();

            // first check whether the timeout is still valid
            if (establishedResourceManagerConnection != null && establishedResourceManagerConnection.getResourceManagerResourceId()
                    .equals(resourceId)) {

                reconnectToResourceManager(cause);
            }
        }

        @Override
        public void reportPayload(ResourceID resourceID,
                                  Void payload) {
            // nothing to do since the payload is of type Void
        }

        @Override
        public TaskExecutorHeartbeatPayload retrievePayload(ResourceID resourceID) {
            validateRunsInMainThread();
            return new TaskExecutorHeartbeatPayload(taskSlotTable.createSlotReport(getResourceID()),
                    partitionTracker.createClusterPartitionReport());
        }
    }

    @VisibleForTesting
    static final class TaskExecutorJobServices implements JobTable.JobServices {

        private final LibraryCacheManager.ClassLoaderLease classLoaderLease;

        private final Runnable closeHook;

        private TaskExecutorJobServices(LibraryCacheManager.ClassLoaderLease classLoaderLease,
                                        Runnable closeHook) {
            this.classLoaderLease = classLoaderLease;
            this.closeHook = closeHook;
        }

        @Override
        public LibraryCacheManager.ClassLoaderHandle getClassLoaderHandle() {
            return classLoaderLease;
        }

        @Override
        public void close() {
            classLoaderLease.release();
            closeHook.run();
        }

        @VisibleForTesting
        static TaskExecutorJobServices create(LibraryCacheManager.ClassLoaderLease classLoaderLease,
                                              Runnable closeHook) {
            return new TaskExecutorJobServices(classLoaderLease, closeHook);
        }
    }
}
