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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JMXServerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.TaskManagerOptionsInternal;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.core.security.FlinkSecurityManager;
import org.apache.flink.management.jmx.JMXService;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterEntrypointUtils;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.externalresource.ExternalResourceUtils;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionTrackerImpl;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalException;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.ReporterSetup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.util.MetricUtils;
import org.apache.flink.runtime.rpc.AddressResolution;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.rpc.RpcSystemUtils;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.state.changelog.StateChangelogStorageLoader;
import org.apache.flink.runtime.taskmanager.MemoryLogger;
import org.apache.flink.runtime.util.ConfigurationParserUtils;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.TaskManagerExceptionUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class is the executable entry point for the task manager in yarn or standalone mode. It
 * constructs the related components (network, I/O manager, memory manager, RPC service, HA service)
 * and starts them.
 */
public class TaskManagerRunner implements FatalErrorHandler {

    private static final Logger LOG = LoggerFactory.getLogger(TaskManagerRunner.class);

    private static final long FATAL_ERROR_SHUTDOWN_TIMEOUT_MS = 10000L;

    private static final int SUCCESS_EXIT_CODE = 0;
    @VisibleForTesting
    static final int FAILURE_EXIT_CODE = 1;

    private final Object lock = new Object();

    private final Configuration configuration;

    // TODO_MA 马中华 注释：
    private final ResourceID resourceId;

    private final Time timeout;

    private final RpcService rpcService;

    private final HighAvailabilityServices highAvailabilityServices;

    private final MetricRegistryImpl metricRegistry;

    private final BlobCacheService blobCacheService;

    /** Executor used to run future callbacks. */
    private final ExecutorService executor;

    private final TaskExecutorService taskExecutorService;

    private final CompletableFuture<Result> terminationFuture;

    private final RpcSystem rpcSystem;

    private boolean shutdown;

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：
     */
    public TaskManagerRunner(Configuration configuration,
                             PluginManager pluginManager,
                             TaskExecutorServiceFactory taskExecutorServiceFactory) throws Exception {
        this.configuration = checkNotNull(configuration);

        rpcSystem = RpcSystem.load(configuration);

        timeout = Time.fromDuration(configuration.get(AkkaOptions.ASK_TIMEOUT_DURATION));

        this.executor = java.util.concurrent.Executors.newScheduledThreadPool(Hardware.getNumberCPUCores(),
                new ExecutorThreadFactory("taskmanager-future")
        );

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： ZooKeeperHaServices
         */
        highAvailabilityServices = HighAvailabilityServicesUtils.createHighAvailabilityServices(configuration,
                executor,
                AddressResolution.NO_ADDRESS_RESOLUTION,
                rpcSystem,
                this
        );

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： JMX 服务
         */
        JMXService.startInstance(configuration.getString(JMXServerOptions.JMX_SERVER_PORT));

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 3 = RPC 服务
         */
        rpcService = createRpcService(configuration, highAvailabilityServices, rpcSystem);

        // TODO_MA 马中华 注释： 给 TaskExecutor 生成一个 ResourceID
        this.resourceId = getTaskManagerResourceID(configuration, rpcService.getAddress(), rpcService.getPort());

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 4= HeartBeat 服务
         */
        HeartbeatServices heartbeatServices = HeartbeatServices.fromConfiguration(configuration);

        // TODO_MA 马中华 注释： 5 = metric 性能监控服务
        metricRegistry = new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(configuration,
                rpcSystem.getMaximumMessageSizeInBytes(configuration)
        ), ReporterSetup.fromConfiguration(configuration, pluginManager));

        final RpcService metricQueryServiceRpcService = MetricUtils.startRemoteMetricsRpcService(configuration,
                rpcService.getAddress(),
                rpcSystem
        );
        metricRegistry.startQueryService(metricQueryServiceRpcService, resourceId);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 6 = BlobCache 文件服务
         */
        blobCacheService = new BlobCacheService(configuration, highAvailabilityServices.createBlobStore(), null);

        final ExternalResourceInfoProvider externalResourceInfoProvider = ExternalResourceUtils.createStaticExternalResourceInfoProviderFromConfig(
                configuration,
                pluginManager
        );

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 重点： 创建 TaskExecutorService
         */
        taskExecutorService = taskExecutorServiceFactory.createTaskExecutor(this.configuration,
                this.resourceId,
                rpcService,
                highAvailabilityServices,
                heartbeatServices,
                metricRegistry,
                blobCacheService,
                false,
                externalResourceInfoProvider,
                this
        );

        this.terminationFuture = new CompletableFuture<>();
        this.shutdown = false;
        handleUnexpectedTaskExecutorServiceTermination();

        MemoryLogger.startIfConfigured(LOG, configuration, terminationFuture.thenAccept(ignored -> {
        }));
    }

    private void handleUnexpectedTaskExecutorServiceTermination() {
        taskExecutorService.getTerminationFuture().whenComplete((unused, throwable) -> {
            synchronized (lock) {
                if (!shutdown) {
                    onFatalError(new FlinkException("Unexpected termination of the TaskExecutor.", throwable));
                }
            }
        });
    }

    // --------------------------------------------------------------------------------------------
    //  Lifecycle management
    // --------------------------------------------------------------------------------------------

    public void start() throws Exception {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        taskExecutorService.start();
    }

    public void close() throws Exception {
        try {
            closeAsync().get();
        } catch (ExecutionException e) {
            ExceptionUtils.rethrowException(ExceptionUtils.stripExecutionException(e));
        }
    }

    public CompletableFuture<Result> closeAsync() {
        return closeAsync(Result.SUCCESS);
    }

    private CompletableFuture<Result> closeAsync(Result terminationResult) {
        synchronized (lock) {
            if (!shutdown) {
                shutdown = true;

                final CompletableFuture<Void> taskManagerTerminationFuture = taskExecutorService.closeAsync();

                final CompletableFuture<Void> serviceTerminationFuture = FutureUtils.composeAfterwards(
                        taskManagerTerminationFuture,
                        this::shutDownServices
                );

                final CompletableFuture<Void> rpcSystemClassLoaderCloseFuture = FutureUtils.runAfterwards(
                        serviceTerminationFuture,
                        rpcSystem::close
                );

                rpcSystemClassLoaderCloseFuture.whenComplete((Void ignored, Throwable throwable) -> {
                    if (throwable != null) {
                        terminationFuture.completeExceptionally(throwable);
                    } else {
                        terminationFuture.complete(terminationResult);
                    }
                });
            }
        }

        return terminationFuture;
    }

    private CompletableFuture<Void> shutDownServices() {
        synchronized (lock) {
            Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);
            Exception exception = null;

            try {
                JMXService.stopInstance();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            try {
                blobCacheService.close();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            try {
                metricRegistry.shutdown();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            try {
                highAvailabilityServices.close();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            terminationFutures.add(rpcService.stopService());

            terminationFutures.add(ExecutorUtils.nonBlockingShutdown(timeout.toMilliseconds(),
                    TimeUnit.MILLISECONDS,
                    executor
            ));

            if (exception != null) {
                terminationFutures.add(FutureUtils.completedExceptionally(exception));
            }

            return FutureUtils.completeAll(terminationFutures);
        }
    }

    // export the termination future for caller to know it is terminated
    public CompletableFuture<Result> getTerminationFuture() {
        return terminationFuture;
    }

    // --------------------------------------------------------------------------------------------
    //  FatalErrorHandler methods
    // --------------------------------------------------------------------------------------------

    @Override
    public void onFatalError(Throwable exception) {
        TaskManagerExceptionUtils.tryEnrichTaskManagerError(exception);
        LOG.error("Fatal error occurred while executing the TaskManager. Shutting it down...", exception);

        // In case of the Metaspace OutOfMemoryError, we expect that the graceful shutdown is
        // possible,
        // as it does not usually require more class loading to fail again with the Metaspace
        // OutOfMemoryError.
        if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(exception) && !ExceptionUtils.isMetaspaceOutOfMemoryError(
                exception)) {
            terminateJVM();
        } else {
            closeAsync(Result.FAILURE);

            FutureUtils.orTimeout(terminationFuture, FATAL_ERROR_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        }
    }

    private void terminateJVM() {
        FlinkSecurityManager.forceProcessExit(FAILURE_EXIT_CODE);
    }

    // --------------------------------------------------------------------------------------------
    //  Static entry point
    // --------------------------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        // startup checks and logging
        EnvironmentInformation.logEnvironmentInfo(LOG, "TaskManager", args);
        SignalHandler.register(LOG);
        JvmShutdownSafeguard.installAsShutdownHook(LOG);

        long maxOpenFileHandles = EnvironmentInformation.getOpenFileHandlesLimit();

        if (maxOpenFileHandles != -1L) {
            LOG.info("Maximum number of open file descriptors is {}.", maxOpenFileHandles);
        } else {
            LOG.info("Cannot determine the maximum number of open file descriptors");
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        runTaskManagerProcessSecurely(args);
    }

    public static Configuration loadConfiguration(String[] args) throws FlinkParseException {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return ConfigurationParserUtils.loadCommonConfiguration(args, TaskManagerRunner.class.getSimpleName());
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 里面两个重要工作：
     *  1、 创建 TaskExecutorService
     *  2、 启动 TaskExecutorService
     *  -
     *  创建和启动 TaskManagerRunner， 但是里面，其实是创建和启动 TaskExecutorService
     *  其实最本质的事情： 创建和启动 TaskExecutor
     *  1、TaskManagerRunner 逻辑上的从节点的概念，代表了从节点上的一切
     *  2、TaskExecutor 从节点上，最为重要的一个工作组件： 管理slot 和 接收 task执行
     *  3、TaskExecutorService(=TaskManagerServices) 这个东东西是对 TaskExecutor 的包装，其实内部除了 TaskExecutor 之外
     *  还有两个重要的组件： TaskManagerServices(从节点上的各种基础服务)   Configurationd
     *  -
     *  所有的重点，都可以总结为两句话：
     *  1、new TaskExecutor()
     *  2、TaskExecutor.onStart()
     *  -
     *  JobMaster = JobManager，  TaskExecutorService = TaskManagerServices
     *  称呼： JobManager， TaskManager， 实际的实现类： JobMaster，  TaskExecutor
     *  jobMaster = crateJobManager()
     *  TaskManagerServices = createTaskExecutorService()
     */
    public static int runTaskManager(Configuration configuration, PluginManager pluginManager) throws Exception {
        final TaskManagerRunner taskManagerRunner;

        try {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 第一步： 创建 11 TaskExecutorService
             */
            taskManagerRunner = new TaskManagerRunner(configuration, pluginManager,

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 创建了一个 22 TaskExecutorToServiceAdapter
                     */
                    TaskManagerRunner::createTaskExecutorService
                    // TODO_MA 马中华 注释： 33 其实内部要创建 TaskExecutor
            );

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 启动 TaskExecutorService
             */
            taskManagerRunner.start();
            // TODO_MA 马中华 注释： 1 = taskManagerRunner.start();
            // TODO_MA 马中华 注释： 2 = taskExecutorService.start()
            // TODO_MA 马中华 注释： 3 = TaskExecutor.start()

        } catch (Exception exception) {
            throw new FlinkException("Failed to start the TaskManagerRunner.", exception);
        }

        try {
            return taskManagerRunner.getTerminationFuture().get().getExitCode();
        } catch (Throwable t) {
            throw new FlinkException("Unexpected failure during runtime of TaskManagerRunner.",
                    ExceptionUtils.stripExecutionException(t)
            );
        }
    }

    public static void runTaskManagerProcessSecurely(String[] args) {
        Configuration configuration = null;

        try {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 1、加载配置： 解析 flink-conf.yaml
             */
            configuration = loadConfiguration(args);
        } catch (FlinkParseException fpe) {
            LOG.error("Could not load the configuration.", fpe);
            System.exit(FAILURE_EXIT_CODE);
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 2、启动 TaskManager
         */
        runTaskManagerProcessSecurely(checkNotNull(configuration));
    }

    public static void runTaskManagerProcessSecurely(Configuration configuration) {

        FlinkSecurityManager.setFromConfiguration(configuration);
        final PluginManager pluginManager = PluginUtils.createPluginManagerFromRootFolder(configuration);
        FileSystem.initialize(configuration, pluginManager);

        StateChangelogStorageLoader.initialize(pluginManager);

        int exitCode;
        Throwable throwable = null;

        ClusterEntrypointUtils.configureUncaughtExceptionHandler(configuration);
        try {
            SecurityUtils.install(new SecurityConfiguration(configuration));

            exitCode = SecurityUtils.getInstalledContext().runSecured(() ->

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 这个方法的唯一重点：  继续启动 TaskManager
                     */
                    runTaskManager(configuration, pluginManager));
        } catch (Throwable t) {
            throwable = ExceptionUtils.stripException(t, UndeclaredThrowableException.class);
            exitCode = FAILURE_EXIT_CODE;
        }

        if (throwable != null) {
            LOG.error("Terminating TaskManagerRunner with exit code {}.", exitCode, throwable);
        } else {
            LOG.info("Terminating TaskManagerRunner with exit code {}.", exitCode);
        }

        System.exit(exitCode);
    }

    // --------------------------------------------------------------------------------------------
    //  Static utilities
    // --------------------------------------------------------------------------------------------

    public static TaskExecutorService createTaskExecutorService(Configuration configuration,
                                                                ResourceID resourceID,
                                                                RpcService rpcService,
                                                                HighAvailabilityServices highAvailabilityServices,
                                                                HeartbeatServices heartbeatServices,
                                                                MetricRegistry metricRegistry,
                                                                BlobCacheService blobCacheService,
                                                                boolean localCommunicationOnly,
                                                                ExternalResourceInfoProvider externalResourceInfoProvider,
                                                                FatalErrorHandler fatalErrorHandler) throws Exception {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 创建 TaskExecutor
         *  1、TaskManager 逻辑从节点
         *  2、TaskExecutor 物理从节点
         */
        final TaskExecutor taskExecutor = startTaskManager(configuration,
                resourceID,
                rpcService,
                highAvailabilityServices,
                heartbeatServices,
                metricRegistry,
                blobCacheService,
                localCommunicationOnly,
                externalResourceInfoProvider,
                fatalErrorHandler
        );

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 开启 TaskExecutor 的生命周期
         */
        return TaskExecutorToServiceAdapter.createFor(taskExecutor);
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 这个方法的作用，就干两件事：
     *  1、 创建 TaskManagerServices
     *  2、 创建 TaskExecutor
     */
    public static TaskExecutor startTaskManager(Configuration configuration,
                                                ResourceID resourceID,
                                                RpcService rpcService,
                                                HighAvailabilityServices highAvailabilityServices,
                                                HeartbeatServices heartbeatServices,
                                                MetricRegistry metricRegistry,
                                                BlobCacheService blobCacheService,
                                                boolean localCommunicationOnly,
                                                ExternalResourceInfoProvider externalResourceInfoProvider,
                                                FatalErrorHandler fatalErrorHandler) throws Exception {

        checkNotNull(configuration);
        checkNotNull(resourceID);
        checkNotNull(rpcService);
        checkNotNull(highAvailabilityServices);

        LOG.info("Starting TaskManager with ResourceID: {}", resourceID.getStringWithMetadata());

        String externalAddress = rpcService.getAddress();

        // TODO_MA 马中华 注释： 资源配置
        final TaskExecutorResourceSpec taskExecutorResourceSpec = TaskExecutorResourceUtils.resourceSpecFromConfig(
                configuration);

        // TODO_MA 马中华 注释： 配置信息
        TaskManagerServicesConfiguration taskManagerServicesConfiguration = TaskManagerServicesConfiguration.fromConfiguration(
                configuration,
                resourceID,
                externalAddress,
                localCommunicationOnly,
                taskExecutorResourceSpec
        );

        Tuple2<TaskManagerMetricGroup, MetricGroup> taskManagerMetricGroup = MetricUtils.instantiateTaskManagerMetricGroup(
                metricRegistry,
                externalAddress,
                resourceID,
                taskManagerServicesConfiguration.getSystemResourceMetricsProbingInterval()
        );

        // TODO_MA 马中华 注释： IO 线程池
        final ExecutorService ioExecutor = Executors.newFixedThreadPool(taskManagerServicesConfiguration.getNumIoThreads(),
                new ExecutorThreadFactory("flink-taskexecutor-io")
        );

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 第一个重点： TaskExecutor 中的各种基础服务
         *  把从节点的所有相关的东西分为三个部分：
         *  1、基础公共服务 （rpc,心跳,ha服务,）
         *  2、核心服务 TaskManagerServices （IO服务，shuffle服务，内存管理，....）
         *  3、TaskExecutor  （真正干活的）
         */
        TaskManagerServices taskManagerServices = TaskManagerServices.fromConfiguration(taskManagerServicesConfiguration,
                blobCacheService.getPermanentBlobService(),
                taskManagerMetricGroup.f1,
                ioExecutor,
                fatalErrorHandler
        );

        MetricUtils.instantiateFlinkMemoryMetricGroup(taskManagerMetricGroup.f1,
                taskManagerServices.getTaskSlotTable(),
                taskManagerServices::getManagedMemorySize
        );

        // TODO_MA 马中华 注释： TaskManager 各种配置
        TaskManagerConfiguration taskManagerConfiguration = TaskManagerConfiguration.fromConfiguration(configuration,
                taskExecutorResourceSpec,
                externalAddress
        );

        String metricQueryServiceAddress = metricRegistry.getMetricQueryServiceGatewayRpcAddress();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 第二个重点： 创建 TaskExecutor
         */
        return new TaskExecutor(rpcService,
                taskManagerConfiguration,
                highAvailabilityServices,
                taskManagerServices,
                externalResourceInfoProvider,
                heartbeatServices,
                taskManagerMetricGroup.f0,
                metricQueryServiceAddress,
                blobCacheService,
                fatalErrorHandler,
                new TaskExecutorPartitionTrackerImpl(taskManagerServices.getShuffleEnvironment())
        );

        // TODO_MA 马中华 注释： 接下来就是 TaskExecutor 启动
    }

    /**
     * Create a RPC service for the task manager.
     *
     * @param configuration The configuration for the TaskManager.
     * @param haServices to use for the task manager hostname retrieval
     */
    @VisibleForTesting
    static RpcService createRpcService(final Configuration configuration,
                                       final HighAvailabilityServices haServices,
                                       final RpcSystem rpcSystem) throws Exception {

        checkNotNull(configuration);
        checkNotNull(haServices);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return RpcUtils.createRemoteRpcService(rpcSystem,
                configuration,
                determineTaskManagerBindAddress(configuration, haServices, rpcSystem),
                configuration.getString(TaskManagerOptions.RPC_PORT),
                configuration.getString(TaskManagerOptions.BIND_HOST),
                configuration.getOptional(TaskManagerOptions.RPC_BIND_PORT)
        );
    }

    private static String determineTaskManagerBindAddress(final Configuration configuration,
                                                          final HighAvailabilityServices haServices,
                                                          RpcSystemUtils rpcSystemUtils) throws Exception {

        final String configuredTaskManagerHostname = configuration.getString(TaskManagerOptions.HOST);

        if (configuredTaskManagerHostname != null) {
            LOG.info("Using configured hostname/address for TaskManager: {}.", configuredTaskManagerHostname);
            return configuredTaskManagerHostname;
        } else {
            return determineTaskManagerBindAddressByConnectingToResourceManager(configuration,
                    haServices,
                    rpcSystemUtils
            );
        }
    }

    private static String determineTaskManagerBindAddressByConnectingToResourceManager(final Configuration configuration,
                                                                                       final HighAvailabilityServices haServices,
                                                                                       RpcSystemUtils rpcSystemUtils) throws LeaderRetrievalException {

        final Duration lookupTimeout = configuration.get(AkkaOptions.LOOKUP_TIMEOUT_DURATION);

        final InetAddress taskManagerAddress = LeaderRetrievalUtils.findConnectingAddress(haServices.getResourceManagerLeaderRetriever(),
                lookupTimeout,
                rpcSystemUtils
        );

        LOG.info("TaskManager will use hostname/address '{}' ({}) for communication.",
                taskManagerAddress.getHostName(),
                taskManagerAddress.getHostAddress()
        );

        HostBindPolicy bindPolicy = HostBindPolicy.fromString(configuration.getString(TaskManagerOptions.HOST_BIND_POLICY));
        return bindPolicy == HostBindPolicy.IP ? taskManagerAddress.getHostAddress() : taskManagerAddress.getHostName();
    }

    @VisibleForTesting
    static ResourceID getTaskManagerResourceID(Configuration config, String rpcAddress, int rpcPort) throws Exception {
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return new ResourceID(config.getString(TaskManagerOptions.TASK_MANAGER_RESOURCE_ID,
                StringUtils.isNullOrWhitespaceOnly(rpcAddress) ?
                        InetAddress.getLocalHost().getHostName() + "-" + new AbstractID().toString().substring(0, 6) :
                        rpcAddress + ":" + rpcPort + "-" + new AbstractID().toString().substring(0, 6)
        ), config.getString(TaskManagerOptionsInternal.TASK_MANAGER_RESOURCE_ID_METADATA, ""));
    }

    /** Factory for {@link TaskExecutor}. */
    public interface TaskExecutorServiceFactory {
        TaskExecutorService createTaskExecutor(Configuration configuration,
                                               ResourceID resourceID,
                                               RpcService rpcService,
                                               HighAvailabilityServices highAvailabilityServices,
                                               HeartbeatServices heartbeatServices,
                                               MetricRegistry metricRegistry,
                                               BlobCacheService blobCacheService,
                                               boolean localCommunicationOnly,
                                               ExternalResourceInfoProvider externalResourceInfoProvider,
                                               FatalErrorHandler fatalErrorHandler) throws Exception;
    }

    public interface TaskExecutorService extends AutoCloseableAsync {
        void start();

        CompletableFuture<Void> getTerminationFuture();
    }

    public enum Result {
        SUCCESS(SUCCESS_EXIT_CODE),
        FAILURE(FAILURE_EXIT_CODE);

        private final int exitCode;

        Result(int exitCode) {
            this.exitCode = exitCode;
        }

        public int getExitCode() {
            return exitCode;
        }
    }
}
