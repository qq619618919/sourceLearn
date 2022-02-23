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

package org.apache.flink.yarn;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.TaskManagerOptionsInternal;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.externalresource.ExternalResourceUtils;
import org.apache.flink.runtime.resourcemanager.active.AbstractResourceManagerDriver;
import org.apache.flink.runtime.resourcemanager.active.ResourceManagerDriver;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.webmonitor.history.HistoryServerUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnResourceManagerDriverConfiguration;

import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/** Implementation of {@link ResourceManagerDriver} for Yarn deployment. */
public class YarnResourceManagerDriver extends AbstractResourceManagerDriver<YarnWorkerNode> {

    /**
     * Environment variable name of the hostname given by YARN. In task executor we use the
     * hostnames given by YARN consistently throughout akka
     */
    static final String ENV_FLINK_NODE_ID = "_FLINK_NODE_ID";

    static final String ERROR_MESSAGE_ON_SHUTDOWN_REQUEST = "Received shutdown request from YARN ResourceManager.";

    private final YarnConfiguration yarnConfig;

    /** The process environment variables. */
    private final YarnResourceManagerDriverConfiguration configuration;

    /** Default heartbeat interval between this resource manager and the YARN ResourceManager. */
    private final int yarnHeartbeatIntervalMillis;

    /** The heartbeat interval while the resource master is waiting for containers. */
    private final int containerRequestHeartbeatIntervalMillis;

    /** Request resource futures, keyed by container's TaskExecutorProcessSpec. */
    private final Map<TaskExecutorProcessSpec, Queue<CompletableFuture<YarnWorkerNode>>> requestResourceFutures;

    private final RegisterApplicationMasterResponseReflector registerApplicationMasterResponseReflector;

    private final YarnResourceManagerClientFactory yarnResourceManagerClientFactory;

    private final YarnNodeManagerClientFactory yarnNodeManagerClientFactory;

    /** Client to communicate with the Resource Manager (YARN's master). */
    private AMRMClientAsync<AMRMClient.ContainerRequest> resourceManagerClient;

    /** Client to communicate with the Node manager and launch TaskExecutor processes. */
    private NMClientAsync nodeManagerClient;

    private TaskExecutorProcessSpecContainerResourcePriorityAdapter taskExecutorProcessSpecContainerResourcePriorityAdapter;

    public YarnResourceManagerDriver(Configuration flinkConfig,
                                     YarnResourceManagerDriverConfiguration configuration,
                                     YarnResourceManagerClientFactory yarnResourceManagerClientFactory,
                                     YarnNodeManagerClientFactory yarnNodeManagerClientFactory) {

        // TODO_MA 马中华 注释： 加载配置
        super(flinkConfig, GlobalConfiguration.loadConfiguration(configuration.getCurrentDir()));

        this.yarnConfig = Utils.getYarnAndHadoopConfiguration(flinkConfig);
        this.requestResourceFutures = new HashMap<>();
        this.configuration = configuration;

        final int yarnHeartbeatIntervalMS = flinkConfig.getInteger(YarnConfigOptions.HEARTBEAT_DELAY_SECONDS) * 1000;

        final long yarnExpiryIntervalMS = yarnConfig.getLong(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS,
                YarnConfiguration.DEFAULT_RM_AM_EXPIRY_INTERVAL_MS
        );

        if (yarnHeartbeatIntervalMS >= yarnExpiryIntervalMS) {
            log.warn("The heartbeat interval of the Flink Application master ({}) is greater "
                            + "than YARN's expiry interval ({}). The application is likely to be killed by YARN.",
                    yarnHeartbeatIntervalMS,
                    yarnExpiryIntervalMS
            );
        }
        yarnHeartbeatIntervalMillis = yarnHeartbeatIntervalMS;
        containerRequestHeartbeatIntervalMillis = flinkConfig.getInteger(YarnConfigOptions.CONTAINER_REQUEST_HEARTBEAT_INTERVAL_MILLISECONDS);

        this.registerApplicationMasterResponseReflector = new RegisterApplicationMasterResponseReflector(log);

        this.yarnResourceManagerClientFactory = yarnResourceManagerClientFactory;
        this.yarnNodeManagerClientFactory = yarnNodeManagerClientFactory;
    }

    // ------------------------------------------------------------------------
    //  ResourceManagerDriver
    // ------------------------------------------------------------------------

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： YARN 中提供了一个客户端组件： AMRMClientAsyncImpl， 其实内部包含了一个 YarnClient
     *  1、 初始化和启动
     *  2、 registerApplicationMaster() 启动 ApplicationMaster
     *  3、 getContainersFromPreviousAttempts() 需要这个 YarnContainerEventHandler
     *  -
     *  首先需要构造一个客户端，然后初始化和启动
     *  注册 AppliationMaster 同时也申请 Container
     *  然后用来启动 JObManager 和 TaskManager
     *  -
     *  从 YARN 中申请到了 Contianer 用来启动 fLINK sesssion 集群：
     *  1、一个 Contianer 用来启动 JObManager
     *  2、一堆其他 Contianer 用来 TaskManager
     */
    @Override
    protected void initializeInternal() throws Exception {

        // TODO_MA 马中华 注释： Container 相关的 EventHandler
        // TODO_MA 马中华 注释：
        final YarnContainerEventHandler yarnContainerEventHandler = new YarnContainerEventHandler();
        try {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 创建链接 YARN 的 RM 的客户端
             *  AMRMClientAsyncImpl 的创建，初始化 和 启动
             *  AMRMClientAsyncImpl = Service
             */
            resourceManagerClient = yarnResourceManagerClientFactory.createResourceManagerClient(
                    yarnHeartbeatIntervalMillis,
                    yarnContainerEventHandler
            );
            resourceManagerClient.init(yarnConfig);     // TODO_MA 马中华 注释： serviceInit()
            resourceManagerClient.start();              // TODO_MA 马中华 注释： serviceStart()

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 注册 ApplicationMaster，其实也包含 Container 申请在里面
             *  发送 RPC 请求给 YARN 的 RM
             */
            final RegisterApplicationMasterResponse registerApplicationMasterResponse = registerApplicationMaster();

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 处理 Container 申请的响应，然后如果申请到了 Container 则用来启动从节点 TaskManagaer
             */
            getContainersFromPreviousAttempts(registerApplicationMasterResponse);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            taskExecutorProcessSpecContainerResourcePriorityAdapter = new TaskExecutorProcessSpecContainerResourcePriorityAdapter(
                    registerApplicationMasterResponse.getMaximumResourceCapability(),
                    ExternalResourceUtils.getExternalResourceConfigurationKeys(flinkConfig,
                            YarnConfigOptions.EXTERNAL_RESOURCE_YARN_CONFIG_KEY_SUFFIX
                    )
            );
        } catch (Exception e) {
            throw new ResourceManagerException("Could not start resource manager client.", e);
        }

        nodeManagerClient = yarnNodeManagerClientFactory.createNodeManagerClient(yarnContainerEventHandler);
        nodeManagerClient.init(yarnConfig);
        nodeManagerClient.start();
    }

    @Override
    public void terminate() throws Exception {
        // shut down all components
        Exception exception = null;

        if (resourceManagerClient != null) {
            try {
                resourceManagerClient.stop();
            } catch (Exception e) {
                exception = e;
            }
        }

        if (nodeManagerClient != null) {
            try {
                nodeManagerClient.stop();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }
        }

        if (exception != null) {
            throw exception;
        }
    }

    @Override
    public void deregisterApplication(ApplicationStatus finalStatus, @Nullable String optionalDiagnostics) {
        // first, de-register from YARN
        final FinalApplicationStatus yarnStatus = getYarnStatus(finalStatus);
        log.info("Unregister application from the YARN Resource Manager with final status {}.", yarnStatus);

        final Optional<URL> historyServerURL = HistoryServerUtils.getHistoryServerURL(flinkConfig);

        final String appTrackingUrl = historyServerURL.map(URL::toString).orElse("");

        try {
            resourceManagerClient.unregisterApplicationMaster(yarnStatus, optionalDiagnostics, appTrackingUrl);
        } catch (YarnException | IOException e) {
            log.error("Could not unregister the application master.", e);
        }

        Utils.deleteApplicationFiles(configuration.getYarnFiles());
    }

    @Override
    public CompletableFuture<YarnWorkerNode> requestResource(TaskExecutorProcessSpec taskExecutorProcessSpec) {
        checkInitialized();

        final CompletableFuture<YarnWorkerNode> requestResourceFuture = new CompletableFuture<>();

        final Optional<TaskExecutorProcessSpecContainerResourcePriorityAdapter.PriorityAndResource> priorityAndResourceOpt = taskExecutorProcessSpecContainerResourcePriorityAdapter.getPriorityAndResource(
                taskExecutorProcessSpec);

        if (!priorityAndResourceOpt.isPresent()) {
            requestResourceFuture.completeExceptionally(new ResourceManagerException(String.format(
                    "Could not compute the container Resource from the given TaskExecutorProcessSpec %s. "
                            + "This usually indicates the requested resource is larger than Yarn's max container resource limit.",
                    taskExecutorProcessSpec
            )));
        } else {
            final Priority priority = priorityAndResourceOpt.get().getPriority();
            final Resource resource = priorityAndResourceOpt.get().getResource();
            resourceManagerClient.addContainerRequest(getContainerRequest(resource, priority));

            // make sure we transmit the request fast and receive fast news of granted allocations
            resourceManagerClient.setHeartbeatInterval(containerRequestHeartbeatIntervalMillis);

            requestResourceFutures
                    .computeIfAbsent(taskExecutorProcessSpec, ignore -> new LinkedList<>())
                    .add(requestResourceFuture);

            log.info("Requesting new TaskExecutor container with resource {}, priority {}.",
                    taskExecutorProcessSpec,
                    priority
            );
        }

        return requestResourceFuture;
    }

    @Override
    public void releaseResource(YarnWorkerNode workerNode) {
        final Container container = workerNode.getContainer();
        log.info("Stopping container {}.", workerNode.getResourceID().getStringWithMetadata());
        nodeManagerClient.stopContainerAsync(container.getId(), container.getNodeId());
        resourceManagerClient.releaseAssignedContainer(container.getId());
    }

    // ------------------------------------------------------------------------
    //  Internal
    // ------------------------------------------------------------------------

    private void onContainersOfPriorityAllocated(Priority priority, List<Container> containers) {
        final Optional<TaskExecutorProcessSpecContainerResourcePriorityAdapter.TaskExecutorProcessSpecAndResource> taskExecutorProcessSpecAndResourceOpt = taskExecutorProcessSpecContainerResourcePriorityAdapter.getTaskExecutorProcessSpecAndResource(
                priority);

        Preconditions.checkState(taskExecutorProcessSpecAndResourceOpt.isPresent(),
                "Receive %s containers with unrecognized priority %s. This should not happen.",
                containers.size(),
                priority.getPriority()
        );

        final TaskExecutorProcessSpec taskExecutorProcessSpec = taskExecutorProcessSpecAndResourceOpt
                .get()
                .getTaskExecutorProcessSpec();
        final Resource resource = taskExecutorProcessSpecAndResourceOpt.get().getResource();

        final Queue<CompletableFuture<YarnWorkerNode>> pendingRequestResourceFutures = requestResourceFutures.getOrDefault(
                taskExecutorProcessSpec,
                new LinkedList<>()
        );

        log.info("Received {} containers with priority {}, {} pending container requests.",
                containers.size(),
                priority,
                pendingRequestResourceFutures.size()
        );

        final Iterator<Container> containerIterator = containers.iterator();
        final Iterator<AMRMClient.ContainerRequest> pendingContainerRequestIterator = getPendingRequestsAndCheckConsistency(
                priority,
                resource,
                pendingRequestResourceFutures.size()
        ).iterator();

        int numAccepted = 0;

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 遍历 每个申请到 Container
         */
        while (containerIterator.hasNext() && pendingContainerRequestIterator.hasNext()) {
            final Container container = containerIterator.next();
            final AMRMClient.ContainerRequest pendingRequest = pendingContainerRequestIterator.next();
            final ResourceID resourceId = getContainerResourceId(container);

            final CompletableFuture<YarnWorkerNode> requestResourceFuture = pendingRequestResourceFutures.poll();
            Preconditions.checkState(requestResourceFuture != null);

            if (pendingRequestResourceFutures.isEmpty()) {
                requestResourceFutures.remove(taskExecutorProcessSpec);
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 去启动 TaskExecutor 在 Container 里面
             */
            startTaskExecutorInContainerAsync(container, taskExecutorProcessSpec, resourceId, requestResourceFuture);
            removeContainerRequest(pendingRequest);

            numAccepted++;
        }

        int numExcess = 0;
        while (containerIterator.hasNext()) {
            returnExcessContainer(containerIterator.next());
            numExcess++;
        }

        log.info(
                "Accepted {} requested containers, returned {} excess containers, {} pending container requests of resource {}.",
                numAccepted,
                numExcess,
                pendingRequestResourceFutures.size(),
                resource
        );
    }

    private int getNumRequestedNotAllocatedWorkers() {
        return requestResourceFutures.values().stream().mapToInt(Queue::size).sum();
    }

    private void removeContainerRequest(AMRMClient.ContainerRequest pendingContainerRequest) {
        log.info("Removing container request {}.", pendingContainerRequest);
        resourceManagerClient.removeContainerRequest(pendingContainerRequest);
    }

    private void returnExcessContainer(Container excessContainer) {
        log.info("Returning excess container {}.", excessContainer.getId());
        resourceManagerClient.releaseAssignedContainer(excessContainer.getId());
    }

    private void startTaskExecutorInContainerAsync(Container container,
                                                   TaskExecutorProcessSpec taskExecutorProcessSpec,
                                                   ResourceID resourceId,
                                                   CompletableFuture<YarnWorkerNode> requestResourceFuture) {
        final CompletableFuture<ContainerLaunchContext> containerLaunchContextFuture = FutureUtils.supplyAsync(() -> createTaskExecutorLaunchContext(
                resourceId,
                container.getNodeId().getHost(),
                taskExecutorProcessSpec
        ), getIoExecutor());

        FutureUtils.assertNoException(containerLaunchContextFuture.handleAsync((context, exception) -> {
            if (exception == null) {

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释：
                 */
                nodeManagerClient.startContainerAsync(container, context);

                requestResourceFuture.complete(new YarnWorkerNode(container, resourceId));
            } else {
                requestResourceFuture.completeExceptionally(exception);
            }
            return null;
        }, getMainThreadExecutor()));
    }

    private Collection<AMRMClient.ContainerRequest> getPendingRequestsAndCheckConsistency(Priority priority,
                                                                                          Resource resource,
                                                                                          int expectedNum) {
        final List<AMRMClient.ContainerRequest> matchingRequests = resourceManagerClient
                .getMatchingRequests(priority, ResourceRequest.ANY, resource)
                .stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        Preconditions.checkState(matchingRequests.size() == expectedNum,
                "The RMClient's and YarnResourceManagers internal state about the number of pending container requests for priority %s has diverged. "
                        + "Number client's pending container requests %s != Number RM's pending container requests %s.",
                priority.getPriority(),
                matchingRequests.size(),
                expectedNum
        );

        return matchingRequests;
    }

    private ContainerLaunchContext createTaskExecutorLaunchContext(ResourceID containerId,
                                                                   String host,
                                                                   TaskExecutorProcessSpec taskExecutorProcessSpec) throws Exception {

        // init the ContainerLaunchContext
        final String currDir = configuration.getCurrentDir();

        final ContaineredTaskManagerParameters taskManagerParameters = ContaineredTaskManagerParameters.create(
                flinkConfig,
                taskExecutorProcessSpec
        );

        log.info("TaskExecutor {} will be started on {} with {}.",
                containerId.getStringWithMetadata(),
                host,
                taskExecutorProcessSpec
        );

        final Configuration taskManagerConfig = BootstrapTools.cloneConfiguration(flinkConfig);
        taskManagerConfig.set(TaskManagerOptions.TASK_MANAGER_RESOURCE_ID, containerId.getResourceIdString());
        taskManagerConfig.set(TaskManagerOptionsInternal.TASK_MANAGER_RESOURCE_ID_METADATA, containerId.getMetadata());

        final String taskManagerDynamicProperties = BootstrapTools.getDynamicPropertiesAsString(flinkClientConfig,
                taskManagerConfig
        );

        log.debug("TaskManager configuration: {}", taskManagerConfig);

        final ContainerLaunchContext taskExecutorLaunchContext = Utils.createTaskExecutorContext(flinkConfig,
                yarnConfig,
                configuration,
                taskManagerParameters,
                taskManagerDynamicProperties,
                currDir,
                YarnTaskExecutorRunner.class,
                log
        );

        taskExecutorLaunchContext.getEnvironment().put(ENV_FLINK_NODE_ID, host);
        return taskExecutorLaunchContext;
    }

    @VisibleForTesting
    Optional<Resource> getContainerResource(TaskExecutorProcessSpec taskExecutorProcessSpec) {
        Optional<TaskExecutorProcessSpecContainerResourcePriorityAdapter.PriorityAndResource> opt = taskExecutorProcessSpecContainerResourcePriorityAdapter.getPriorityAndResource(
                taskExecutorProcessSpec);

        if (!opt.isPresent()) {
            return Optional.empty();
        }

        return Optional.of(opt.get().getResource());
    }

    private RegisterApplicationMasterResponse registerApplicationMaster() throws Exception {
        final int restPort;
        final String webInterfaceUrl = configuration.getWebInterfaceUrl();
        final String rpcAddress = configuration.getRpcAddress();

        if (webInterfaceUrl != null) {
            final int lastColon = webInterfaceUrl.lastIndexOf(':');

            if (lastColon == -1) {
                restPort = -1;
            } else {
                restPort = Integer.parseInt(webInterfaceUrl.substring(lastColon + 1));
            }
        } else {
            restPort = -1;
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return resourceManagerClient.registerApplicationMaster(rpcAddress, restPort, webInterfaceUrl);
    }

    private void getContainersFromPreviousAttempts(final RegisterApplicationMasterResponse registerApplicationMasterResponse) {

        // TODO_MA 马中华 注释：
        final List<Container> containersFromPreviousAttempts = registerApplicationMasterResponseReflector.getContainersFromPreviousAttempts(
                registerApplicationMasterResponse);

        // TODO_MA 马中华 注释： 结果容器
        final List<YarnWorkerNode> recoveredWorkers = new ArrayList<>();

        log.info("Recovered {} containers from previous attempts ({}).",
                containersFromPreviousAttempts.size(),
                containersFromPreviousAttempts
        );

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        for (Container container : containersFromPreviousAttempts) {
            final YarnWorkerNode worker = new YarnWorkerNode(container, getContainerResourceId(container));
            recoveredWorkers.add(worker);
        }

        // Should not invoke resource event handler on the main thread executor.
        // We are in the initializing thread. The main thread executor is not yet ready.
        getResourceEventHandler().onPreviousAttemptWorkersRecovered(recoveredWorkers);
    }

    // ------------------------------------------------------------------------
    //  Utility methods
    // ------------------------------------------------------------------------

    /**
     * Converts a Flink application status enum to a YARN application status enum.
     *
     * @param status The Flink application status.
     *
     * @return The corresponding YARN application status.
     */
    private FinalApplicationStatus getYarnStatus(ApplicationStatus status) {
        if (status == null) {
            return FinalApplicationStatus.UNDEFINED;
        } else {
            switch (status) {
                case SUCCEEDED:
                    return FinalApplicationStatus.SUCCEEDED;
                case FAILED:
                    return FinalApplicationStatus.FAILED;
                case CANCELED:
                    return FinalApplicationStatus.KILLED;
                default:
                    return FinalApplicationStatus.UNDEFINED;
            }
        }
    }

    @Nonnull
    @VisibleForTesting
    static AMRMClient.ContainerRequest getContainerRequest(Resource containerResource, Priority priority) {
        return new AMRMClient.ContainerRequest(containerResource, null, null, priority);
    }

    @VisibleForTesting
    private static ResourceID getContainerResourceId(Container container) {
        return new ResourceID(container.getId().toString(), container.getNodeId().toString());
    }

    private Map<Priority, List<Container>> groupContainerByPriority(List<Container> containers) {
        return containers.stream().collect(Collectors.groupingBy(Container::getPriority));
    }

    private void checkInitialized() {
        Preconditions.checkState(taskExecutorProcessSpecContainerResourcePriorityAdapter != null,
                "Driver not initialized."
        );
    }

    // ------------------------------------------------------------------------
    //  Event handlers
    // ------------------------------------------------------------------------

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：
     */
    class YarnContainerEventHandler implements AMRMClientAsync.CallbackHandler, NMClientAsync.CallbackHandler {

        @Override
        public void onContainersCompleted(List<ContainerStatus> statuses) {
            runAsyncWithFatalHandler(() -> {
                checkInitialized();
                log.debug("YARN ResourceManager reported the following containers completed: {}.", statuses);
                for (final ContainerStatus containerStatus : statuses) {

                    final String containerId = containerStatus.getContainerId().toString();
                    getResourceEventHandler().onWorkerTerminated(new ResourceID(containerId),
                            getContainerCompletedCause(containerStatus)
                    );
                }
            });
        }

        @Override
        public void onContainersAllocated(List<Container> containers) {
            runAsyncWithFatalHandler(() -> {
                checkInitialized();
                log.info("Received {} containers.", containers.size());

                for (Map.Entry<Priority, List<Container>> entry : groupContainerByPriority(containers).entrySet()) {
                    onContainersOfPriorityAllocated(entry.getKey(), entry.getValue());
                }

                // if we are waiting for no further containers, we can go to the
                // regular heartbeat interval
                if (getNumRequestedNotAllocatedWorkers() <= 0) {
                    resourceManagerClient.setHeartbeatInterval(yarnHeartbeatIntervalMillis);
                }
            });
        }

        private void runAsyncWithFatalHandler(Runnable runnable) {
            getMainThreadExecutor().execute(() -> {
                try {
                    runnable.run();
                } catch (Throwable t) {
                    onError(t);
                }
            });
        }

        @Override
        public void onShutdownRequest() {
            getResourceEventHandler().onError(new ResourceManagerException(ERROR_MESSAGE_ON_SHUTDOWN_REQUEST));
        }

        @Override
        public void onNodesUpdated(List<NodeReport> list) {
            // We are not interested in node updates
        }

        @Override
        public float getProgress() {
            // Temporarily need not record the total size of asked and allocated containers
            return 1;
        }

        @Override
        public void onError(Throwable throwable) {
            getResourceEventHandler().onError(throwable);
        }

        @Override
        public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> map) {
            log.debug("Succeeded to call YARN Node Manager to start container {}.", containerId);
        }

        @Override
        public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
            // We are not interested in getting container status
        }

        @Override
        public void onContainerStopped(ContainerId containerId) {
            log.debug("Succeeded to call YARN Node Manager to stop container {}.", containerId);
        }

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable throwable) {
            runAsyncWithFatalHandler(() -> {
                resourceManagerClient.releaseAssignedContainer(containerId);
                getResourceEventHandler().onWorkerTerminated(new ResourceID(containerId.toString()),
                        throwable.getMessage()
                );
            });
        }

        @Override
        public void onGetContainerStatusError(ContainerId containerId, Throwable throwable) {
            // We are not interested in getting container status
        }

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable throwable) {
            log.warn("Error while calling YARN Node Manager to stop container {}.", containerId, throwable);
        }
    }

    public static String getContainerCompletedCause(ContainerStatus containerStatus) {
        final String completeContainerMessage;
        switch (containerStatus.getExitStatus()) {
            case ContainerExitStatus.SUCCESS:
                completeContainerMessage = String.format("Container %s exited normally. Diagnostics: %s",
                        containerStatus.getContainerId().toString(),
                        containerStatus.getDiagnostics()
                );
                break;
            case ContainerExitStatus.PREEMPTED:
                completeContainerMessage = String.format("Container %s was preempted by yarn. Diagnostics: %s",
                        containerStatus.getContainerId().toString(),
                        containerStatus.getDiagnostics()
                );
                break;
            case ContainerExitStatus.INVALID:
                completeContainerMessage = String.format("Container %s was invalid. Diagnostics: %s",
                        containerStatus.getContainerId().toString(),
                        containerStatus.getDiagnostics()
                );
                break;
            case ContainerExitStatus.ABORTED:
                completeContainerMessage = String.format(
                        "Container %s killed by YARN, either due to being released by the application or being 'lost' due to node failures etc. Diagnostics: %s",
                        containerStatus.getContainerId().toString(),
                        containerStatus.getDiagnostics()
                );
                break;
            case ContainerExitStatus.DISKS_FAILED:
                completeContainerMessage = String.format(
                        "Container %s is failed because threshold number of the nodemanager-local-directories or"
                                + " threshold number of the nodemanager-log-directories have become bad. Diagnostics: %s",
                        containerStatus.getContainerId().toString(),
                        containerStatus.getDiagnostics()
                );
                break;
            default:
                completeContainerMessage = String.format(
                        "Container %s marked as failed.\n Exit code:%s.\n Diagnostics:%s",
                        containerStatus.getContainerId().toString(),
                        containerStatus.getExitStatus(),
                        containerStatus.getDiagnostics()
                );
        }
        return completeContainerMessage;
    }
}
