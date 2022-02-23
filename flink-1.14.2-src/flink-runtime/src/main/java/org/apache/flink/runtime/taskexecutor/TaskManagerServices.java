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
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.blob.PermanentBlobService;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.shuffle.ShuffleEnvironmentContext;
import org.apache.flink.runtime.shuffle.ShuffleServiceLoader;
import org.apache.flink.runtime.state.TaskExecutorLocalStateStoresManager;
import org.apache.flink.runtime.state.TaskExecutorStateChangelogStoragesManager;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTableImpl;
import org.apache.flink.runtime.taskexecutor.slot.TimerService;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Container for {@link TaskExecutor} services such as the {@link MemoryManager}, {@link IOManager},
 * {@link ShuffleEnvironment}. All services are exclusive to a single {@link TaskExecutor}.
 * Consequently, the respective {@link TaskExecutor} is responsible for closing them.
 */
public class TaskManagerServices {
    private static final Logger LOG = LoggerFactory.getLogger(TaskManagerServices.class);

    @VisibleForTesting
    public static final String LOCAL_STATE_SUB_DIRECTORY_ROOT = "localState";

    /** TaskManager services. */
    private final UnresolvedTaskManagerLocation unresolvedTaskManagerLocation;

    private final long managedMemorySize;
    private final IOManager ioManager;
    private final ShuffleEnvironment<?, ?> shuffleEnvironment;
    private final KvStateService kvStateService;
    private final BroadcastVariableManager broadcastVariableManager;
    private final TaskSlotTable<Task> taskSlotTable;
    private final JobTable jobTable;
    private final JobLeaderService jobLeaderService;
    private final TaskExecutorLocalStateStoresManager taskManagerStateStore;
    private final TaskExecutorStateChangelogStoragesManager taskManagerChangelogManager;
    private final TaskEventDispatcher taskEventDispatcher;
    private final ExecutorService ioExecutor;
    private final LibraryCacheManager libraryCacheManager;

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：
     */
    TaskManagerServices(UnresolvedTaskManagerLocation unresolvedTaskManagerLocation, long managedMemorySize, IOManager ioManager,
                        ShuffleEnvironment<?, ?> shuffleEnvironment, KvStateService kvStateService,
                        BroadcastVariableManager broadcastVariableManager, TaskSlotTable<Task> taskSlotTable, JobTable jobTable,
                        JobLeaderService jobLeaderService, TaskExecutorLocalStateStoresManager taskManagerStateStore,
                        TaskExecutorStateChangelogStoragesManager taskManagerChangelogManager,
                        TaskEventDispatcher taskEventDispatcher, ExecutorService ioExecutor,
                        LibraryCacheManager libraryCacheManager) {

        this.unresolvedTaskManagerLocation = Preconditions.checkNotNull(unresolvedTaskManagerLocation);
        this.managedMemorySize = managedMemorySize;
        this.ioManager = Preconditions.checkNotNull(ioManager);
        this.shuffleEnvironment = Preconditions.checkNotNull(shuffleEnvironment);
        this.kvStateService = Preconditions.checkNotNull(kvStateService);
        this.broadcastVariableManager = Preconditions.checkNotNull(broadcastVariableManager);
        this.taskSlotTable = Preconditions.checkNotNull(taskSlotTable);
        this.jobTable = Preconditions.checkNotNull(jobTable);
        this.jobLeaderService = Preconditions.checkNotNull(jobLeaderService);
        this.taskManagerStateStore = Preconditions.checkNotNull(taskManagerStateStore);
        this.taskManagerChangelogManager = Preconditions.checkNotNull(taskManagerChangelogManager);
        this.taskEventDispatcher = Preconditions.checkNotNull(taskEventDispatcher);
        this.ioExecutor = Preconditions.checkNotNull(ioExecutor);
        this.libraryCacheManager = Preconditions.checkNotNull(libraryCacheManager);
    }

    // --------------------------------------------------------------------------------------------
    //  Getter/Setter
    // --------------------------------------------------------------------------------------------

    public long getManagedMemorySize() {
        return managedMemorySize;
    }

    public IOManager getIOManager() {
        return ioManager;
    }

    public ShuffleEnvironment<?, ?> getShuffleEnvironment() {
        return shuffleEnvironment;
    }

    public KvStateService getKvStateService() {
        return kvStateService;
    }

    public UnresolvedTaskManagerLocation getUnresolvedTaskManagerLocation() {
        return unresolvedTaskManagerLocation;
    }

    public BroadcastVariableManager getBroadcastVariableManager() {
        return broadcastVariableManager;
    }

    public TaskSlotTable<Task> getTaskSlotTable() {
        return taskSlotTable;
    }

    public JobTable getJobTable() {
        return jobTable;
    }

    public JobLeaderService getJobLeaderService() {
        return jobLeaderService;
    }

    public TaskExecutorLocalStateStoresManager getTaskManagerStateStore() {
        return taskManagerStateStore;
    }

    public TaskExecutorStateChangelogStoragesManager getTaskManagerChangelogManager() {
        return taskManagerChangelogManager;
    }

    public TaskEventDispatcher getTaskEventDispatcher() {
        return taskEventDispatcher;
    }

    public Executor getIOExecutor() {
        return ioExecutor;
    }

    public LibraryCacheManager getLibraryCacheManager() {
        return libraryCacheManager;
    }

    // --------------------------------------------------------------------------------------------
    //  Shut down method
    // --------------------------------------------------------------------------------------------

    /** Shuts the {@link TaskExecutor} services down. */
    public void shutDown() throws FlinkException {

        Exception exception = null;

        try {
            taskManagerStateStore.shutdown();
        } catch (Exception e) {
            exception = e;
        }

        try {
            ioManager.close();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            shuffleEnvironment.close();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            kvStateService.shutdown();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            taskSlotTable.close();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            jobLeaderService.stop();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            ioExecutor.shutdown();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            jobTable.close();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            libraryCacheManager.shutdown();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        taskEventDispatcher.clearAll();

        if (exception != null) {
            throw new FlinkException("Could not properly shut down the TaskManager services.", exception);
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Static factory methods for task manager services
    // --------------------------------------------------------------------------------------------

    /**
     * Creates and returns the task manager services.
     *
     * @param taskManagerServicesConfiguration task manager configuration
     * @param permanentBlobService permanentBlobService used by the services
     * @param taskManagerMetricGroup metric group of the task manager
     * @param ioExecutor executor for async IO operations
     * @param fatalErrorHandler to handle class loading OOMs
     *
     * @return task manager components
     *
     * @throws Exception
     */
    public static TaskManagerServices fromConfiguration(TaskManagerServicesConfiguration taskManagerServicesConfiguration,
                                                        PermanentBlobService permanentBlobService,
                                                        MetricGroup taskManagerMetricGroup, ExecutorService ioExecutor,
                                                        FatalErrorHandler fatalErrorHandler) throws Exception {

        // pre-start checks
        // TODO_MA 马中华 注释： 检查 TaskExecutor 会用到的各种临时目录，是否符合要求，能创建，能写等
        checkTempDirs(taskManagerServicesConfiguration.getTmpDirPaths());

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： Task 的事件分发器
         */
        final TaskEventDispatcher taskEventDispatcher = new TaskEventDispatcher();

        // start the I/O manager, it will create some temp directories.
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： IO 管理器， 内部创建一组 WriterThread 和 ReaderThread 负责读写
         *  通过 NIO 提供的 异步方式来读写文件
         *  内部有两组线程： WriterThread  ReaderThread
         */
        final IOManager ioManager = new IOManagerAsync(taskManagerServicesConfiguration.getTmpDirPaths());

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 启动 ShuffleEnvironment 内的 NettyClient 和 NettyServer
         *  返回结果： NettyShuffleEnvironment
         */
        final ShuffleEnvironment<?, ?> shuffleEnvironment = createShuffleEnvironment(taskManagerServicesConfiguration,
                taskEventDispatcher, taskManagerMetricGroup, ioExecutor);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： NettyShuffleEnvironment 启动， 就是启动内部的 NettyClient 和 NettyServer
         */
        final int listeningDataPort = shuffleEnvironment.start();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： KvState 服务
         */
        final KvStateService kvStateService = KvStateService.fromConfiguration(taskManagerServicesConfiguration);
        kvStateService.start();

        final UnresolvedTaskManagerLocation unresolvedTaskManagerLocation = new UnresolvedTaskManagerLocation(
                taskManagerServicesConfiguration.getResourceID(), taskManagerServicesConfiguration.getExternalAddress(),
                // we expose the task manager location with the listening port
                // iff the external data port is not explicitly defined
                taskManagerServicesConfiguration.getExternalDataPort() > 0 ? taskManagerServicesConfiguration.getExternalDataPort() : listeningDataPort);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 广播变量管理器
         */
        final BroadcastVariableManager broadcastVariableManager = new BroadcastVariableManager();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： TaskExecutor 中用来管理 Slot 的组件： TaskSlotTable
         *  TaskSlotTable 是 TaskExecutor 中最为重要的一个组件
         *  TaskManager ---> TaskExecutor(管理slot，接受Task执行) --->  TaskSlotTable管理slot和task
         *  -
         *  当前这个从节点的所有的资源，按照 NumberOfSlots 全部均分
         *  64 / 8 = 8/slot
         *  network memory = 1200M， 每个 slot 分到的是： 1200M / 8 = 150M
         */
        final TaskSlotTable<Task> taskSlotTable = createTaskSlotTable(
                // TODO_MA 马中华 注释： 总 slot 数量
                taskManagerServicesConfiguration.getNumberOfSlots(),
                // TODO_MA 马中华 注释： 从节点的资源配置
                taskManagerServicesConfiguration.getTaskExecutorResourceSpec(),
                taskManagerServicesConfiguration.getTimerServiceShutdownTimeout(), taskManagerServicesConfiguration.getPageSize(),
                ioExecutor);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： Job 登记表 = DefaultJobTable = job 的登记表
         */
        final JobTable jobTable = DefaultJobTable.create();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： JobMaster 管理组件
         */
        final JobLeaderService jobLeaderService = new DefaultJobLeaderService(unresolvedTaskManagerLocation,
                taskManagerServicesConfiguration.getRetryingRegistrationConfiguration());

        final String[] stateRootDirectoryStrings = taskManagerServicesConfiguration.getLocalRecoveryStateRootDirectories();
        final File[] stateRootDirectoryFiles = new File[stateRootDirectoryStrings.length];
        for (int i = 0; i < stateRootDirectoryStrings.length; ++i) {
            stateRootDirectoryFiles[i] = new File(stateRootDirectoryStrings[i], LOCAL_STATE_SUB_DIRECTORY_ROOT);
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        final TaskExecutorLocalStateStoresManager taskStateManager = new TaskExecutorLocalStateStoresManager(
                taskManagerServicesConfiguration.isLocalRecoveryEnabled(), stateRootDirectoryFiles, ioExecutor);

        final TaskExecutorStateChangelogStoragesManager changelogStoragesManager = new TaskExecutorStateChangelogStoragesManager();

        final boolean failOnJvmMetaspaceOomError = taskManagerServicesConfiguration.getConfiguration()
                .getBoolean(CoreOptions.FAIL_ON_USER_CLASS_LOADING_METASPACE_OOM);
        final boolean checkClassLoaderLeak = taskManagerServicesConfiguration.getConfiguration()
                .getBoolean(CoreOptions.CHECK_LEAKED_CLASSLOADER);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        final LibraryCacheManager libraryCacheManager = new BlobLibraryCacheManager(permanentBlobService,
                BlobLibraryCacheManager.defaultClassLoaderFactory(taskManagerServicesConfiguration.getClassLoaderResolveOrder(),
                        taskManagerServicesConfiguration.getAlwaysParentFirstLoaderPatterns(),
                        failOnJvmMetaspaceOomError ? fatalErrorHandler : null, checkClassLoaderLeak));

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 把从节点上的各种核心服务，包装成一个整体，这个就是 TaskExecutor 在工作的时候所需要的
         */
        return new TaskManagerServices(unresolvedTaskManagerLocation,
                taskManagerServicesConfiguration.getManagedMemorySize().getBytes(), ioManager,          // TODO_MA 马中华 注释：
                shuffleEnvironment,   // TODO_MA 马中华 注释：
                kvStateService, broadcastVariableManager, taskSlotTable,   // TODO_MA 马中华 注释：
                jobTable,  // TODO_MA 马中华 注释：
                jobLeaderService,   // TODO_MA 马中华 注释：
                taskStateManager, changelogStoragesManager, taskEventDispatcher, ioExecutor, libraryCacheManager);
    }

    // TODO_MA 马中华 注释： TaskManager ====> TaskExecutor(TaskManagerServices)  ===> TaskSlotTable

    private static TaskSlotTable<Task> createTaskSlotTable(final int numberOfSlots,
                                                           final TaskExecutorResourceSpec taskExecutorResourceSpec,
                                                           final long timerServiceShutdownTimeout, final int pageSize,
                                                           final Executor memoryVerificationExecutor) {
        final TimerService<AllocationID> timerService = new TimerService<>(new ScheduledThreadPoolExecutor(1),
                timerServiceShutdownTimeout);
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return new TaskSlotTableImpl<>(
                // TODO_MA 马中华 注释： 总 slot 数量
                numberOfSlots,
                // TODO_MA 马中华 注释： 总资源
                TaskExecutorResourceUtils.generateTotalAvailableResourceProfile(taskExecutorResourceSpec),
                // TODO_MA 马中华 注释： slot资源， 根据上述前两个信息，全部平分
                TaskExecutorResourceUtils.generateDefaultSlotResourceProfile(taskExecutorResourceSpec, numberOfSlots),
                // TODO_MA 马中华 注释： 每页内存大小
                pageSize, timerService, memoryVerificationExecutor);
    }

    private static ShuffleEnvironment<?, ?> createShuffleEnvironment(
            TaskManagerServicesConfiguration taskManagerServicesConfiguration, TaskEventDispatcher taskEventDispatcher,
            MetricGroup taskManagerMetricGroup, Executor ioExecutor) throws FlinkException {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 创建 ShuffleEnvironmentContext 上下文对象
         */
        final ShuffleEnvironmentContext shuffleEnvironmentContext = new ShuffleEnvironmentContext(
                taskManagerServicesConfiguration.getConfiguration(),
                taskManagerServicesConfiguration.getResourceID(),
                taskManagerServicesConfiguration.getNetworkMemorySize(),
                taskManagerServicesConfiguration.isLocalCommunicationOnly(),
                taskManagerServicesConfiguration.getBindAddress(),
                taskEventDispatcher, taskManagerMetricGroup, ioExecutor);

        // TODO_MA 马中华 注释： 通过反射创建 NettyShuffleEnvironmentFactory
        return ShuffleServiceLoader.loadShuffleServiceFactory(taskManagerServicesConfiguration.getConfiguration())
                // TODO_MA 马中华 注释： 创建 NettyShuffleEnvironment
                .createShuffleEnvironment(shuffleEnvironmentContext);
    }

    /**
     * // TODO_MA 马中华 注释： 验证字符串表示的所有目录确实存在或可以创建，是正确的目录（不是文件），并且是可写的。
     * Validates that all the directories denoted by the strings do actually exist or can be
     * created, are proper directories (not files), and are writable.
     *
     * @param tmpDirs The array of directory paths to check.
     *
     * @throws IOException Thrown if any of the directories does not exist and cannot be created or
     *         is not writable or is a file, rather than a directory.
     */
    private static void checkTempDirs(String[] tmpDirs) throws IOException {

        // TODO_MA 马中华 注释： 遍历每个目录
        for (String dir : tmpDirs) {

            // TODO_MA 马中华 注释： 不是 空
            if (dir != null && !dir.equals("")) {
                File file = new File(dir);

                // TODO_MA 马中华 注释： 不存在，则创建，创建不成功，则报错
                if (!file.exists()) {
                    if (!file.mkdirs()) {
                        throw new IOException(
                                "Temporary file directory " + file.getAbsolutePath() + " does not exist and could not be created.");
                    }
                }

                // TODO_MA 马中华 注释： 不是文件夹
                if (!file.isDirectory()) {
                    throw new IOException("Temporary file directory " + file.getAbsolutePath() + " is not a directory.");
                }
                // TODO_MA 马中华 注释： 不能写
                if (!file.canWrite()) {
                    throw new IOException("Temporary file directory " + file.getAbsolutePath() + " is not writable.");
                }

                if (LOG.isInfoEnabled()) {
                    long totalSpaceGb = file.getTotalSpace() >> 30;
                    long usableSpaceGb = file.getUsableSpace() >> 30;
                    double usablePercentage = (double) usableSpaceGb / totalSpaceGb * 100;
                    String path = file.getAbsolutePath();
                    LOG.info(String.format("Temporary file directory '%s': total %d GB, " + "usable %d GB (%.2f%% usable)", path,
                            totalSpaceGb, usableSpaceGb, usablePercentage));
                }
            } else {
                throw new IllegalArgumentException("Temporary file directory #$id is null.");
            }
        }
    }
}
