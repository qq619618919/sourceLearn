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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default implementation of {@link ResourceManagerService}. */
public class ResourceManagerServiceImpl implements ResourceManagerService, LeaderContender {

    public static final String ENABLE_MULTI_LEADER_SESSION_PROPERTY = "flink.tests.enable-rm-multi-leader-session";

    private static final Logger LOG = LoggerFactory.getLogger(ResourceManagerServiceImpl.class);

    private final ResourceManagerFactory<?> resourceManagerFactory;
    private final ResourceManagerProcessContext rmProcessContext;

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：
     */
    private final LeaderElectionService leaderElectionService;

    private final FatalErrorHandler fatalErrorHandler;
    private final Executor ioExecutor;

    private final ExecutorService handleLeaderEventExecutor;
    private final CompletableFuture<Void> serviceTerminationFuture;

    private final Object lock = new Object();

    private final boolean enableMultiLeaderSession;

    @GuardedBy("lock")
    private boolean running;

    @Nullable
    @GuardedBy("lock")
    private ResourceManager<?> leaderResourceManager;

    @Nullable
    @GuardedBy("lock")
    private UUID leaderSessionID;

    @GuardedBy("lock")
    private CompletableFuture<Void> previousResourceManagerTerminationFuture;

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：
     */
    private ResourceManagerServiceImpl(ResourceManagerFactory<?> resourceManagerFactory,
                                       ResourceManagerProcessContext rmProcessContext) {

        this.resourceManagerFactory = checkNotNull(resourceManagerFactory);
        this.rmProcessContext = checkNotNull(rmProcessContext);

        this.leaderElectionService = rmProcessContext
                .getHighAvailabilityServices()
                .getResourceManagerLeaderElectionService();

        this.fatalErrorHandler = rmProcessContext.getFatalErrorHandler();
        this.ioExecutor = rmProcessContext.getIoExecutor();

        this.handleLeaderEventExecutor = Executors.newSingleThreadExecutor();
        this.serviceTerminationFuture = new CompletableFuture<>();

        this.enableMultiLeaderSession = System.getProperties().containsKey(ENABLE_MULTI_LEADER_SESSION_PROPERTY);

        this.running = false;
        this.leaderResourceManager = null;
        this.leaderSessionID = null;
        this.previousResourceManagerTerminationFuture = FutureUtils.completedVoidFuture();
    }

    // ------------------------------------------------------------------------
    //  ResourceManagerService
    // ------------------------------------------------------------------------

    @Override
    public void start() throws Exception {

        // TODO_MA 马中华 注释： 状态的更新
        synchronized (lock) {
            if (running) {
                LOG.debug("Resource manager service has already started.");
                return;
            }
            running = true;
        }

        LOG.info("Starting resource manager service.");

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 这句代码的具体执行机制：
         *  1、leaderElectionService = DefaultLeaderElectionService
         *  2、调用 start() 启动选举/注册
         *  3、选举成功或者失败，都会回调 leaderContender 的某个方法： grantLeadership()
         *  -
         *  补充：
         *  1、leaderContender 有四个： resourcemanager + dispatcher + webmontorEndpoint + jobmaster
         *  2、此时此刻的 leaderContender = this = ResourceMangerServieImpl
         */
        leaderElectionService.start(this);

        // TODO_MA 马中华 注释： 之后去到： this.grantLeadership()
    }

    @Override
    public CompletableFuture<Void> getTerminationFuture() {
        return serviceTerminationFuture;
    }

    @Override
    public CompletableFuture<Void> deregisterApplication(final ApplicationStatus applicationStatus,
                                                         final @Nullable String diagnostics) {
        synchronized (lock) {
            if (running && leaderResourceManager != null) {
                return leaderResourceManager
                        .getSelfGateway(ResourceManagerGateway.class)
                        .deregisterApplication(applicationStatus, diagnostics)
                        .thenApply(ack -> null);
            } else {
                return FutureUtils.completedExceptionally(new FlinkException(
                        "Cannot deregister application. Resource manager service is not available."));
            }
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        synchronized (lock) {
            if (running) {
                LOG.info("Stopping resource manager service.");
                running = false;
                stopLeaderElectionService();
                stopLeaderResourceManager();
            } else {
                LOG.debug("Resource manager service is not running.");
            }

            FutureUtils.forward(previousResourceManagerTerminationFuture, serviceTerminationFuture);
        }

        handleLeaderEventExecutor.shutdownNow();

        return serviceTerminationFuture;
    }

    // ------------------------------------------------------------------------
    //  LeaderContender
    // ------------------------------------------------------------------------

    @Override
    public void grantLeadership(UUID newLeaderSessionID) {
        handleLeaderEventExecutor.execute(() -> {
            synchronized (lock) {
                if (!running) {
                    LOG.info("Resource manager service is not running. Ignore granting leadership with session ID {}.",
                            newLeaderSessionID
                    );
                    return;
                }
                LOG.info("Resource manager service is granted leadership with session id {}.", newLeaderSessionID);

                try {
                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释：
                     *  1、创建
                     *  2、启动
                     */
                    startNewLeaderResourceManager(newLeaderSessionID);
                } catch (Throwable t) {
                    fatalErrorHandler.onFatalError(new FlinkException("Cannot start resource manager.", t));
                }
            }
        });
    }

    @Override
    public void revokeLeadership() {
        handleLeaderEventExecutor.execute(() -> {
            synchronized (lock) {
                if (!running) {
                    LOG.info("Resource manager service is not running. Ignore revoking leadership.");
                    return;
                }

                LOG.info("Resource manager service is revoked leadership with session id {}.", leaderSessionID);

                stopLeaderResourceManager();

                if (!enableMultiLeaderSession) {
                    closeAsync();
                }
            }
        });
    }

    @Override
    public void handleError(Exception exception) {
        fatalErrorHandler.onFatalError(new FlinkException(
                "Exception during leader election of resource manager occurred.",
                exception
        ));
    }

    // ------------------------------------------------------------------------
    //  Internal
    // ------------------------------------------------------------------------

    @GuardedBy("lock")
    private void startNewLeaderResourceManager(UUID newLeaderSessionID) throws Exception {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 关闭
         */
        stopLeaderResourceManager();

        this.leaderSessionID = newLeaderSessionID;

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 第一件事： 创建
         *  1、standalone 模式下： StandaloneResourceManager
         *  2、on YARN 模式下： ActiveResourceManager
         */
        this.leaderResourceManager = resourceManagerFactory.createResourceManager(rmProcessContext,
                newLeaderSessionID,
                ResourceID.generate()
        );

        final ResourceManager<?> newLeaderResourceManager = this.leaderResourceManager;

        // TODO_MA 马中华 注释： 都是异步编程的 API ，都是 java8 提供的特性
        previousResourceManagerTerminationFuture.thenComposeAsync((ignore) -> {
            synchronized (lock) {
                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第二件事： 启动
                 */
                return startResourceManagerIfIsLeader(newLeaderResourceManager);
            }
        }, handleLeaderEventExecutor).thenAcceptAsync((isStillLeader) -> {
            if (isStillLeader) {
                leaderElectionService.confirmLeadership(newLeaderSessionID, newLeaderResourceManager.getAddress());
            }
        }, ioExecutor);
    }

    /**
     * Returns a future that completes as {@code true} if the resource manager is still leader and
     * started, and {@code false} if it's no longer leader.
     */
    @GuardedBy("lock")
    private CompletableFuture<Boolean> startResourceManagerIfIsLeader(ResourceManager<?> resourceManager) {

        // TODO_MA 马中华 注释：
        if (isLeader(resourceManager)) {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 启动这个 RpcEndpoint 组件：
             *  resourceManager = StandaloneResourcemanager
             *  onStart() 在父类中
             */
            resourceManager.start();

            forwardTerminationFuture(resourceManager);

            return resourceManager.getStartedFuture().thenApply(ignore -> true);
        } else {
            return CompletableFuture.completedFuture(false);
        }
    }

    private void forwardTerminationFuture(ResourceManager<?> resourceManager) {
        resourceManager.getTerminationFuture().whenComplete((ignore, throwable) -> {
            synchronized (lock) {
                if (isLeader(resourceManager)) {
                    if (throwable != null) {
                        serviceTerminationFuture.completeExceptionally(throwable);
                    } else {
                        serviceTerminationFuture.complete(null);
                    }
                }
            }
        });
    }

    @GuardedBy("lock")
    private boolean isLeader(ResourceManager<?> resourceManager) {
        return running && this.leaderResourceManager == resourceManager;
    }

    @GuardedBy("lock")
    private void stopLeaderResourceManager() {
        if (leaderResourceManager != null) {
            previousResourceManagerTerminationFuture = previousResourceManagerTerminationFuture.thenCombine(
                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： close
                     */
                    leaderResourceManager.closeAsync(),
                    (ignore1, ignore2) -> null
            );
            leaderResourceManager = null;
            leaderSessionID = null;
        }
    }

    private void stopLeaderElectionService() {
        try {
            leaderElectionService.stop();
        } catch (Exception e) {
            serviceTerminationFuture.completeExceptionally(new FlinkException("Cannot stop leader election service.",
                    e
            ));
        }
    }

    @VisibleForTesting
    @Nullable
    public ResourceManager<?> getLeaderResourceManager() {
        synchronized (lock) {
            return leaderResourceManager;
        }
    }

    public static ResourceManagerServiceImpl create(ResourceManagerFactory<?> resourceManagerFactory,
                                                    Configuration configuration,
                                                    RpcService rpcService,
                                                    HighAvailabilityServices highAvailabilityServices,
                                                    HeartbeatServices heartbeatServices,
                                                    FatalErrorHandler fatalErrorHandler,
                                                    ClusterInformation clusterInformation,
                                                    @Nullable String webInterfaceUrl,
                                                    MetricRegistry metricRegistry,
                                                    String hostname,
                                                    Executor ioExecutor) throws ConfigurationException {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： ResourceManagerServiceImpl 的 resourceManager 的服务包装
         */
        return new ResourceManagerServiceImpl(
                // TODO_MA 马中华 注释： resourceManagerFactory 是用来创建 resourceManager 的
                // TODO_MA 马中华 注释： 具体的创建时机，比较靠后
                resourceManagerFactory,
                // TODO_MA 马中华 注释： 上下文Context ： 你需要什么东西，你就问他要， 这就是上下文
                // TODO_MA 马中华 注释： 写作文： 你说的某一句话，我不太理解到底是什么意思，我的了解上下文 = 语境
                // TODO_MA 马中华 注释： 很多技术里面都有这个概念
                // TODO_MA 马中华 注释： SparkContext 里面包含了所有的工作组价
                resourceManagerFactory.createResourceManagerProcessContext(configuration,
                        rpcService,
                        highAvailabilityServices,
                        heartbeatServices,
                        fatalErrorHandler,
                        clusterInformation,
                        webInterfaceUrl,
                        metricRegistry,
                        hostname,
                        ioExecutor
                )
        );
    }
}
