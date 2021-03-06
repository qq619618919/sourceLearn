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

package org.apache.flink.runtime.entrypoint.component;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.dispatcher.ExecutionGraphInfoStore;
import org.apache.flink.runtime.dispatcher.HistoryServerArchivist;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.dispatcher.SessionDispatcherFactory;
import org.apache.flink.runtime.dispatcher.runner.DefaultDispatcherRunnerFactory;
import org.apache.flink.runtime.dispatcher.runner.DispatcherRunner;
import org.apache.flink.runtime.dispatcher.runner.DispatcherRunnerFactory;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobmanager.HaServicesJobGraphStoreFactory;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManagerFactory;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.ResourceManagerService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerServiceImpl;
import org.apache.flink.runtime.rest.JobRestEndpointFactory;
import org.apache.flink.runtime.rest.RestEndpointFactory;
import org.apache.flink.runtime.rest.SessionRestEndpointFactory;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcherImpl;
import org.apache.flink.runtime.rest.handler.legacy.metrics.VoidMetricFetcher;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.WebMonitorEndpoint;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.runtime.webmonitor.retriever.impl.RpcGatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.ExponentialBackoffRetryStrategy;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Abstract class which implements the creation of the {@link DispatcherResourceManagerComponent}
 * components.
 */
public class DefaultDispatcherResourceManagerComponentFactory implements DispatcherResourceManagerComponentFactory {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Nonnull
    private final DispatcherRunnerFactory dispatcherRunnerFactory;

    @Nonnull
    private final ResourceManagerFactory<?> resourceManagerFactory;

    @Nonnull
    private final RestEndpointFactory<?> restEndpointFactory;

    public DefaultDispatcherResourceManagerComponentFactory(@Nonnull DispatcherRunnerFactory dispatcherRunnerFactory,
                                                            @Nonnull ResourceManagerFactory<?> resourceManagerFactory,
                                                            @Nonnull RestEndpointFactory<?> restEndpointFactory) {
        this.dispatcherRunnerFactory = dispatcherRunnerFactory;
        this.resourceManagerFactory = resourceManagerFactory;
        this.restEndpointFactory = restEndpointFactory;
    }

    @Override
    public DispatcherResourceManagerComponent create(Configuration configuration,
                                                     Executor ioExecutor,
                                                     RpcService rpcService,
                                                     HighAvailabilityServices highAvailabilityServices,
                                                     BlobServer blobServer,
                                                     HeartbeatServices heartbeatServices,
                                                     MetricRegistry metricRegistry,
                                                     ExecutionGraphInfoStore executionGraphInfoStore,
                                                     MetricQueryServiceRetriever metricQueryServiceRetriever,
                                                     FatalErrorHandler fatalErrorHandler) throws Exception {

        // TODO_MA ????????? ????????? Dispatcher ?????????
        LeaderRetrievalService dispatcherLeaderRetrievalService = null;
        // TODO_MA ????????? ????????? ResourceManager ?????????
        LeaderRetrievalService resourceManagerRetrievalService = null;
        // TODO_MA ????????? ????????? WebMonitorEndpoint
        WebMonitorEndpoint<?> webMonitorEndpoint = null;
        // TODO_MA ????????? ????????? ResourceManager
        ResourceManagerService resourceManagerService = null;
        // TODO_MA ????????? ????????? DispatcherRunner
        DispatcherRunner dispatcherRunner = null;

        try {

            /*************************************************
             * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
             *  ????????? /dispatcher
             */
            dispatcherLeaderRetrievalService = highAvailabilityServices.getDispatcherLeaderRetriever();

            /*************************************************
             * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
             *  ????????? /resource_manager
             */
            resourceManagerRetrievalService = highAvailabilityServices.getResourceManagerLeaderRetriever();

            // TODO_MA ????????? ????????? Dispatcher ??? GatewayRetriever
            final LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever = new RpcGatewayRetriever<>(
                    rpcService,
                    DispatcherGateway.class,
                    DispatcherId::fromUuid,
                    new ExponentialBackoffRetryStrategy(12, Duration.ofMillis(10), Duration.ofMillis(50))
            );
            // TODO_MA ????????? ????????? ResourceManager GatewayRetriever
            final LeaderGatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever = new RpcGatewayRetriever<>(
                    rpcService,
                    ResourceManagerGateway.class,
                    ResourceManagerId::fromUuid,
                    new ExponentialBackoffRetryStrategy(12, Duration.ofMillis(10), Duration.ofMillis(50))
            );

            // TODO_MA ????????? ????????? ?????????????????????????????? rest.server.numThreads = 4
            final ScheduledExecutorService executor = WebMonitorEndpoint.createExecutorService(configuration.getInteger(
                            RestOptions.SERVER_NUM_THREADS),
                    configuration.getInteger(RestOptions.SERVER_THREAD_PRIORITY),
                    "DispatcherRestEndpoint"
            );

            final long updateInterval = configuration.getLong(MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL);
            final MetricFetcher metricFetcher = updateInterval
                    == 0 ? VoidMetricFetcher.INSTANCE : MetricFetcherImpl.fromConfiguration(configuration,
                    metricQueryServiceRetriever,
                    dispatcherGatewayRetriever,
                    executor
            );

            /*************************************************
             * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
             *  ????????? Step1:  ?????????????????????????????? WebMonitorEndpoint
             *  DispatcherRestEndpoint -> WebMonitorEndpoint -> RestServerEndpoint
             */
            webMonitorEndpoint = restEndpointFactory.createRestEndpoint(configuration,
                    dispatcherGatewayRetriever,
                    resourceManagerGatewayRetriever,
                    blobServer,
                    executor,
                    metricFetcher,
                    highAvailabilityServices.getClusterRestEndpointLeaderElectionService(),
                    fatalErrorHandler
            );

            log.debug("Starting Dispatcher REST endpoint.");
            /*************************************************
             * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
             *  ????????? Step2:  ?????????????????????????????? WebMonitorEndpoint
             */
            webMonitorEndpoint.start();

            final String hostname = RpcUtils.getHostname(rpcService);

            /*************************************************
             * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
             *  ????????? Step3:  ?????????????????????????????? ResourceManagerService
             *  ???????????? ResourceManagerServiceImpl??? ????????? resourceManagerFactory
             */
            resourceManagerService = ResourceManagerServiceImpl.create(resourceManagerFactory,
                    configuration,
                    rpcService,
                    highAvailabilityServices,
                    heartbeatServices,
                    fatalErrorHandler,
                    new ClusterInformation(hostname, blobServer.getPort()),
                    webMonitorEndpoint.getRestBaseUrl(),
                    metricRegistry,
                    hostname,
                    ioExecutor
            );

            final HistoryServerArchivist historyServerArchivist = HistoryServerArchivist.createHistoryServerArchivist(
                    configuration,
                    webMonitorEndpoint,
                    ioExecutor
            );

            final PartialDispatcherServices partialDispatcherServices = new PartialDispatcherServices(configuration,
                    highAvailabilityServices,
                    resourceManagerGatewayRetriever,
                    blobServer,
                    heartbeatServices,
                    () -> JobManagerMetricGroup.createJobManagerMetricGroup(metricRegistry, hostname),
                    executionGraphInfoStore,
                    fatalErrorHandler,
                    historyServerArchivist,
                    metricRegistry.getMetricQueryServiceGatewayRpcAddress(),
                    ioExecutor
            );

            log.debug("Starting Dispatcher.");

            /*************************************************
             * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
             *  ????????? Step4:  ?????????????????????????????? DefaultDispatcherRunner, ????????? Dispatcher
             *      1???????????? Dispatcher
             *      2???????????? Dispatcher
             *  ??????????????????????????????????????????????????????????????????????????????
             *  1??? ?????? job ??????
             *  2??? ?????? job ?????????
             *  3??? ?????? job ?????????
             *  4??? ??? job ?????? JobMaster
             */
            dispatcherRunner = dispatcherRunnerFactory.createDispatcherRunner(highAvailabilityServices.getDispatcherLeaderElectionService(),
                    fatalErrorHandler,
                    new HaServicesJobGraphStoreFactory(highAvailabilityServices),
                    ioExecutor,
                    rpcService,
                    partialDispatcherServices
            );

            log.debug("Starting ResourceManagerService.");

            /*************************************************
             * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
             *  ????????? Step5:  ?????????????????????????????? ResourceManagerService
             */
            resourceManagerService.start();

            /*************************************************
             * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
             *  ????????? ????????????
             */
            resourceManagerRetrievalService.start(resourceManagerGatewayRetriever);

            /*************************************************
             * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
             *  ????????? ????????????
             */
            dispatcherLeaderRetrievalService.start(dispatcherGatewayRetriever);

            /*************************************************
             * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
             *  ????????? ??????????????????????????????????????? ?????????????????????????????? ??????????????? XXXComponent
             *  ???????????????????????? ComponentFactory ????????? Component
             *  ???????????????????????????????????????
             */
            return new DispatcherResourceManagerComponent(
                    // TODO_MA ????????? ????????? DispatcherRunner??? ???????????? Dispatcher
                    dispatcherRunner,
                    // TODO_MA ????????? ????????? ResourceManagerService ???????????? ResourceManager
                    resourceManagerService,
                    // TODO_MA ????????? ????????? Dispatcher ?????????????????? LeaderRetrievalService
                    dispatcherLeaderRetrievalService,
                    // TODO_MA ????????? ????????? ResourceManager ?????????????????? LeaderRetrievalService
                    resourceManagerRetrievalService,
                    // TODO_MA ????????? ????????? WebMonitorEndpoint ????????? ??????????????? NettyServer
                    webMonitorEndpoint,
                    // TODO_MA ????????? ????????? ?????????????????? fatalErrorHandler.onFatalError() ????????????
                    fatalErrorHandler
            );

            // TODO_MA ????????? ????????? ????????????????????? ???????????????????????????????????????
            // TODO_MA ????????? ?????????

        } catch (Exception exception) {
            // clean up all started components
            if (dispatcherLeaderRetrievalService != null) {
                try {
                    dispatcherLeaderRetrievalService.stop();
                } catch (Exception e) {
                    exception = ExceptionUtils.firstOrSuppressed(e, exception);
                }
            }

            if (resourceManagerRetrievalService != null) {
                try {
                    resourceManagerRetrievalService.stop();
                } catch (Exception e) {
                    exception = ExceptionUtils.firstOrSuppressed(e, exception);
                }
            }

            final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);

            if (webMonitorEndpoint != null) {
                terminationFutures.add(webMonitorEndpoint.closeAsync());
            }

            if (resourceManagerService != null) {
                terminationFutures.add(resourceManagerService.closeAsync());
            }

            if (dispatcherRunner != null) {
                terminationFutures.add(dispatcherRunner.closeAsync());
            }

            final FutureUtils.ConjunctFuture<Void> terminationFuture = FutureUtils.completeAll(terminationFutures);

            try {
                terminationFuture.get();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            throw new FlinkException("Could not create the DispatcherResourceManagerComponent.", exception);
        }
    }

    /*************************************************
     * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
     *  ?????????
     */
    public static DefaultDispatcherResourceManagerComponentFactory createSessionComponentFactory(ResourceManagerFactory<?> resourceManagerFactory) {

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ?????????
         */
        return new DefaultDispatcherResourceManagerComponentFactory(
                // TODO_MA ????????? ????????? ?????? SessionDispatcherFactory
                // TODO_MA ????????? ????????? A2 = DefaultDispatcherRunnerFactory
                DefaultDispatcherRunnerFactory.createSessionRunner(SessionDispatcherFactory.INSTANCE),

                // TODO_MA ????????? ????????? A1 = StandaloneResourceManagerFactory
                resourceManagerFactory,

                // TODO_MA ????????? ????????? A3 = SessionRestEndpointFactory
                SessionRestEndpointFactory.INSTANCE
        );
    }

    public static DefaultDispatcherResourceManagerComponentFactory createJobComponentFactory(ResourceManagerFactory<?> resourceManagerFactory,
                                                                                             JobGraphRetriever jobGraphRetriever) {
        return new DefaultDispatcherResourceManagerComponentFactory(

                // TODO_MA ????????? ????????? DefaultDispatcherRunnerFactory
                DefaultDispatcherRunnerFactory.createJobRunner(jobGraphRetriever),

                // TODO_MA ????????? ????????? YarnResourceManagerFactory
                resourceManagerFactory,

                // TODO_MA ????????? ?????????
                JobRestEndpointFactory.INSTANCE
        );
    }
}
