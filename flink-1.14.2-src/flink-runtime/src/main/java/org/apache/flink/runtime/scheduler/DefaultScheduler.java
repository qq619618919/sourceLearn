/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.executiongraph.failover.flip1.ExecutionFailureHandler;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailureHandlingResult;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartBackoffTimeStrategy;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinatorHolder;
import org.apache.flink.runtime.scheduler.exceptionhistory.FailureHandlingResultSnapshot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategyFactory;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.topology.Vertex;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IterableUtils;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The future default scheduler. */
public class DefaultScheduler extends SchedulerBase implements SchedulerOperations {

    private final Logger log;

    private final ClassLoader userCodeLoader;

    private final ExecutionSlotAllocator executionSlotAllocator;

    private final ExecutionFailureHandler executionFailureHandler;

    private final ScheduledExecutor delayExecutor;

    private final SchedulingStrategy schedulingStrategy;

    private final ExecutionVertexOperations executionVertexOperations;

    private final Set<ExecutionVertexID> verticesWaitingForRestart;

    private final ShuffleMaster<?> shuffleMaster;

    private final Time rpcTimeout;

    DefaultScheduler(final Logger log,
                     final JobGraph jobGraph,
                     final Executor ioExecutor,
                     final Configuration jobMasterConfiguration,
                     final Consumer<ComponentMainThreadExecutor> startUpAction,
                     final ScheduledExecutor delayExecutor,
                     final ClassLoader userCodeLoader,
                     final CheckpointRecoveryFactory checkpointRecoveryFactory,
                     final JobManagerJobMetricGroup jobManagerJobMetricGroup,
                     final SchedulingStrategyFactory schedulingStrategyFactory,
                     final FailoverStrategy.Factory failoverStrategyFactory,
                     final RestartBackoffTimeStrategy restartBackoffTimeStrategy,
                     final ExecutionVertexOperations executionVertexOperations,
                     final ExecutionVertexVersioner executionVertexVersioner,
                     final ExecutionSlotAllocatorFactory executionSlotAllocatorFactory,
                     long initializationTimestamp,
                     final ComponentMainThreadExecutor mainThreadExecutor,
                     final JobStatusListener jobStatusListener,
                     final ExecutionGraphFactory executionGraphFactory,
                     final ShuffleMaster<?> shuffleMaster,
                     final Time rpcTimeout) throws Exception {

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ????????? ?????? ExecutionGraph
         */
        super(log, jobGraph, ioExecutor, jobMasterConfiguration, userCodeLoader, checkpointRecoveryFactory,
                jobManagerJobMetricGroup, executionVertexVersioner, initializationTimestamp, mainThreadExecutor, jobStatusListener,
                executionGraphFactory);

        this.log = log;

        this.delayExecutor = checkNotNull(delayExecutor);
        this.userCodeLoader = checkNotNull(userCodeLoader);
        this.executionVertexOperations = checkNotNull(executionVertexOperations);
        this.shuffleMaster = checkNotNull(shuffleMaster);
        this.rpcTimeout = checkNotNull(rpcTimeout);

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ????????? Task ????????????
         */
        final FailoverStrategy failoverStrategy = failoverStrategyFactory.create(getSchedulingTopology(),
                getResultPartitionAvailabilityChecker());
        log.info("Using failover strategy {} for {} ({}).", failoverStrategy, jobGraph.getName(), jobGraph.getJobID());

        enrichResourceProfile();

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ????????? ?????????????????????????????????
         *  1???FailoverStrategy
         *  2???RestartStrategy
         */
        this.executionFailureHandler = new ExecutionFailureHandler(getSchedulingTopology(), failoverStrategy,
                restartBackoffTimeStrategy);

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ????????? ????????????
         */
        this.schedulingStrategy = schedulingStrategyFactory.createInstance(this, getSchedulingTopology());

        this.executionSlotAllocator = checkNotNull(executionSlotAllocatorFactory).createInstance(
                new DefaultExecutionSlotAllocationContext());

        this.verticesWaitingForRestart = new HashSet<>();
        startUpAction.accept(mainThreadExecutor);
    }

    // ------------------------------------------------------------------------
    // SchedulerNG
    // ------------------------------------------------------------------------

    @Override
    protected long getNumberOfRestarts() {
        return executionFailureHandler.getNumberOfRestarts();
    }

    @Override
    protected void cancelAllPendingSlotRequestsInternal() {
        IterableUtils.toStream(getSchedulingTopology().getVertices()).map(Vertex::getId).forEach(executionSlotAllocator::cancel);
    }

    @Override
    protected void startSchedulingInternal() {
        log.info("Starting scheduling with scheduling strategy [{}]", schedulingStrategy.getClass().getName());

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ????????? ???????????? Job ?????????
         */
        transitionToRunning();

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ????????? ????????????????????????????????????PipelinedRegion SchedulingStrategy
         *  PipelinedRegion (??????????????????????????????????????????Task?????? slot??????) ???????????????
         *  ??????????????????????????????Task?????????????????????????????????
         *  Execution ?????????????????? = Topology = ????????????????????????????????????Task?????????????????????slot
         *  ???????????? StrewamGraph + JobGraph + ExecutionGraph + ???????????????
         *  -
         *  ?????? PipelinedRegion ???????????? Slot
         *  ?????? Topology ???????????? PipelinedRegion????????? PipelinedRegion ?????????????????????????????????
         */
        schedulingStrategy.startScheduling();
    }

    @Override
    protected void updateTaskExecutionStateInternal(final ExecutionVertexID executionVertexId,
                                                    final TaskExecutionStateTransition taskExecutionState) {

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ?????????  taskExecutionState.getExecutionState() = Initializing
         */
        schedulingStrategy.onExecutionStateChange(executionVertexId, taskExecutionState.getExecutionState());

        maybeHandleTaskFailure(taskExecutionState, executionVertexId);
    }

    private void maybeHandleTaskFailure(final TaskExecutionStateTransition taskExecutionState,
                                        final ExecutionVertexID executionVertexId) {

        if (taskExecutionState.getExecutionState() == ExecutionState.FAILED) {
            final Throwable error = taskExecutionState.getError(userCodeLoader);
            handleTaskFailure(executionVertexId, error);
        }
    }

    private void handleTaskFailure(final ExecutionVertexID executionVertexId,
                                   @Nullable final Throwable error) {
        final long timestamp = System.currentTimeMillis();
        setGlobalFailureCause(error, timestamp);
        notifyCoordinatorsAboutTaskFailure(executionVertexId, error);
        final FailureHandlingResult failureHandlingResult = executionFailureHandler.getFailureHandlingResult(executionVertexId,
                error, timestamp);
        maybeRestartTasks(failureHandlingResult);
    }

    private void notifyCoordinatorsAboutTaskFailure(final ExecutionVertexID executionVertexId,
                                                    @Nullable final Throwable error) {
        final ExecutionJobVertex jobVertex = getExecutionJobVertex(executionVertexId.getJobVertexId());
        final int subtaskIndex = executionVertexId.getSubtaskIndex();

        jobVertex.getOperatorCoordinators().forEach(c -> c.subtaskFailed(subtaskIndex, error));
    }

    @Override
    public void handleGlobalFailure(final Throwable error) {
        final long timestamp = System.currentTimeMillis();
        setGlobalFailureCause(error, timestamp);

        log.info("Trying to recover from a global failure.", error);
        final FailureHandlingResult failureHandlingResult = executionFailureHandler.getGlobalFailureHandlingResult(error,
                timestamp);

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ?????????
         */
        maybeRestartTasks(failureHandlingResult);
    }

    private void maybeRestartTasks(final FailureHandlingResult failureHandlingResult) {
        if (failureHandlingResult.canRestart()) {
            /*************************************************
             * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
             *  ?????????
             */
            restartTasksWithDelay(failureHandlingResult);
        } else {
            failJob(failureHandlingResult.getError(), failureHandlingResult.getTimestamp());
        }
    }

    private void restartTasksWithDelay(final FailureHandlingResult failureHandlingResult) {
        final Set<ExecutionVertexID> verticesToRestart = failureHandlingResult.getVerticesToRestart();

        final Set<ExecutionVertexVersion> executionVertexVersions = new HashSet<>(
                executionVertexVersioner.recordVertexModifications(verticesToRestart).values());
        final boolean globalRecovery = failureHandlingResult.isGlobalFailure();

        addVerticesToRestartPending(verticesToRestart);

        final CompletableFuture<?> cancelFuture = cancelTasksAsync(verticesToRestart);

        final FailureHandlingResultSnapshot failureHandlingResultSnapshot = FailureHandlingResultSnapshot.create(
                failureHandlingResult, id -> this.getExecutionVertex(id).getCurrentExecutionAttempt());

        delayExecutor.schedule(() -> FutureUtils.assertNoException(cancelFuture.thenRunAsync(() -> {
            archiveFromFailureHandlingResult(failureHandlingResultSnapshot);
            /*************************************************
             * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
             *  ?????????
             */
            restartTasks(executionVertexVersions, globalRecovery);
        }, getMainThreadExecutor())), failureHandlingResult.getRestartDelayMS(), TimeUnit.MILLISECONDS);
    }

    private void addVerticesToRestartPending(final Set<ExecutionVertexID> verticesToRestart) {
        verticesWaitingForRestart.addAll(verticesToRestart);
        transitionExecutionGraphState(JobStatus.RUNNING, JobStatus.RESTARTING);
    }

    private void removeVerticesFromRestartPending(final Set<ExecutionVertexID> verticesToRestart) {
        verticesWaitingForRestart.removeAll(verticesToRestart);
        if (verticesWaitingForRestart.isEmpty()) {
            transitionExecutionGraphState(JobStatus.RESTARTING, JobStatus.RUNNING);
        }
    }

    private void restartTasks(final Set<ExecutionVertexVersion> executionVertexVersions,
                              final boolean isGlobalRecovery) {
        final Set<ExecutionVertexID> verticesToRestart = executionVertexVersioner.getUnmodifiedExecutionVertices(
                executionVertexVersions);

        removeVerticesFromRestartPending(verticesToRestart);

        resetForNewExecutions(verticesToRestart);

        try {
            restoreState(verticesToRestart, isGlobalRecovery);
        } catch (Throwable t) {
            handleGlobalFailure(t);
            return;
        }

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ?????????
         */
        schedulingStrategy.restartTasks(verticesToRestart);
    }

    private CompletableFuture<?> cancelTasksAsync(final Set<ExecutionVertexID> verticesToRestart) {
        // clean up all the related pending requests to avoid that immediately returned slot
        // is used to fulfill the pending requests of these tasks
        verticesToRestart.stream().forEach(executionSlotAllocator::cancel);

        final List<CompletableFuture<?>> cancelFutures = verticesToRestart.stream().map(this::cancelExecutionVertex)
                .collect(Collectors.toList());

        return FutureUtils.combineAll(cancelFutures);
    }

    private CompletableFuture<?> cancelExecutionVertex(final ExecutionVertexID executionVertexId) {
        final ExecutionVertex vertex = getExecutionVertex(executionVertexId);

        notifyCoordinatorOfCancellation(vertex);

        return executionVertexOperations.cancel(vertex);
    }

    @Override
    protected void notifyPartitionDataAvailableInternal(final IntermediateResultPartitionID partitionId) {
        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ?????????
         */
        schedulingStrategy.onPartitionConsumable(partitionId);
    }

    // ------------------------------------------------------------------------
    // SchedulerOperations
    // ------------------------------------------------------------------------

    @Override
    public void allocateSlotsAndDeploy(final List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {

        validateDeploymentOptions(executionVertexDeploymentOptions);

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ?????????
         */
        final Map<ExecutionVertexID, ExecutionVertexDeploymentOption> deploymentOptionsByVertex = groupDeploymentOptionsByVertexId(
                executionVertexDeploymentOptions);

        final List<ExecutionVertexID> verticesToDeploy = executionVertexDeploymentOptions.stream()
                .map(ExecutionVertexDeploymentOption::getExecutionVertexId).collect(Collectors.toList());

        final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex = executionVertexVersioner.recordVertexModifications(
                verticesToDeploy);

        // TODO_MA ????????? ????????? ?????? job ?????????
        transitionToScheduled(verticesToDeploy);

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ????????? ???????????? ?????? slot
         *  SlotExecutionVertexAssignment:  ????????? Task ??? Slot ???????????????
         *  1???Slot  ??????
         *  2???ExecutionVertex  ExecutionGraph???????????????????????????????????????Task
         *  3???Assignment  ????????????
         */
        final List<SlotExecutionVertexAssignment> slotExecutionVertexAssignments = allocateSlots(executionVertexDeploymentOptions);

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ????????? ??? List<SlotExecutionVertexAssignment> ????????? List<DeploymentHandle>
         *      ???????????????SlotExecutionVertexAssignment ????????? DeploymentHandle
         *      SlotExecutionVertexAssignment: ????????? ????????? Task ???????????????
         *      DeploymentHandle??? ????????? Task ????????????????????????
         *  ?????????????????????????????????????????????????????????????????????????????????
         *  ?????????????????????????????????????????????????????? Java??? ????????????
         */
        final List<DeploymentHandle> deploymentHandles = createDeploymentHandles(requiredVersionByVertex, deploymentOptionsByVertex,
                slotExecutionVertexAssignments);

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ????????? ???????????? ?????? Task????????? PiplineRegion ???????????? Task???
         */
        waitForAllSlotsAndDeploy(deploymentHandles);
    }

    private void validateDeploymentOptions(final Collection<ExecutionVertexDeploymentOption> deploymentOptions) {
        deploymentOptions.stream().map(ExecutionVertexDeploymentOption::getExecutionVertexId).map(this::getExecutionVertex).forEach(
                v -> checkState(v.getExecutionState() == ExecutionState.CREATED,
                        "expected vertex %s to be in CREATED state, was: %s", v.getID(), v.getExecutionState()));
    }

    private static Map<ExecutionVertexID, ExecutionVertexDeploymentOption> groupDeploymentOptionsByVertexId(final Collection<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
        return executionVertexDeploymentOptions.stream()
                .collect(Collectors.toMap(ExecutionVertexDeploymentOption::getExecutionVertexId, Function.identity()));
    }

    private List<SlotExecutionVertexAssignment> allocateSlots(final List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ?????????
         */
        return executionSlotAllocator.allocateSlotsFor(
                executionVertexDeploymentOptions.stream().map(ExecutionVertexDeploymentOption::getExecutionVertexId)
                        .collect(Collectors.toList()));
    }

    /*************************************************
     * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
     *  ?????????
     */
    private static List<DeploymentHandle> createDeploymentHandles(final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex,
                                                                  final Map<ExecutionVertexID, ExecutionVertexDeploymentOption> deploymentOptionsByVertex,
                                                                  final List<SlotExecutionVertexAssignment> slotExecutionVertexAssignments) {

        return slotExecutionVertexAssignments.stream().map(slotExecutionVertexAssignment -> {
            final ExecutionVertexID executionVertexId = slotExecutionVertexAssignment.getExecutionVertexId();
            
            /*************************************************
             * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
             *  ????????? 
             */
            return new DeploymentHandle(requiredVersionByVertex.get(executionVertexId),
                    deploymentOptionsByVertex.get(executionVertexId), slotExecutionVertexAssignment);
        }).collect(Collectors.toList());
    }

    private void waitForAllSlotsAndDeploy(final List<DeploymentHandle> deploymentHandles) {
        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ?????????
         */
        FutureUtils.assertNoException(

                /*************************************************
                 * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
                 *  ?????????
                 */
                assignAllResourcesAndRegisterProducedPartitions(deploymentHandles).handle(

                        /*************************************************
                         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
                         *  ????????? ??????????????? Task
                         *  ?????? PiplineRegion ???????????? Task
                         */
                        deployAll(deploymentHandles)));
    }

    private CompletableFuture<Void> assignAllResourcesAndRegisterProducedPartitions(final List<DeploymentHandle> deploymentHandles) {
        final List<CompletableFuture<Void>> resultFutures = new ArrayList<>();

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ?????????
         */
        for (DeploymentHandle deploymentHandle : deploymentHandles) {
            final CompletableFuture<Void> resultFuture = deploymentHandle.getSlotExecutionVertexAssignment().getLogicalSlotFuture()

                    /*************************************************
                     * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
                     *  ????????? ??????????????? ????????????
                     */.handle(assignResource(deploymentHandle))

                    /*************************************************
                     * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
                     *  ????????? ??????????????? ?????? ResultPartitionDeploymentDescriptor ????????????
                     */.thenCompose(registerProducedPartitions(deploymentHandle)).handle((ignore, throwable) -> {
                        if (throwable != null) {
                            handleTaskDeploymentFailure(deploymentHandle.getExecutionVertexId(), throwable);
                        }
                        return null;
                    });

            resultFutures.add(resultFuture);
        }
        return FutureUtils.waitForAll(resultFutures);
    }

    /*************************************************
     * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
     *  ?????????
     */
    private BiFunction<Void, Throwable, Void> deployAll(final List<DeploymentHandle> deploymentHandles) {
        return (ignored, throwable) -> {
            propagateIfNonNull(throwable);

            // TODO_MA ????????? ????????? ???????????? DeploymentHandle
            // TODO_MA ????????? ????????? ??????????????????????????? Task ????????????
            for (final DeploymentHandle deploymentHandle : deploymentHandles) {

                // TODO_MA ????????? ????????? ??????????????? SlotExecutionVertexAssignment
                final SlotExecutionVertexAssignment slotExecutionVertexAssignment = deploymentHandle.getSlotExecutionVertexAssignment();

                // TODO_MA ????????? ????????? ?????? LogicalSlot
                final CompletableFuture<LogicalSlot> slotAssigned = slotExecutionVertexAssignment.getLogicalSlotFuture();
                checkState(slotAssigned.isDone());

                /*************************************************
                 * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
                 *  ????????? ???????????? Task
                 */
                FutureUtils.assertNoException(slotAssigned.handle(deployOrHandleError(deploymentHandle)));
            }
            return null;
        };
    }

    private static void propagateIfNonNull(final Throwable throwable) {
        if (throwable != null) {
            throw new CompletionException(throwable);
        }
    }

    private BiFunction<LogicalSlot, Throwable, LogicalSlot> assignResource(final DeploymentHandle deploymentHandle) {

        final ExecutionVertexVersion requiredVertexVersion = deploymentHandle.getRequiredVertexVersion();
        final ExecutionVertexID executionVertexId = deploymentHandle.getExecutionVertexId();

        return (logicalSlot, throwable) -> {
            if (executionVertexVersioner.isModified(requiredVertexVersion)) {
                if (throwable == null) {
                    log.debug(
                            "Refusing to assign slot to execution vertex {} because this deployment was " + "superseded by another deployment",
                            executionVertexId);
                    releaseSlotIfPresent(logicalSlot);
                }
                return null;
            }

            // throw exception only if the execution version is not outdated.
            // this ensures that canceling a pending slot request does not fail
            // a task which is about to cancel in #restartTasksWithDelay(...)
            if (throwable != null) {
                throw new CompletionException(maybeWrapWithNoResourceAvailableException(throwable));
            }

            final ExecutionVertex executionVertex = getExecutionVertex(executionVertexId);

            /*************************************************
             * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
             *  ?????????
             */
            executionVertex.tryAssignResource(logicalSlot);
            return logicalSlot;
        };
    }

    private Function<LogicalSlot, CompletableFuture<Void>> registerProducedPartitions(final DeploymentHandle deploymentHandle) {

        final ExecutionVertexID executionVertexId = deploymentHandle.getExecutionVertexId();

        return logicalSlot -> {
            // a null logicalSlot means the slot assignment is skipped, in which case
            // the produced partition registration process can be skipped as well
            if (logicalSlot != null) {

                /*************************************************
                 * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
                 *  ?????????
                 */
                final ExecutionVertex executionVertex = getExecutionVertex(executionVertexId);

                final boolean notifyPartitionDataAvailable = deploymentHandle.getDeploymentOption().notifyPartitionDataAvailable();

                /*************************************************
                 * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
                 *  ?????????
                 */
                final CompletableFuture<Void> partitionRegistrationFuture = executionVertex.getCurrentExecutionAttempt()
                        .registerProducedPartitions(logicalSlot.getTaskManagerLocation(), notifyPartitionDataAvailable);

                return FutureUtils.orTimeout(partitionRegistrationFuture, rpcTimeout.toMilliseconds(), TimeUnit.MILLISECONDS,
                        getMainThreadExecutor());
            } else {
                return FutureUtils.completedVoidFuture();
            }
        };
    }

    private void releaseSlotIfPresent(@Nullable final LogicalSlot logicalSlot) {
        if (logicalSlot != null) {
            logicalSlot.releaseSlot(null);
        }
    }

    private void handleTaskDeploymentFailure(final ExecutionVertexID executionVertexId,
                                             final Throwable error) {
        executionVertexOperations.markFailed(getExecutionVertex(executionVertexId), error);
    }

    private static Throwable maybeWrapWithNoResourceAvailableException(final Throwable failure) {
        final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(failure);
        if (strippedThrowable instanceof TimeoutException) {
            return new NoResourceAvailableException(
                    "Could not allocate the required slot within slot request timeout. " + "Please make sure that the cluster has enough resources.",
                    failure);
        } else {
            return failure;
        }
    }

    /*************************************************
     * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
     *  ????????? DeploymentHandle
     *  1???Task
     *  2???Slot???Slot ???????????? TaskExecutor??? TaskExecutorGateway ???
     *  3???????????????
     *  ?????????
     *  ?????? RPC ?????????TaskExecutorGateway.submitTask()
     */
    private BiFunction<Object, Throwable, Void> deployOrHandleError(final DeploymentHandle deploymentHandle) {

        // TODO_MA ????????? ????????? ?????? ExecutionVertexID
        final ExecutionVertexVersion requiredVertexVersion = deploymentHandle.getRequiredVertexVersion();
        final ExecutionVertexID executionVertexId = requiredVertexVersion.getExecutionVertexId();

        return (ignored, throwable) -> {
            if (executionVertexVersioner.isModified(requiredVertexVersion)) {
                log.debug(
                        "Refusing to deploy execution vertex {} because this deployment was " + "superseded by another deployment",
                        executionVertexId);
                return null;
            }

            if (throwable == null) {
                /*************************************************
                 * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
                 *  ?????????
                 */
                deployTaskSafe(executionVertexId);
            } else {
                handleTaskDeploymentFailure(executionVertexId, throwable);
            }
            return null;
        };
    }

    private void deployTaskSafe(final ExecutionVertexID executionVertexId) {
        try {
            // TODO_MA ????????? ????????? ?????? ExecutionVertexID ?????? ExecutionVertex
            final ExecutionVertex executionVertex = getExecutionVertex(executionVertexId);

            /*************************************************
             * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
             *  ?????????
             */
            executionVertexOperations.deploy(executionVertex);
        } catch (Throwable e) {
            handleTaskDeploymentFailure(executionVertexId, e);
        }
    }

    private void notifyCoordinatorOfCancellation(ExecutionVertex vertex) {
        // this method makes a best effort to filter out duplicate notifications, meaning cases
        // where
        // the coordinator was already notified for that specific task
        // we don't notify if the task is already FAILED, CANCELLING, or CANCELED

        final ExecutionState currentState = vertex.getExecutionState();
        if (currentState == ExecutionState.FAILED || currentState == ExecutionState.CANCELING || currentState == ExecutionState.CANCELED) {
            return;
        }

        for (OperatorCoordinatorHolder coordinator : vertex.getJobVertex().getOperatorCoordinators()) {
            coordinator.subtaskFailed(vertex.getParallelSubtaskIndex(), null);
        }
    }

    private class DefaultExecutionSlotAllocationContext implements ExecutionSlotAllocationContext {

        @Override
        public ResourceProfile getResourceProfile(final ExecutionVertexID executionVertexId) {
            return getExecutionVertex(executionVertexId).getResourceProfile();
        }

        @Override
        public AllocationID getPriorAllocationId(final ExecutionVertexID executionVertexId) {
            return getExecutionVertex(executionVertexId).getLatestPriorAllocation();
        }

        @Override
        public SchedulingTopology getSchedulingTopology() {
            return DefaultScheduler.this.getSchedulingTopology();
        }

        @Override
        public Set<SlotSharingGroup> getLogicalSlotSharingGroups() {
            return getJobGraph().getSlotSharingGroups();
        }

        @Override
        public Set<CoLocationGroup> getCoLocationGroups() {
            return getJobGraph().getCoLocationGroups();
        }

        @Override
        public Collection<Collection<ExecutionVertexID>> getConsumedResultPartitionsProducers(ExecutionVertexID executionVertexId) {
            return inputsLocationsRetriever.getConsumedResultPartitionsProducers(executionVertexId);
        }

        @Override
        public Optional<CompletableFuture<TaskManagerLocation>> getTaskManagerLocation(ExecutionVertexID executionVertexId) {
            return inputsLocationsRetriever.getTaskManagerLocation(executionVertexId);
        }

        @Override
        public Optional<TaskManagerLocation> getStateLocation(ExecutionVertexID executionVertexId) {
            return stateLocationRetriever.getStateLocation(executionVertexId);
        }
    }

    private void enrichResourceProfile() {
        Set<SlotSharingGroup> ssgs = new HashSet<>();
        getJobGraph().getVertices().forEach(jv -> ssgs.add(jv.getSlotSharingGroup()));
        ssgs.forEach(ssg -> SsgNetworkMemoryCalculationUtils.enrichNetworkMemory(ssg, this::getExecutionJobVertex, shuffleMaster));
    }
}
