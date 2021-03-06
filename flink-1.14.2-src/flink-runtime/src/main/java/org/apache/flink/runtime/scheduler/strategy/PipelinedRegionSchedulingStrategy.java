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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.DeploymentOption;
import org.apache.flink.runtime.scheduler.ExecutionVertexDeploymentOption;
import org.apache.flink.runtime.scheduler.SchedulerOperations;
import org.apache.flink.util.IterableUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link SchedulingStrategy} instance which schedules tasks in granularity of pipelined regions.
 */
public class PipelinedRegionSchedulingStrategy implements SchedulingStrategy {

    private final SchedulerOperations schedulerOperations;

    private final SchedulingTopology schedulingTopology;

    private final DeploymentOption deploymentOption = new DeploymentOption(false);

    /** External consumer regions of each ConsumedPartitionGroup. */
    private final Map<ConsumedPartitionGroup, Set<SchedulingPipelinedRegion>> partitionGroupConsumerRegions = new IdentityHashMap<>();

    private final Map<SchedulingPipelinedRegion, List<ExecutionVertexID>> regionVerticesSorted = new IdentityHashMap<>();

    /** The ConsumedPartitionGroups which are produced by multiple regions. */
    private final Set<ConsumedPartitionGroup> crossRegionConsumedPartitionGroups = Collections.newSetFromMap(
            new IdentityHashMap<>());

    public PipelinedRegionSchedulingStrategy(final SchedulerOperations schedulerOperations,
                                             final SchedulingTopology schedulingTopology) {

        this.schedulerOperations = checkNotNull(schedulerOperations);
        this.schedulingTopology = checkNotNull(schedulingTopology);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        init();
    }

    private void init() {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        initCrossRegionConsumedPartitionGroups();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        initPartitionGroupConsumerRegions();

        for (SchedulingExecutionVertex vertex : schedulingTopology.getVertices()) {
            final SchedulingPipelinedRegion region = schedulingTopology.getPipelinedRegionOfVertex(vertex.getId());
            regionVerticesSorted.computeIfAbsent(region, r -> new ArrayList<>()).add(vertex.getId());
        }
    }

    private void initCrossRegionConsumedPartitionGroups() {
        final Map<ConsumedPartitionGroup, Set<SchedulingPipelinedRegion>> producerRegionsByConsumedPartitionGroup = new IdentityHashMap<>();

        for (SchedulingPipelinedRegion pipelinedRegion : schedulingTopology.getAllPipelinedRegions()) {
            for (ConsumedPartitionGroup consumedPartitionGroup : pipelinedRegion.getAllBlockingConsumedPartitionGroups()) {
                producerRegionsByConsumedPartitionGroup.computeIfAbsent(consumedPartitionGroup,
                        this::getProducerRegionsForConsumedPartitionGroup
                );
            }
        }

        for (SchedulingPipelinedRegion pipelinedRegion : schedulingTopology.getAllPipelinedRegions()) {
            for (ConsumedPartitionGroup consumedPartitionGroup : pipelinedRegion.getAllBlockingConsumedPartitionGroups()) {
                final Set<SchedulingPipelinedRegion> producerRegions = producerRegionsByConsumedPartitionGroup.get(
                        consumedPartitionGroup);
                if (producerRegions.size() > 1 && producerRegions.contains(pipelinedRegion)) {
                    crossRegionConsumedPartitionGroups.add(consumedPartitionGroup);
                }
            }
        }
    }

    private Set<SchedulingPipelinedRegion> getProducerRegionsForConsumedPartitionGroup(ConsumedPartitionGroup consumedPartitionGroup) {
        final Set<SchedulingPipelinedRegion> producerRegions = Collections.newSetFromMap(new IdentityHashMap<>());
        for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
            producerRegions.add(getProducerRegion(partitionId));
        }
        return producerRegions;
    }

    private SchedulingPipelinedRegion getProducerRegion(IntermediateResultPartitionID partitionId) {
        return schedulingTopology.getPipelinedRegionOfVertex(schedulingTopology
                .getResultPartition(partitionId)
                .getProducer()
                .getId());
    }

    private void initPartitionGroupConsumerRegions() {
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        for (SchedulingPipelinedRegion region : schedulingTopology.getAllPipelinedRegions()) {

            // TODO_MA 马中华 注释：
            for (ConsumedPartitionGroup consumedPartitionGroup : region.getAllBlockingConsumedPartitionGroups()) {
                if (crossRegionConsumedPartitionGroups.contains(consumedPartitionGroup)
                        || isExternalConsumedPartitionGroup(consumedPartitionGroup, region)) {
                    partitionGroupConsumerRegions
                            .computeIfAbsent(consumedPartitionGroup, group -> new HashSet<>())
                            .add(region);
                }
            }
        }
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：
     *  1、Set<SchedulingPipelinedRegion> sourceRegions = schedulingTopology.getAllPipelinedRegions()
     *  2、maybeScheduleRegions(sourceRegions);
     */
    @Override
    public void startScheduling() {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 首先获取 血缘区域
         */
        final Set<SchedulingPipelinedRegion> sourceRegions = IterableUtils
                .toStream(schedulingTopology.getAllPipelinedRegions())
                .filter(this::isSourceRegion)
                .collect(Collectors.toSet());

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 开始按照 血缘区域 调度
         */
        maybeScheduleRegions(sourceRegions);
    }

    private boolean isSourceRegion(SchedulingPipelinedRegion region) {
        for (ConsumedPartitionGroup consumedPartitionGroup : region.getAllBlockingConsumedPartitionGroups()) {
            if (crossRegionConsumedPartitionGroups.contains(consumedPartitionGroup) || isExternalConsumedPartitionGroup(
                    consumedPartitionGroup,
                    region
            )) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void restartTasks(final Set<ExecutionVertexID> verticesToRestart) {
        final Set<SchedulingPipelinedRegion> regionsToRestart = verticesToRestart
                .stream()
                .map(schedulingTopology::getPipelinedRegionOfVertex)
                .collect(Collectors.toSet());

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        maybeScheduleRegions(regionsToRestart);
    }

    @Override
    public void onExecutionStateChange(final ExecutionVertexID executionVertexId,
                                       final ExecutionState executionState) {
        if (executionState == ExecutionState.FINISHED) {
            final Set<ConsumedPartitionGroup> finishedConsumedPartitionGroups = IterableUtils
                    .toStream(schedulingTopology.getVertex(executionVertexId).getProducedResults())
                    .filter(partition -> partition.getState() == ResultPartitionState.CONSUMABLE)
                    .flatMap(partition -> partition.getConsumedPartitionGroups().stream())
                    .filter(group -> crossRegionConsumedPartitionGroups.contains(group)
                            || group.areAllPartitionsFinished())
                    .collect(Collectors.toSet());

            final Set<SchedulingPipelinedRegion> consumerRegions = finishedConsumedPartitionGroups
                    .stream()
                    .flatMap(partitionGroup -> partitionGroupConsumerRegions
                            .getOrDefault(partitionGroup, Collections.emptySet())
                            .stream())
                    .collect(Collectors.toSet());

            maybeScheduleRegions(consumerRegions);
        }
        else {

        }
    }

    @Override
    public void onPartitionConsumable(final IntermediateResultPartitionID resultPartitionId) {
    }

    private void maybeScheduleRegions(final Set<SchedulingPipelinedRegion> regions) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 对于 PiplineRegion 做拓扑排序
         */
        final List<SchedulingPipelinedRegion> regionsSorted = SchedulingStrategyUtils.sortPipelinedRegionsInTopologicalOrder(
                schedulingTopology,
                regions
        );

        final Map<ConsumedPartitionGroup, Boolean> consumableStatusCache = new HashMap<>();

        // TODO_MA 马中华 注释： 顺序调度
        for (SchedulingPipelinedRegion region : regionsSorted) {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            maybeScheduleRegion(region, consumableStatusCache);
        }
    }

    private void maybeScheduleRegion(final SchedulingPipelinedRegion region,
                                     final Map<ConsumedPartitionGroup, Boolean> consumableStatusCache) {
        if (!areRegionInputsAllConsumable(region, consumableStatusCache)) {
            return;
        }

        checkState(areRegionVerticesAllInCreatedState(region),
                "BUG: trying to schedule a region which is not in CREATED state"
        );

        final List<ExecutionVertexDeploymentOption> vertexDeploymentOptions = SchedulingStrategyUtils.createExecutionVertexDeploymentOptions(
                regionVerticesSorted.get(region),
                id -> deploymentOption
        );

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 这是 JobMaster 调度和部署的起点：
         *  1、allocateSlots()   申请这个 Job 所需要的所有的 slot
         *  2、Deploy()  申请到了 slot 之后，执行 Task 的部署
         */
        schedulerOperations.allocateSlotsAndDeploy(vertexDeploymentOptions);
    }

    private boolean areRegionInputsAllConsumable(final SchedulingPipelinedRegion region,
                                                 final Map<ConsumedPartitionGroup, Boolean> consumableStatusCache) {
        for (ConsumedPartitionGroup consumedPartitionGroup : region.getAllBlockingConsumedPartitionGroups()) {
            if (crossRegionConsumedPartitionGroups.contains(consumedPartitionGroup)) {
                if (!isCrossRegionConsumedPartitionConsumable(consumedPartitionGroup, region)) {
                    return false;
                }
            } else if (isExternalConsumedPartitionGroup(consumedPartitionGroup, region)) {
                if (!consumableStatusCache.computeIfAbsent(consumedPartitionGroup,
                        this::isConsumedPartitionGroupConsumable
                )) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean isConsumedPartitionGroupConsumable(final ConsumedPartitionGroup consumedPartitionGroup) {
        for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
            if (schedulingTopology.getResultPartition(partitionId).getState() != ResultPartitionState.CONSUMABLE) {
                return false;
            }
        }
        return true;
    }

    private boolean isCrossRegionConsumedPartitionConsumable(final ConsumedPartitionGroup consumedPartitionGroup,
                                                             final SchedulingPipelinedRegion pipelinedRegion) {
        for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
            if (isExternalConsumedPartition(partitionId, pipelinedRegion)
                    && schedulingTopology.getResultPartition(partitionId).getState()
                    != ResultPartitionState.CONSUMABLE) {
                return false;
            }
        }
        return true;
    }

    private boolean areRegionVerticesAllInCreatedState(final SchedulingPipelinedRegion region) {
        for (SchedulingExecutionVertex vertex : region.getVertices()) {
            if (vertex.getState() != ExecutionState.CREATED) {
                return false;
            }
        }
        return true;
    }

    private boolean isExternalConsumedPartitionGroup(ConsumedPartitionGroup consumedPartitionGroup,
                                                     SchedulingPipelinedRegion pipelinedRegion) {

        return isExternalConsumedPartition(consumedPartitionGroup.getFirst(), pipelinedRegion);
    }

    private boolean isExternalConsumedPartition(IntermediateResultPartitionID partitionId,
                                                SchedulingPipelinedRegion pipelinedRegion) {
        return !pipelinedRegion.contains(schedulingTopology.getResultPartition(partitionId).getProducer().getId());
    }

    @VisibleForTesting
    Set<ConsumedPartitionGroup> getCrossRegionConsumedPartitionGroups() {
        return Collections.unmodifiableSet(crossRegionConsumedPartitionGroups);
    }

    /** The factory for creating {@link PipelinedRegionSchedulingStrategy}. */
    public static class Factory implements SchedulingStrategyFactory {
        @Override
        public SchedulingStrategy createInstance(final SchedulerOperations schedulerOperations,
                                                 final SchedulingTopology schedulingTopology) {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 默认调度策略。基于 pipline 的 region 调度
             */
            return new PipelinedRegionSchedulingStrategy(schedulerOperations, schedulingTopology);
        }
    }
}
