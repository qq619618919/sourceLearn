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
 * limitations under the License
 */

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** Utilities for building {@link EdgeManager}. */
public class EdgeManagerBuildUtil {

    /**
     * Calculate the connections between {@link ExecutionJobVertex} and {@link IntermediateResult} *
     * based on the {@link DistributionPattern}.
     *
     * @param vertex the downstream consumer {@link ExecutionJobVertex}
     * @param intermediateResult the upstream consumed {@link IntermediateResult}
     * @param distributionPattern the {@link DistributionPattern} of the edge that connects the
     *         upstream {@link IntermediateResult} and the downstream {@link IntermediateResult}
     */
    static void connectVertexToResult(ExecutionJobVertex vertex,
                                      IntermediateResult intermediateResult,
                                      DistributionPattern distributionPattern) {

        // TODO_MA ????????? ????????? Flink ????????????????????? ??????
        switch (distributionPattern) {

            /*************************************************
             * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
             *  ????????? ?????????
             */
            case POINTWISE:
                connectPointwise(vertex.getTaskVertices(), intermediateResult);
                break;

            /*************************************************
             * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
             *  ????????? ?????????
             */
            case ALL_TO_ALL:
                connectAllToAll(vertex.getTaskVertices(), intermediateResult);
                break;
            default:
                throw new IllegalArgumentException("Unrecognized distribution pattern.");
        }
    }

    /**
     * Given parallelisms of two job vertices, compute the max number of edges connected to a target
     * execution vertex from the source execution vertices. Note that edge is considered undirected
     * here. It can be an edge connected from an upstream job vertex to a downstream job vertex, or
     * in a reversed way.
     *
     * @param targetParallelism parallelism of the target job vertex.
     * @param sourceParallelism parallelism of the source job vertex.
     * @param distributionPattern the {@link DistributionPattern} of the connecting edge.
     */
    public static int computeMaxEdgesToTargetExecutionVertex(int targetParallelism,
                                                             int sourceParallelism,
                                                             DistributionPattern distributionPattern) {
        switch (distributionPattern) {
            case POINTWISE:
                return (sourceParallelism + targetParallelism - 1) / targetParallelism;
            case ALL_TO_ALL:
                return sourceParallelism;
            default:
                throw new IllegalArgumentException("Unrecognized distribution pattern.");
        }
    }

    /*************************************************
     * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
     *  ????????? All to All ??????
     */
    private static void connectAllToAll(ExecutionVertex[] taskVertices, IntermediateResult intermediateResult) {

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ????????? ????????? IntermediateResult ??????????????? IntermediateResultPartition ????????????????????? ConsumedPartitionGroup
         */
        List<IntermediateResultPartitionID> consumedPartitions = Arrays
                .stream(intermediateResult.getPartitions())
                .map(IntermediateResultPartition::getPartitionId)
                .collect(Collectors.toList());

        ConsumedPartitionGroup consumedPartitionGroup = createAndRegisterConsumedPartitionGroupToEdgeManager(
                consumedPartitions,
                intermediateResult
        );

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ????????? ??? ConsumedPartitionGroup ???????????? ExecutionVertex ???????????????
         */
        for (ExecutionVertex ev : taskVertices) {
            ev.addConsumedPartitionGroup(consumedPartitionGroup);
        }

        // TODO_MA ????????? ????????? ?????? ExecutionJobVertex ??????????????? ExecutionVertex ??????????????? ConsumerVertexGroup
        List<ExecutionVertexID> consumerVertices = Arrays
                .stream(taskVertices)
                .map(ExecutionVertex::getID)
                .collect(Collectors.toList());

        ConsumerVertexGroup consumerVertexGroup = ConsumerVertexGroup.fromMultipleVertices(consumerVertices);

        /*************************************************
         * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
         *  ????????? ??? ConsumerVertexGroup ??????????????? IntermediateResultPartition
         */
        for (IntermediateResultPartition partition : intermediateResult.getPartitions()) {
            partition.addConsumers(consumerVertexGroup);
        }
    }

    /*************************************************
     * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
     *  ?????????  ???????????????????????????
     *  1??? ????????? = n : 1  ( 1-2 ==> 1,  3-3 ==> 2)
     *  2??? ????????? = 1 : 1  ( 1==>1, 2==> 2)
     *  3??? ????????? = 1 : n  ( 1 ==> 1-2,  2==> 3-4)
     */
    private static void connectPointwise(ExecutionVertex[] taskVertices, IntermediateResult intermediateResult) {

        // TODO_MA ????????? ????????? ???????????????????????????
        final int sourceCount = intermediateResult.getPartitions().length;
        final int targetCount = taskVertices.length;

        // TODO_MA ????????? ????????? ?????????????????????
        // TODO_MA ????????? ????????? ????????? = 1 : 1
        if (sourceCount == targetCount) {

            /*************************************************
             * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
             *  ????????? ?????????????????????
             *  ?????? ExecutionJobVertex ?????? ExecutionVertex
             *  ???
             *  ?????? IntermediateResult ?????? IntermediateResultParitition
             *  ??????????????????
             */
            for (int i = 0; i < sourceCount; i++) {

                // TODO_MA ????????? ????????? ?????? ExecutionVertex ??? IntermediateResultPartition
                ExecutionVertex executionVertex = taskVertices[i];
                IntermediateResultPartition partition = intermediateResult.getPartitions()[i];

                /*************************************************
                 * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
                 *  ????????? ??? IntermediateResultPartition ??????????????? ExecutionVertex ????????????
                 */
                ConsumerVertexGroup consumerVertexGroup = ConsumerVertexGroup.fromSingleVertex(executionVertex.getID());
                partition.addConsumers(consumerVertexGroup);

                /*************************************************
                 * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
                 *  ????????? ??? executionVertex ?????? IntermediateResultPartition ????????????
                 */
                ConsumedPartitionGroup consumedPartitionGroup = createAndRegisterConsumedPartitionGroupToEdgeManager(
                        partition.getPartitionId(),
                        intermediateResult
                );

                // TODO_MA ????????? ????????? ??????
                executionVertex.addConsumedPartitionGroup(consumedPartitionGroup);
            }
        }

        // TODO_MA ????????? ????????? ???????????????????????????????????? source ???
        // TODO_MA ????????? ????????? ????????? = n : 1
        else if (sourceCount > targetCount) {
            for (int index = 0; index < targetCount; index++) {

                ExecutionVertex executionVertex = taskVertices[index];
                ConsumerVertexGroup consumerVertexGroup = ConsumerVertexGroup.fromSingleVertex(executionVertex.getID());

                // TODO_MA ????????? ?????????
                int start = index * sourceCount / targetCount;
                int end = (index + 1) * sourceCount / targetCount;

                List<IntermediateResultPartitionID> consumedPartitions = new ArrayList<>(end - start);

                for (int i = start; i < end; i++) {
                    IntermediateResultPartition partition = intermediateResult.getPartitions()[i];
                    partition.addConsumers(consumerVertexGroup);
                    consumedPartitions.add(partition.getPartitionId());
                }

                ConsumedPartitionGroup consumedPartitionGroup = createAndRegisterConsumedPartitionGroupToEdgeManager(
                        consumedPartitions,
                        intermediateResult
                );
                executionVertex.addConsumedPartitionGroup(consumedPartitionGroup);
            }
        }

        // TODO_MA ????????? ????????? ???????????????????????????????????? source ???
        // TODO_MA ????????? ????????? ????????? = 1 : n
        else {
            for (int partitionNum = 0; partitionNum < sourceCount; partitionNum++) {

                IntermediateResultPartition partition = intermediateResult.getPartitions()[partitionNum];
                ConsumedPartitionGroup consumedPartitionGroup = createAndRegisterConsumedPartitionGroupToEdgeManager(
                        partition.getPartitionId(),
                        intermediateResult
                );

                int start = (partitionNum * targetCount + sourceCount - 1) / sourceCount;
                int end = ((partitionNum + 1) * targetCount + sourceCount - 1) / sourceCount;

                List<ExecutionVertexID> consumers = new ArrayList<>(end - start);

                for (int i = start; i < end; i++) {
                    ExecutionVertex executionVertex = taskVertices[i];
                    executionVertex.addConsumedPartitionGroup(consumedPartitionGroup);
                    consumers.add(executionVertex.getID());
                }

                ConsumerVertexGroup consumerVertexGroup = ConsumerVertexGroup.fromMultipleVertices(consumers);
                partition.addConsumers(consumerVertexGroup);
            }
        }
    }

    private static ConsumedPartitionGroup createAndRegisterConsumedPartitionGroupToEdgeManager(
            IntermediateResultPartitionID consumedPartitionId,
            IntermediateResult intermediateResult) {
        ConsumedPartitionGroup consumedPartitionGroup = ConsumedPartitionGroup.fromSinglePartition(consumedPartitionId);
        registerConsumedPartitionGroupToEdgeManager(consumedPartitionGroup, intermediateResult);
        return consumedPartitionGroup;
    }

    private static ConsumedPartitionGroup createAndRegisterConsumedPartitionGroupToEdgeManager(List<IntermediateResultPartitionID> consumedPartitions,
                                                                                               IntermediateResult intermediateResult) {
        ConsumedPartitionGroup consumedPartitionGroup = ConsumedPartitionGroup.fromMultiplePartitions(consumedPartitions);
        registerConsumedPartitionGroupToEdgeManager(consumedPartitionGroup, intermediateResult);
        return consumedPartitionGroup;
    }

    private static void registerConsumedPartitionGroupToEdgeManager(ConsumedPartitionGroup consumedPartitionGroup,
                                                                    IntermediateResult intermediateResult) {
        intermediateResult
                .getProducer()
                .getGraph()
                .getEdgeManager()
                .registerConsumedPartitionGroup(consumedPartitionGroup);
    }
}
