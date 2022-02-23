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

import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Class that manages all the connections between tasks.
 */

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 边管理器
 */
public class EdgeManager {

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 维护 IntermediateResultPartition 和 ExecutionVertex 之间的关系
     *  一个 IntermediateResultPartition 对应到下游的多个 ExecutionVertex
     */
    private final Map<IntermediateResultPartitionID, List<ConsumerVertexGroup>> partitionConsumers = new HashMap<>();

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 维护 ExecutionVertex 和 IntermediateResultPartition 之间的关系
     *  一个 ExecutionVertex 有多个 IntermediateResultPartition 输出
     */
    private final Map<ExecutionVertexID, List<ConsumedPartitionGroup>> vertexConsumedPartitions = new HashMap<>();

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：
     *
     */
    private final Map<IntermediateResultPartitionID, List<ConsumedPartitionGroup>> consumedPartitionsById = new HashMap<>();

    public void connectPartitionWithConsumerVertexGroup(IntermediateResultPartitionID resultPartitionId,
                                                        ConsumerVertexGroup consumerVertexGroup) {

        checkNotNull(consumerVertexGroup);

        final List<ConsumerVertexGroup> consumers = getConsumerVertexGroupsForPartitionInternal(resultPartitionId);

        // sanity check
        checkState(consumers.isEmpty(), "Currently there has to be exactly one consumer in real jobs");

        consumers.add(consumerVertexGroup);
    }

    public void connectVertexWithConsumedPartitionGroup(ExecutionVertexID executionVertexId,
                                                        ConsumedPartitionGroup consumedPartitionGroup) {

        checkNotNull(consumedPartitionGroup);

        final List<ConsumedPartitionGroup> consumedPartitions = getConsumedPartitionGroupsForVertexInternal(
                executionVertexId);

        consumedPartitions.add(consumedPartitionGroup);
    }

    private List<ConsumerVertexGroup> getConsumerVertexGroupsForPartitionInternal(IntermediateResultPartitionID resultPartitionId) {
        return partitionConsumers.computeIfAbsent(resultPartitionId, id -> new ArrayList<>());
    }

    private List<ConsumedPartitionGroup> getConsumedPartitionGroupsForVertexInternal(ExecutionVertexID executionVertexId) {
        return vertexConsumedPartitions.computeIfAbsent(executionVertexId, id -> new ArrayList<>());
    }

    public List<ConsumerVertexGroup> getConsumerVertexGroupsForPartition(IntermediateResultPartitionID resultPartitionId) {
        return Collections.unmodifiableList(getConsumerVertexGroupsForPartitionInternal(resultPartitionId));
    }

    public List<ConsumedPartitionGroup> getConsumedPartitionGroupsForVertex(ExecutionVertexID executionVertexId) {
        return Collections.unmodifiableList(getConsumedPartitionGroupsForVertexInternal(executionVertexId));
    }

    public void registerConsumedPartitionGroup(ConsumedPartitionGroup group) {
        for (IntermediateResultPartitionID partitionId : group) {
            consumedPartitionsById.computeIfAbsent(partitionId, ignore -> new ArrayList<>()).add(group);
        }
    }

    private List<ConsumedPartitionGroup> getConsumedPartitionGroupsByIdInternal(IntermediateResultPartitionID resultPartitionId) {
        return consumedPartitionsById.computeIfAbsent(resultPartitionId, id -> new ArrayList<>());
    }

    public List<ConsumedPartitionGroup> getConsumedPartitionGroupsById(IntermediateResultPartitionID resultPartitionId) {
        return Collections.unmodifiableList(getConsumedPartitionGroupsByIdInternal(resultPartitionId));
    }
}
