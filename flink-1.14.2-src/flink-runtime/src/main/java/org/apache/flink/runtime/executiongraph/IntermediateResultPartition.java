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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;

import java.util.List;

public class IntermediateResultPartition {

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 该 IntermediateResultPartition 从属于哪一个 IntermediateResult
     */
    private final IntermediateResult totalResult;

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 对应的生产者 ExecutionVertex 是谁
     */
    private final ExecutionVertex producer;

    private final IntermediateResultPartitionID partitionId;

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 边管理器， 管理当前 IntermediateResultPartition 和下游 Task 之间的关系
     */
    private final EdgeManager edgeManager;

    /** Whether this partition has produced some data. */
    private boolean hasDataProduced = false;

    public IntermediateResultPartition(IntermediateResult totalResult,
                                       ExecutionVertex producer,
                                       int partitionNumber,
                                       EdgeManager edgeManager) {
        this.totalResult = totalResult;
        this.producer = producer;
        this.partitionId = new IntermediateResultPartitionID(totalResult.getId(), partitionNumber);
        this.edgeManager = edgeManager;
    }

    public ExecutionVertex getProducer() {
        return producer;
    }

    public int getPartitionNumber() {
        return partitionId.getPartitionNumber();
    }

    public IntermediateResult getIntermediateResult() {
        return totalResult;
    }

    public IntermediateResultPartitionID getPartitionId() {
        return partitionId;
    }

    public ResultPartitionType getResultType() {
        return totalResult.getResultType();
    }

    public List<ConsumerVertexGroup> getConsumerVertexGroups() {
        return getEdgeManager().getConsumerVertexGroupsForPartition(partitionId);
    }

    public List<ConsumedPartitionGroup> getConsumedPartitionGroups() {
        return getEdgeManager().getConsumedPartitionGroupsById(partitionId);
    }

    public void markDataProduced() {
        hasDataProduced = true;
    }

    public boolean isConsumable() {
        return hasDataProduced;
    }

    void resetForNewExecution() {
        if (getResultType().isBlocking() && hasDataProduced) {
            // A BLOCKING result partition with data produced means it is finished
            // Need to add the running producer count of the result on resetting it
            for (ConsumedPartitionGroup consumedPartitionGroup : getConsumedPartitionGroups()) {
                consumedPartitionGroup.partitionUnfinished();
            }
        }
        hasDataProduced = false;
        for (ConsumedPartitionGroup consumedPartitionGroup : getConsumedPartitionGroups()) {
            totalResult.clearCachedInformationForPartitionGroup(consumedPartitionGroup);
        }
    }

    public void addConsumers(ConsumerVertexGroup consumers) {
        getEdgeManager().connectPartitionWithConsumerVertexGroup(partitionId, consumers);
    }

    private EdgeManager getEdgeManager() {
        return edgeManager;
    }

    void markFinished() {
        // Sanity check that this is only called on blocking partitions.
        if (!getResultType().isBlocking()) {
            throw new IllegalStateException("Tried to mark a non-blocking result partition as finished");
        }

        // Sanity check to make sure a result partition cannot be marked as finished twice.
        if (hasDataProduced) {
            throw new IllegalStateException("Tried to mark a finished result partition as finished.");
        }

        hasDataProduced = true;

        for (ConsumedPartitionGroup consumedPartitionGroup : getConsumedPartitionGroups()) {
            consumedPartitionGroup.partitionFinished();
        }
    }
}
