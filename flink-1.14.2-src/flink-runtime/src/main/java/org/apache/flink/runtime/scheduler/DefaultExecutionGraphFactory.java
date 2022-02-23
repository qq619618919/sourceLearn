/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.executiongraph.DefaultExecutionGraphBuilder;
import org.apache.flink.runtime.executiongraph.ExecutionDeploymentListener;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionStateUpdateListener;
import org.apache.flink.runtime.executiongraph.VertexAttemptNumberStore;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobmaster.ExecutionDeploymentTracker;
import org.apache.flink.runtime.jobmaster.ExecutionDeploymentTrackerDeploymentListenerAdapter;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.shuffle.ShuffleMaster;

import org.slf4j.Logger;

import java.util.HashSet;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/** Default {@link ExecutionGraphFactory} implementation. */
public class DefaultExecutionGraphFactory implements ExecutionGraphFactory {

    private final Configuration configuration;
    private final ClassLoader userCodeClassLoader;
    private final ExecutionDeploymentTracker executionDeploymentTracker;
    private final ScheduledExecutorService futureExecutor;
    private final Executor ioExecutor;
    private final Time rpcTimeout;
    private final JobManagerJobMetricGroup jobManagerJobMetricGroup;
    private final BlobWriter blobWriter;
    private final ShuffleMaster<?> shuffleMaster;
    private final JobMasterPartitionTracker jobMasterPartitionTracker;

    public DefaultExecutionGraphFactory(Configuration configuration,
                                        ClassLoader userCodeClassLoader,
                                        ExecutionDeploymentTracker executionDeploymentTracker,
                                        ScheduledExecutorService futureExecutor,
                                        Executor ioExecutor,
                                        Time rpcTimeout,
                                        JobManagerJobMetricGroup jobManagerJobMetricGroup,
                                        BlobWriter blobWriter,
                                        ShuffleMaster<?> shuffleMaster,
                                        JobMasterPartitionTracker jobMasterPartitionTracker) {
        this.configuration = configuration;
        this.userCodeClassLoader = userCodeClassLoader;
        this.executionDeploymentTracker = executionDeploymentTracker;
        this.futureExecutor = futureExecutor;
        this.ioExecutor = ioExecutor;
        this.rpcTimeout = rpcTimeout;
        this.jobManagerJobMetricGroup = jobManagerJobMetricGroup;
        this.blobWriter = blobWriter;
        this.shuffleMaster = shuffleMaster;
        this.jobMasterPartitionTracker = jobMasterPartitionTracker;
    }

    @Override
    public ExecutionGraph createAndRestoreExecutionGraph(JobGraph jobGraph,
                                                         CompletedCheckpointStore completedCheckpointStore,
                                                         CheckpointsCleaner checkpointsCleaner,
                                                         CheckpointIDCounter checkpointIdCounter,
                                                         TaskDeploymentDescriptorFactory.PartitionLocationConstraint partitionLocationConstraint,
                                                         long initializationTimestamp,
                                                         VertexAttemptNumberStore vertexAttemptNumberStore,
                                                         VertexParallelismStore vertexParallelismStore,
                                                         Logger log) throws Exception {

        ExecutionDeploymentListener executionDeploymentListener = new ExecutionDeploymentTrackerDeploymentListenerAdapter(
                executionDeploymentTracker);
        ExecutionStateUpdateListener executionStateUpdateListener = (execution, newState) -> {
            if (newState.isTerminal()) {
                executionDeploymentTracker.stopTrackingDeploymentOf(execution);
            }
        };

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 将 JobGraph 变成 ExecutionGraph
         */
        final ExecutionGraph newExecutionGraph = DefaultExecutionGraphBuilder.buildGraph(jobGraph, configuration, futureExecutor,
                ioExecutor, userCodeClassLoader, completedCheckpointStore, checkpointsCleaner, checkpointIdCounter, rpcTimeout,
                jobManagerJobMetricGroup, blobWriter, log, shuffleMaster, jobMasterPartitionTracker, partitionLocationConstraint,
                executionDeploymentListener, executionStateUpdateListener, initializationTimestamp, vertexAttemptNumberStore,
                vertexParallelismStore);

        final CheckpointCoordinator checkpointCoordinator = newExecutionGraph.getCheckpointCoordinator();

        if (checkpointCoordinator != null) {
            // check whether we find a valid checkpoint
            if (!checkpointCoordinator.restoreInitialCheckpointIfPresent(
                    new HashSet<>(newExecutionGraph.getAllVertices().values()))) {
                // check whether we can restore from a savepoint
                // TODO_MA 马中华 注释： 从 savepoint 恢复得到 ExecutionGraph
                tryRestoreExecutionGraphFromSavepoint(newExecutionGraph, jobGraph.getSavepointRestoreSettings());
            }
        }

        return newExecutionGraph;
    }

    /**
     * Tries to restore the given {@link ExecutionGraph} from the provided {@link
     * SavepointRestoreSettings}, iff checkpointing is enabled.
     *
     * @param executionGraphToRestore {@link ExecutionGraph} which is supposed to be restored
     * @param savepointRestoreSettings {@link SavepointRestoreSettings} containing information about
     *         the savepoint to restore from
     *
     * @throws Exception if the {@link ExecutionGraph} could not be restored
     */
    private void tryRestoreExecutionGraphFromSavepoint(ExecutionGraph executionGraphToRestore,
                                                       SavepointRestoreSettings savepointRestoreSettings) throws Exception {
        if (savepointRestoreSettings.restoreSavepoint()) {
            final CheckpointCoordinator checkpointCoordinator = executionGraphToRestore.getCheckpointCoordinator();
            if (checkpointCoordinator != null) {
                checkpointCoordinator.restoreSavepoint(savepointRestoreSettings.getRestorePath(),
                        savepointRestoreSettings.allowNonRestoredState(), executionGraphToRestore.getAllVertices(),
                        userCodeClassLoader);
            }
        }
    }
}
