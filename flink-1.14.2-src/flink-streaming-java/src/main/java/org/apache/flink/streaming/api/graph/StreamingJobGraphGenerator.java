/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.InputOutputFormatContainer;
import org.apache.flink.runtime.jobgraph.InputOutputFormatVertex;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobgraph.tasks.TaskInvokable;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalPipelinedRegion;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalTopology;
import org.apache.flink.runtime.jobgraph.topology.LogicalVertex;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroupImpl;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.util.config.memory.ManagedMemoryUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.WithMasterCheckpointHook;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.UdfStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.YieldingOperatorFactory;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.streaming.runtime.partitioner.CustomPartitionerWrapper;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.tasks.StreamIterationHead;
import org.apache.flink.streaming.runtime.tasks.StreamIterationTail;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SerializedValue;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.MINIMAL_CHECKPOINT_TIME;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The StreamingJobGraphGenerator converts a {@link StreamGraph} into a {@link JobGraph}. */
@Internal
public class StreamingJobGraphGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingJobGraphGenerator.class);

    private static final long DEFAULT_NETWORK_BUFFER_TIMEOUT = 100L;

    public static final long UNDEFINED_NETWORK_BUFFER_TIMEOUT = -1L;

    // ------------------------------------------------------------------------

    public static JobGraph createJobGraph(StreamGraph streamGraph) {
        return createJobGraph(streamGraph, null);
    }

    public static JobGraph createJobGraph(StreamGraph streamGraph,
                                          @Nullable JobID jobID) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return new StreamingJobGraphGenerator(streamGraph, jobID).createJobGraph();
    }

    // ------------------------------------------------------------------------

    private final StreamGraph streamGraph;

    private final Map<Integer, JobVertex> jobVertices;
    private final JobGraph jobGraph;
    private final Collection<Integer> builtVertices;

    private final List<StreamEdge> physicalEdgesInOrder;

    private final Map<Integer, Map<Integer, StreamConfig>> chainedConfigs;

    private final Map<Integer, StreamConfig> vertexConfigs;
    private final Map<Integer, String> chainedNames;

    private final Map<Integer, ResourceSpec> chainedMinResources;
    private final Map<Integer, ResourceSpec> chainedPreferredResources;

    private final Map<Integer, InputOutputFormatContainer> chainedInputOutputFormats;

    private final StreamGraphHasher defaultStreamGraphHasher;
    private final List<StreamGraphHasher> legacyStreamGraphHashers;

    private StreamingJobGraphGenerator(StreamGraph streamGraph,
                                       @Nullable JobID jobID) {
        this.streamGraph = streamGraph;
        this.defaultStreamGraphHasher = new StreamGraphHasherV2();
        this.legacyStreamGraphHashers = Arrays.asList(new StreamGraphUserHashHasher());

        this.jobVertices = new HashMap<>();
        this.builtVertices = new HashSet<>();
        this.chainedConfigs = new HashMap<>();
        this.vertexConfigs = new HashMap<>();
        this.chainedNames = new HashMap<>();
        this.chainedMinResources = new HashMap<>();
        this.chainedPreferredResources = new HashMap<>();
        this.chainedInputOutputFormats = new HashMap<>();
        this.physicalEdgesInOrder = new ArrayList<>();

        jobGraph = new JobGraph(jobID, streamGraph.getJobName());
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 开始进入真正的转换逻辑了
     */
    private JobGraph createJobGraph() {

        // TODO_MA 马中华 注释： 校验
        preValidate();

        // TODO_MA 马中华 注释： JobType.STREAMING
        jobGraph.setJobType(streamGraph.getJobType());

        jobGraph.enableApproximateLocalRecovery(streamGraph.getCheckpointConfig().isApproximateLocalRecoveryEnabled());

        // Generate deterministic hashes for the nodes in order to identify them across
        // submission iff they didn't change.
        Map<Integer, byte[]> hashes = defaultStreamGraphHasher.traverseStreamGraphAndGenerateHashes(streamGraph);
        // Generate legacy version hashes for backwards compatibility
        List<Map<Integer, byte[]>> legacyHashes = new ArrayList<>(legacyStreamGraphHashers.size());
        for (StreamGraphHasher hasher : legacyStreamGraphHashers) {
            legacyHashes.add(hasher.traverseStreamGraphAndGenerateHashes(streamGraph));
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 重点
         */
        setChaining(hashes, legacyHashes);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        setPhysicalEdges();

        setSlotSharingAndCoLocation();

        setManagedMemoryFraction(Collections.unmodifiableMap(jobVertices), Collections.unmodifiableMap(vertexConfigs),
                Collections.unmodifiableMap(chainedConfigs),
                id -> streamGraph.getStreamNode(id).getManagedMemoryOperatorScopeUseCaseWeights(),
                id -> streamGraph.getStreamNode(id).getManagedMemorySlotScopeUseCases());

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        configureCheckpointing();

        jobGraph.setSavepointRestoreSettings(streamGraph.getSavepointRestoreSettings());

        final Map<String, DistributedCache.DistributedCacheEntry> distributedCacheEntries = JobGraphUtils.prepareUserArtifactEntries(
                streamGraph.getUserArtifacts().stream().collect(Collectors.toMap(e -> e.f0, e -> e.f1)), jobGraph.getJobID());

        for (Map.Entry<String, DistributedCache.DistributedCacheEntry> entry : distributedCacheEntries.entrySet()) {
            jobGraph.addUserArtifact(entry.getKey(), entry.getValue());
        }

        // set the ExecutionConfig last when it has been finalized
        try {
            jobGraph.setExecutionConfig(streamGraph.getExecutionConfig());
        } catch (IOException e) {
            throw new IllegalConfigurationException(
                    "Could not serialize the ExecutionConfig." + "This indicates that non-serializable types (like custom serializers) were registered");
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 返回 jobGraph
         */
        return jobGraph;
    }

    @SuppressWarnings("deprecation")
    private void preValidate() {
        CheckpointConfig checkpointConfig = streamGraph.getCheckpointConfig();

        if (checkpointConfig.isCheckpointingEnabled()) {
            // temporarily forbid checkpointing for iterative jobs
            if (streamGraph.isIterative() && !checkpointConfig.isForceCheckpointing()) {
                throw new UnsupportedOperationException(
                        "Checkpointing is currently not supported by default for iterative jobs, as we cannot guarantee exactly once semantics. " + "State checkpoints happen normally, but records in-transit during the snapshot will be lost upon failure. " + "\nThe user can force enable state checkpoints with the reduced guarantees by calling: env.enableCheckpointing(interval,true)");
            }
            if (streamGraph.isIterative() && checkpointConfig.isUnalignedCheckpointsEnabled() && !checkpointConfig.isForceUnalignedCheckpoints()) {
                throw new UnsupportedOperationException(
                        "Unaligned Checkpoints are currently not supported for iterative jobs, " + "as rescaling would require alignment (in addition to the reduced checkpointing guarantees)." + "\nThe user can force Unaligned Checkpoints by using 'execution.checkpointing.unaligned.forced'");
            }
            if (checkpointConfig.isUnalignedCheckpointsEnabled() && !checkpointConfig.isForceUnalignedCheckpoints() && streamGraph.getStreamNodes()
                    .stream().anyMatch(this::hasCustomPartitioner)) {
                throw new UnsupportedOperationException(
                        "Unaligned checkpoints are currently not supported for custom partitioners, " + "as rescaling is not guaranteed to work correctly." + "\nThe user can force Unaligned Checkpoints by using 'execution.checkpointing.unaligned.forced'");
            }

            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            for (StreamNode node : streamGraph.getStreamNodes()) {
                StreamOperatorFactory operatorFactory = node.getOperatorFactory();
                if (operatorFactory != null) {
                    Class<?> operatorClass = operatorFactory.getStreamOperatorClass(classLoader);
                    if (InputSelectable.class.isAssignableFrom(operatorClass)) {

                        throw new UnsupportedOperationException(
                                "Checkpointing is currently not supported for operators that implement InputSelectable:" + operatorClass.getName());
                    }
                }
            }
        }

        if (checkpointConfig.isUnalignedCheckpointsEnabled() && getCheckpointingMode(
                checkpointConfig) != CheckpointingMode.EXACTLY_ONCE) {
            LOG.warn("Unaligned checkpoints can only be used with checkpointing mode EXACTLY_ONCE");
            checkpointConfig.enableUnalignedCheckpoints(false);
        }
    }

    private boolean hasCustomPartitioner(StreamNode node) {
        return node.getOutEdges().stream().anyMatch(edge -> edge.getPartitioner() instanceof CustomPartitionerWrapper);
    }

    private void setPhysicalEdges() {

        Map<Integer, List<StreamEdge>> physicalInEdgesInOrder = new HashMap<Integer, List<StreamEdge>>();

        for (StreamEdge edge : physicalEdgesInOrder) {
            int target = edge.getTargetId();
            List<StreamEdge> inEdges = physicalInEdgesInOrder.computeIfAbsent(target, k -> new ArrayList<>());
            inEdges.add(edge);
        }

        for (Map.Entry<Integer, List<StreamEdge>> inEdges : physicalInEdgesInOrder.entrySet()) {
            int vertex = inEdges.getKey();
            List<StreamEdge> edgeList = inEdges.getValue();
            vertexConfigs.get(vertex).setInPhysicalEdges(edgeList);
        }
    }

    private Map<Integer, OperatorChainInfo> buildChainedInputsAndGetHeadInputs(final Map<Integer, byte[]> hashes,
                                                                               final List<Map<Integer, byte[]>> legacyHashes) {

        final Map<Integer, ChainedSourceInfo> chainedSources = new HashMap<>();
        final Map<Integer, OperatorChainInfo> chainEntryPoints = new HashMap<>();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        for (Integer sourceNodeId : streamGraph.getSourceIDs()) {
            final StreamNode sourceNode = streamGraph.getStreamNode(sourceNodeId);

            if (sourceNode.getOperatorFactory() instanceof SourceOperatorFactory && sourceNode.getOutEdges().size() == 1) {
                // as long as only NAry ops support this chaining, we need to skip the other parts
                final StreamEdge sourceOutEdge = sourceNode.getOutEdges().get(0);
                final StreamNode target = streamGraph.getStreamNode(sourceOutEdge.getTargetId());
                final ChainingStrategy targetChainingStrategy = target.getOperatorFactory().getChainingStrategy();

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释：
                 */
                if (targetChainingStrategy == ChainingStrategy.HEAD_WITH_SOURCES && isChainableInput(sourceOutEdge, streamGraph)) {
                    final OperatorID opId = new OperatorID(hashes.get(sourceNodeId));
                    final StreamConfig.SourceInputConfig inputConfig = new StreamConfig.SourceInputConfig(sourceOutEdge);
                    final StreamConfig operatorConfig = new StreamConfig(new Configuration());
                    setVertexConfig(sourceNodeId, operatorConfig, Collections.emptyList(), Collections.emptyList(),
                            Collections.emptyMap());
                    operatorConfig.setChainIndex(0); // sources are always first
                    operatorConfig.setOperatorID(opId);
                    operatorConfig.setOperatorName(sourceNode.getOperatorName());
                    chainedSources.put(sourceNodeId, new ChainedSourceInfo(operatorConfig, inputConfig));

                    final SourceOperatorFactory<?> sourceOpFact = (SourceOperatorFactory<?>) sourceNode.getOperatorFactory();
                    final OperatorCoordinator.Provider coord = sourceOpFact.getCoordinatorProvider(sourceNode.getOperatorName(),
                            opId);

                    final OperatorChainInfo chainInfo = chainEntryPoints.computeIfAbsent(sourceOutEdge.getTargetId(),
                            (k) -> new OperatorChainInfo(sourceOutEdge.getTargetId(), hashes, legacyHashes, chainedSources,
                                    streamGraph));
                    chainInfo.addCoordinatorProvider(coord);
                    continue;
                }
            }

            chainEntryPoints.put(sourceNodeId,
                    new OperatorChainInfo(sourceNodeId, hashes, legacyHashes, chainedSources, streamGraph));
        }

        return chainEntryPoints;
    }

    /**
     * Sets up task chains from the source {@link StreamNode} instances.
     *
     * <p>This will recursively create all {@link JobVertex} instances.
     */
    private void setChaining(Map<Integer, byte[]> hashes,
                             List<Map<Integer, byte[]>> legacyHashes) {

        // we separate out the sources that run as inputs to another operator (chained inputs)
        // from the sources that needs to run as the main (head) operator.
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        final Map<Integer, OperatorChainInfo> chainEntryPoints = buildChainedInputsAndGetHeadInputs(hashes, legacyHashes);
        final Collection<OperatorChainInfo> initialEntryPoints = chainEntryPoints.entrySet().stream()
                .sorted(Comparator.comparing(Map.Entry::getKey)).map(Map.Entry::getValue).collect(Collectors.toList());

        // iterate over a copy of the values, because this map gets concurrently modified
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 遍历每个 Operator
         */
        for (OperatorChainInfo info : initialEntryPoints) {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： createChain 如果当前 Operator 不能完成和之前的 Operator 的 chain 操作，则单独创建一个
             *  如果可以呢，先缓存起来
             *  A B C D
             */
            createChain(info.getStartNodeId(), 1,
                    // operators start at position 1 because 0 is for chained source inputs
                    info, chainEntryPoints);
        }
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 创建 OperatorChain
     *  整个这个方法有三个要点：
     *  1、判断是否可 chain
     *  2、创建每个 OoperatorChain 对应的 JobVertex 顶点
     *  3、创建边
     */
    private List<StreamEdge> createChain(final Integer currentNodeId,
                                         final int chainIndex,
                                         final OperatorChainInfo chainInfo,
                                         final Map<Integer, OperatorChainInfo> chainEntryPoints) {

        Integer startNodeId = chainInfo.getStartNodeId();
        if (!builtVertices.contains(startNodeId)) {

            List<StreamEdge> transitiveOutEdges = new ArrayList<StreamEdge>();

            // TODO_MA 马中华 注释： 可 chain 的边
            List<StreamEdge> chainableOutputs = new ArrayList<StreamEdge>();
            // TODO_MA 马中华 注释： 不可以 chain 的边
            List<StreamEdge> nonChainableOutputs = new ArrayList<StreamEdge>();

            // TODO_MA 马中华 注释： 当前节点
            // TODO_MA 马中华 注释： 构建 StreamGraph 的时候，维护了 StreamNode 和 StreamEdge 的双边关系
            StreamNode currentNode = streamGraph.getStreamNode(currentNodeId);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 第一个重点
             *  判断当前顶点，和下游顶点之间的关系，是否可以进行 chain
             */
            for (StreamEdge outEdge : currentNode.getOutEdges()) {

                // TODO_MA 马中华 注释： 执行判断 判断是否可以进行 chain
                if (isChainable(outEdge, streamGraph)) {
                    chainableOutputs.add(outEdge);
                } else {
                    nonChainableOutputs.add(outEdge);
                }
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 继续判断
             */
            for (StreamEdge chainable : chainableOutputs) {
                transitiveOutEdges.addAll(createChain(chainable.getTargetId(), chainIndex + 1, chainInfo, chainEntryPoints));
            }

            for (StreamEdge nonChainable : nonChainableOutputs) {
                transitiveOutEdges.add(nonChainable);
                createChain(nonChainable.getTargetId(), 1,
                        // operators start at position 1 because 0 is for chained source inputs
                        chainEntryPoints.computeIfAbsent(nonChainable.getTargetId(),
                                (k) -> chainInfo.newChain(nonChainable.getTargetId())), chainEntryPoints);
            }

            chainedNames.put(currentNodeId,
                    createChainedName(currentNodeId, chainableOutputs, Optional.ofNullable(chainEntryPoints.get(currentNodeId))));

            chainedMinResources.put(currentNodeId, createChainedMinResources(currentNodeId, chainableOutputs));
            chainedPreferredResources.put(currentNodeId, createChainedPreferredResources(currentNodeId, chainableOutputs));

            OperatorID currentOperatorId = chainInfo.addNodeToChain(currentNodeId, chainedNames.get(currentNodeId));

            if (currentNode.getInputFormat() != null) {
                getOrCreateFormatContainer(startNodeId).addInputFormat(currentOperatorId, currentNode.getInputFormat());
            }

            if (currentNode.getOutputFormat() != null) {
                getOrCreateFormatContainer(startNodeId).addOutputFormat(currentOperatorId, currentNode.getOutputFormat());
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 第二个重点
             *  创建 JobVertex
             */
            StreamConfig config = currentNodeId.equals(startNodeId) ? createJobVertex(startNodeId, chainInfo) : new StreamConfig(
                    new Configuration());

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            setVertexConfig(currentNodeId, config, chainableOutputs, nonChainableOutputs, chainInfo.getChainedSources());

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 遍历每个 OperatorChain 维护上下游 JobVertex 的关系
             */
            if (currentNodeId.equals(startNodeId)) {
                config.setChainStart();
                config.setChainIndex(chainIndex);
                config.setOperatorName(streamGraph.getStreamNode(currentNodeId).getOperatorName());

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 重点，链接上下游顶点 JobVertex，并且创建 IntermediateDataset 和 JobEdge 并且维护他们的关系
                 */
                for (StreamEdge edge : transitiveOutEdges) {

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释：
                     *  1、创建 IntermediateDataSet 和 JobEdge
                     *  2、维护 IntermediateDataSet 和 JobEdge 和 JobVertex 之间的关系
                     */
                    connect(startNodeId, edge);
                }

                config.setOutEdgesInOrder(transitiveOutEdges);
                config.setTransitiveChainedTaskConfigs(chainedConfigs.get(startNodeId));

            } else {
                chainedConfigs.computeIfAbsent(startNodeId, k -> new HashMap<Integer, StreamConfig>());

                config.setChainIndex(chainIndex);
                StreamNode node = streamGraph.getStreamNode(currentNodeId);
                config.setOperatorName(node.getOperatorName());
                chainedConfigs.get(startNodeId).put(currentNodeId, config);
            }

            config.setOperatorID(currentOperatorId);

            if (chainableOutputs.isEmpty()) {
                config.setChainEnd();
            }
            return transitiveOutEdges;

        } else {
            return new ArrayList<>();
        }
    }

    private InputOutputFormatContainer getOrCreateFormatContainer(Integer startNodeId) {
        return chainedInputOutputFormats.computeIfAbsent(startNodeId,
                k -> new InputOutputFormatContainer(Thread.currentThread().getContextClassLoader()));
    }

    private String createChainedName(Integer vertexID,
                                     List<StreamEdge> chainedOutputs,
                                     Optional<OperatorChainInfo> operatorChainInfo) {
        final String operatorName = nameWithChainedSourcesInfo(streamGraph.getStreamNode(vertexID).getOperatorName(),
                operatorChainInfo.map(chain -> chain.getChainedSources().values()).orElse(Collections.emptyList()));
        if (chainedOutputs.size() > 1) {
            List<String> outputChainedNames = new ArrayList<>();
            for (StreamEdge chainable : chainedOutputs) {
                outputChainedNames.add(chainedNames.get(chainable.getTargetId()));
            }
            return operatorName + " -> (" + StringUtils.join(outputChainedNames, ", ") + ")";
        } else if (chainedOutputs.size() == 1) {
            return operatorName + " -> " + chainedNames.get(chainedOutputs.get(0).getTargetId());
        } else {
            return operatorName;
        }
    }

    private ResourceSpec createChainedMinResources(Integer vertexID,
                                                   List<StreamEdge> chainedOutputs) {
        ResourceSpec minResources = streamGraph.getStreamNode(vertexID).getMinResources();
        for (StreamEdge chainable : chainedOutputs) {
            minResources = minResources.merge(chainedMinResources.get(chainable.getTargetId()));
        }
        return minResources;
    }

    private ResourceSpec createChainedPreferredResources(Integer vertexID,
                                                         List<StreamEdge> chainedOutputs) {
        ResourceSpec preferredResources = streamGraph.getStreamNode(vertexID).getPreferredResources();
        for (StreamEdge chainable : chainedOutputs) {
            preferredResources = preferredResources.merge(chainedPreferredResources.get(chainable.getTargetId()));
        }
        return preferredResources;
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 创建了饿一个 JobVertex 实例
     */
    private StreamConfig createJobVertex(Integer streamNodeId,
                                         OperatorChainInfo chainInfo) {

        JobVertex jobVertex;
        StreamNode streamNode = streamGraph.getStreamNode(streamNodeId);

        byte[] hash = chainInfo.getHash(streamNodeId);

        if (hash == null) {
            throw new IllegalStateException("Cannot find node hash. " + "Did you generate them before calling this method?");
        }

        // TODO_MA 马中华 注释：
        JobVertexID jobVertexId = new JobVertexID(hash);

        List<Tuple2<byte[], byte[]>> chainedOperators = chainInfo.getChainedOperatorHashes(streamNodeId);
        List<OperatorIDPair> operatorIDPairs = new ArrayList<>();
        if (chainedOperators != null) {
            for (Tuple2<byte[], byte[]> chainedOperator : chainedOperators) {
                OperatorID userDefinedOperatorID = chainedOperator.f1 == null ? null : new OperatorID(chainedOperator.f1);
                operatorIDPairs.add(OperatorIDPair.of(new OperatorID(chainedOperator.f0), userDefinedOperatorID));
            }
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        if (chainedInputOutputFormats.containsKey(streamNodeId)) {
            jobVertex = new InputOutputFormatVertex(chainedNames.get(streamNodeId), jobVertexId, operatorIDPairs);
            chainedInputOutputFormats.get(streamNodeId).write(new TaskConfig(jobVertex.getConfiguration()));
        } else {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            jobVertex = new JobVertex(chainedNames.get(streamNodeId), jobVertexId, operatorIDPairs);
        }

        for (OperatorCoordinator.Provider coordinatorProvider : chainInfo.getCoordinatorProviders()) {
            try {
                jobVertex.addOperatorCoordinator(new SerializedValue<>(coordinatorProvider));
            } catch (IOException e) {
                throw new FlinkRuntimeException(
                        String.format("Coordinator Provider for node %s is not serializable.", chainedNames.get(streamNodeId)), e);
            }
        }

        jobVertex.setResources(chainedMinResources.get(streamNodeId), chainedPreferredResources.get(streamNodeId));

        // TODO_MA 马中华 注释： 设置该顶点的 Task 对应的启动类
        jobVertex.setInvokableClass(streamNode.getJobVertexClass());

        // TODO_MA 马中华 注释： 并行度
        int parallelism = streamNode.getParallelism();
        if (parallelism > 0) {
            jobVertex.setParallelism(parallelism);
        } else {
            parallelism = jobVertex.getParallelism();
        }
        jobVertex.setMaxParallelism(streamNode.getMaxParallelism());

        if (LOG.isDebugEnabled()) {
            LOG.debug("Parallelism set: {} for {}", parallelism, streamNodeId);
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        jobVertices.put(streamNodeId, jobVertex);
        builtVertices.add(streamNodeId);
        jobGraph.addVertex(jobVertex);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return new StreamConfig(jobVertex.getConfiguration());
    }

    private void setVertexConfig(Integer vertexID,
                                 StreamConfig config,
                                 List<StreamEdge> chainableOutputs,
                                 List<StreamEdge> nonChainableOutputs,
                                 Map<Integer, ChainedSourceInfo> chainedSources) {

        StreamNode vertex = streamGraph.getStreamNode(vertexID);

        config.setVertexID(vertexID);

        // build the inputs as a combination of source and network inputs
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        final List<StreamEdge> inEdges = vertex.getInEdges();

        final TypeSerializer<?>[] inputSerializers = vertex.getTypeSerializersIn();

        final StreamConfig.InputConfig[] inputConfigs = new StreamConfig.InputConfig[inputSerializers.length];

        int inputGateCount = 0;

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        for (final StreamEdge inEdge : inEdges) {
            final ChainedSourceInfo chainedSource = chainedSources.get(inEdge.getSourceId());

            final int inputIndex = inEdge.getTypeNumber() == 0 ? 0 // single input operator
                    : inEdge.getTypeNumber() - 1; // in case of 2 or more inputs

            if (chainedSource != null) {
                // chained source is the input
                if (inputConfigs[inputIndex] != null) {
                    throw new IllegalStateException("Trying to union a chained source with another input.");
                }
                inputConfigs[inputIndex] = chainedSource.getInputConfig();
                chainedConfigs.computeIfAbsent(vertexID, (key) -> new HashMap<>())
                        .put(inEdge.getSourceId(), chainedSource.getOperatorConfig());
            } else {
                // network input. null if we move to a new input, non-null if this is a further edge
                // that is union-ed into the same input
                if (inputConfigs[inputIndex] == null) {
                    // PASS_THROUGH is a sensible default for streaming jobs. Only for BATCH
                    // execution can we have sorted inputs
                    StreamConfig.InputRequirement inputRequirement = vertex.getInputRequirements()
                            .getOrDefault(inputIndex, StreamConfig.InputRequirement.PASS_THROUGH);
                    inputConfigs[inputIndex] = new StreamConfig.NetworkInputConfig(inputSerializers[inputIndex], inputGateCount++,
                            inputRequirement);
                }
            }
        }
        config.setInputs(inputConfigs);

        config.setTypeSerializerOut(vertex.getTypeSerializerOut());

        // iterate edges, find sideOutput edges create and save serializers for each outputTag type
        for (StreamEdge edge : chainableOutputs) {
            if (edge.getOutputTag() != null) {
                config.setTypeSerializerSideOut(edge.getOutputTag(),
                        edge.getOutputTag().getTypeInfo().createSerializer(streamGraph.getExecutionConfig()));
            }
        }
        for (StreamEdge edge : nonChainableOutputs) {
            if (edge.getOutputTag() != null) {
                config.setTypeSerializerSideOut(edge.getOutputTag(),
                        edge.getOutputTag().getTypeInfo().createSerializer(streamGraph.getExecutionConfig()));
            }
        }

        config.setStreamOperatorFactory(vertex.getOperatorFactory());

        config.setNumberOfOutputs(nonChainableOutputs.size());
        config.setNonChainedOutputs(nonChainableOutputs);
        config.setChainedOutputs(chainableOutputs);

        config.setTimeCharacteristic(streamGraph.getTimeCharacteristic());

        final CheckpointConfig checkpointCfg = streamGraph.getCheckpointConfig();

        config.setStateBackend(streamGraph.getStateBackend());
        config.setChangelogStateBackendEnabled(streamGraph.isChangelogStateBackendEnabled());
        config.setCheckpointStorage(streamGraph.getCheckpointStorage());
        config.setSavepointDir(streamGraph.getSavepointDirectory());
        config.setGraphContainingLoops(streamGraph.isIterative());
        config.setTimerServiceProvider(streamGraph.getTimerServiceProvider());
        config.setCheckpointingEnabled(checkpointCfg.isCheckpointingEnabled());
        config.getConfiguration().set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH,
                streamGraph.isEnableCheckpointsAfterTasksFinish());
        config.setCheckpointMode(getCheckpointingMode(checkpointCfg));
        config.setUnalignedCheckpointsEnabled(checkpointCfg.isUnalignedCheckpointsEnabled());
        config.setAlignedCheckpointTimeout(checkpointCfg.getAlignedCheckpointTimeout());

        for (int i = 0; i < vertex.getStatePartitioners().length; i++) {
            config.setStatePartitioner(i, vertex.getStatePartitioners()[i]);
        }
        config.setStateKeySerializer(vertex.getStateKeySerializer());

        Class<? extends TaskInvokable> vertexClass = vertex.getJobVertexClass();

        if (vertexClass.equals(StreamIterationHead.class) || vertexClass.equals(StreamIterationTail.class)) {
            config.setIterationId(streamGraph.getBrokerID(vertexID));
            config.setIterationWaitTime(streamGraph.getLoopTimeout(vertexID));
        }

        vertexConfigs.put(vertexID, config);
    }

    private CheckpointingMode getCheckpointingMode(CheckpointConfig checkpointConfig) {
        CheckpointingMode checkpointingMode = checkpointConfig.getCheckpointingMode();

        checkArgument(checkpointingMode == CheckpointingMode.EXACTLY_ONCE || checkpointingMode == CheckpointingMode.AT_LEAST_ONCE,
                "Unexpected checkpointing mode.");

        if (checkpointConfig.isCheckpointingEnabled()) {
            return checkpointingMode;
        } else {
            // the "at-least-once" input handler is slightly cheaper (in the absence of
            // checkpoints),
            // so we use that one if checkpointing is not enabled
            return CheckpointingMode.AT_LEAST_ONCE;
        }
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 上下游顶点链接起来
     */
    private void connect(Integer headOfChain,
                         StreamEdge edge) {

        physicalEdgesInOrder.add(edge);

        Integer downStreamVertexID = edge.getTargetId();

        // TODO_MA 马中华 注释： 上下游顶点
        JobVertex headVertex = jobVertices.get(headOfChain);
        JobVertex downStreamVertex = jobVertices.get(downStreamVertexID);

        StreamConfig downStreamConfig = new StreamConfig(downStreamVertex.getConfiguration());

        downStreamConfig.setNumberOfNetworkInputs(downStreamConfig.getNumberOfNetworkInputs() + 1);

        // TODO_MA 马中华 注释： 获取流分区器
        StreamPartitioner<?> partitioner = edge.getPartitioner();

        ResultPartitionType resultPartitionType;
        switch (edge.getExchangeMode()) {
            case PIPELINED:
                resultPartitionType = ResultPartitionType.PIPELINED_BOUNDED;
                break;
            case BATCH:
                resultPartitionType = ResultPartitionType.BLOCKING;
                break;
            case UNDEFINED:
                resultPartitionType = determineResultPartitionType(partitioner);
                break;
            default:
                throw new UnsupportedOperationException("Data exchange mode " + edge.getExchangeMode() + " is not supported yet.");
        }

        checkAndResetBufferTimeout(resultPartitionType, edge);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：  做了两件重要的事情：
         *  1、创建 IntermediateDataSet 和 JobEdge
         *  2、维护 JobVertex IntermediateDataSet JobEdge 三者的关系
         */
        JobEdge jobEdge;
        // TODO_MA 马中华 注释： 判断是否是 Pointwise, 只有 forward 和 rescale 是 true
        if (partitioner.isPointwise()) {
            jobEdge = downStreamVertex.connectNewDataSetAsInput(headVertex, DistributionPattern.POINTWISE, resultPartitionType);
        } else {
            jobEdge = downStreamVertex.connectNewDataSetAsInput(headVertex, DistributionPattern.ALL_TO_ALL, resultPartitionType);
        }

        // set strategy name so that web interface can show it.
        jobEdge.setShipStrategyName(partitioner.toString());
        jobEdge.setDownstreamSubtaskStateMapper(partitioner.getDownstreamSubtaskStateMapper());
        jobEdge.setUpstreamSubtaskStateMapper(partitioner.getUpstreamSubtaskStateMapper());

        if (LOG.isDebugEnabled()) {
            LOG.debug("CONNECTED: {} - {} -> {}", partitioner.getClass().getSimpleName(), headOfChain, downStreamVertexID);
        }
    }

    private void checkAndResetBufferTimeout(ResultPartitionType type,
                                            StreamEdge edge) {
        long bufferTimeout = edge.getBufferTimeout();
        if (type.isBlocking() && bufferTimeout != UNDEFINED_NETWORK_BUFFER_TIMEOUT) {
            throw new UnsupportedOperationException(
                    "Blocking partition does not support buffer timeout " + bufferTimeout + " for src operator in edge " + edge.toString() + ". \nPlease either reset buffer timeout as -1 or use the non-blocking partition.");
        }

        if (type.isPipelined() && bufferTimeout == UNDEFINED_NETWORK_BUFFER_TIMEOUT) {
            edge.setBufferTimeout(DEFAULT_NETWORK_BUFFER_TIMEOUT);
        }
    }

    private ResultPartitionType determineResultPartitionType(StreamPartitioner<?> partitioner) {
        switch (streamGraph.getGlobalStreamExchangeMode()) {
            case ALL_EDGES_BLOCKING:
                return ResultPartitionType.BLOCKING;
            case FORWARD_EDGES_PIPELINED:
                if (partitioner instanceof ForwardPartitioner) {
                    return ResultPartitionType.PIPELINED_BOUNDED;
                } else {
                    return ResultPartitionType.BLOCKING;
                }
            case POINTWISE_EDGES_PIPELINED:
                if (partitioner.isPointwise()) {
                    return ResultPartitionType.PIPELINED_BOUNDED;
                } else {
                    return ResultPartitionType.BLOCKING;
                }
            case ALL_EDGES_PIPELINED:
                return ResultPartitionType.PIPELINED_BOUNDED;
            case ALL_EDGES_PIPELINED_APPROXIMATE:
                return ResultPartitionType.PIPELINED_APPROXIMATE;
            default:
                throw new RuntimeException("Unrecognized global data exchange mode " + streamGraph.getGlobalStreamExchangeMode());
        }
    }

    // TODO_MA 马中华 注释： 判断的是： 当前顶点，和 下游一个顶点之间的边 StreamEdge
    public static boolean isChainable(StreamEdge edge,
                                      StreamGraph streamGraph) {

        // TODO_MA 马中华 注释： 获取下游顶点
        StreamNode downStreamVertex = streamGraph.getTargetVertex(edge);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 1、下游节点的入度为1 （也就是说下游节点没有来自其他节点的输入）
         *  判断下游顶点的入边只有一个： 上游顶点的所有数据，都输出给上下游顶点。 forward
         */
        return downStreamVertex.getInEdges().size() == 1 && isChainableInput(edge, streamGraph);
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：
     */
    private static boolean isChainableInput(StreamEdge edge,
                                            StreamGraph streamGraph) {

        // TODO_MA 马中华 注释： 获取上下游顶点
        StreamNode upStreamVertex = streamGraph.getSourceVertex(edge);
        StreamNode downStreamVertex = streamGraph.getTargetVertex(edge);

        // TODO_MA 马中华 注释： 如果不满足这 9 个条件中的，任何一条，就会返回 false
        // TODO_MA 马中华 注释： 2、上下游节点都在同一个 slot group 中
        if (!(upStreamVertex.isSameSlotSharingGroup(downStreamVertex)
                // TODO_MA 马中华 注释： 3 4 5 三个条件
                && areOperatorsChainable(upStreamVertex, downStreamVertex, streamGraph)
                // TODO_MA 马中华 注释： 6、两个节点间物理分区逻辑是 ForwardPartitioner
                && (edge.getPartitioner() instanceof ForwardPartitioner)
                // TODO_MA 马中华 注释： 7、两个算子间的 shuffle 方式不等于批处理模式
                && edge.getExchangeMode() != StreamExchangeMode.BATCH
                // TODO_MA 马中华 注释： 8、上下游的并行度一致
                && upStreamVertex.getParallelism() == downStreamVertex.getParallelism()
                // TODO_MA 马中华 注释： 9、用户没有禁用 chain
                && streamGraph.isChainingEnabled()
        )) {
            // TODO_MA 马中华 注释： 当 if(!(A)) 条件中的 (A) 为 false 的时候，最终表示不 可 chain
            // TODO_MA 马中华 注释： 需要当所有条件都满足的情况，然后才能执行 chain 操作
            return false;
        }

        // check that we do not have a union operation, because unions currently only work
        // through the network/byte-channel stack.
        // we check that by testing that each "type" (which means input position) is used only once
        for (StreamEdge inEdge : downStreamVertex.getInEdges()) {
            if (inEdge != edge && inEdge.getTypeNumber() == edge.getTypeNumber()) {
                return false;
            }
        }
        return true;
    }

    @VisibleForTesting
    static boolean areOperatorsChainable(StreamNode upStreamVertex,
                                         StreamNode downStreamVertex,
                                         StreamGraph streamGraph) {

        // TODO_MA 马中华 注释： 获取上下游 Operator
        StreamOperatorFactory<?> upStreamOperator = upStreamVertex.getOperatorFactory();
        StreamOperatorFactory<?> downStreamOperator = downStreamVertex.getOperatorFactory();

        // TODO_MA 马中华 注释： // TODO_MA 马中华 注释： 3、前后算子不为空
        if (downStreamOperator == null || upStreamOperator == null) {
            return false;
        }

        // yielding operators cannot be chained to legacy sources
        // unfortunately the information that vertices have been chained is not preserved at this point
        // TODO_MA 马中华 注释： 条件10
        if (downStreamOperator instanceof YieldingOperatorFactory && getHeadOperator(upStreamVertex,
                streamGraph).isLegacySource()) {
            return false;
        }

        // we use switch/case here to make sure this is exhaustive if ever values are added to the
        // ChainingStrategy enum
        boolean isChainable;

        // TODO_MA 马中华 注释： 4、上游节点的 chain 策略为 ALWAYS 或 HEAD（只能与下游链接，不能与上游链接，Source 默认是 HEAD）
        switch (upStreamOperator.getChainingStrategy()) {
            case NEVER:
                isChainable = false;
                break;
            case ALWAYS:
            case HEAD:
            case HEAD_WITH_SOURCES:
                isChainable = true;
                break;
            default:
                throw new RuntimeException("Unknown chaining strategy: " + upStreamOperator.getChainingStrategy());
        }

        // TODO_MA 马中华 注释： 5、下游节点的 chain 策略为 ALWAYS（可以与上下游链接，map、flatmap、filter 等默认是 ALWAYS）
        switch (downStreamOperator.getChainingStrategy()) {
            case NEVER:
            case HEAD:
                isChainable = false;
                break;
            case ALWAYS:
                // keep the value from upstream
                break;
            case HEAD_WITH_SOURCES:
                // only if upstream is a source
                isChainable &= (upStreamOperator instanceof SourceOperatorFactory);
                break;
            default:
                throw new RuntimeException("Unknown chaining strategy: " + upStreamOperator.getChainingStrategy());
        }

        return isChainable;
    }

    /** Backtraces the head of an operator chain. */
    private static StreamOperatorFactory<?> getHeadOperator(StreamNode upStreamVertex,
                                                            StreamGraph streamGraph) {
        if (upStreamVertex.getInEdges().size() == 1 && isChainable(upStreamVertex.getInEdges().get(0), streamGraph)) {
            return getHeadOperator(streamGraph.getSourceVertex(upStreamVertex.getInEdges().get(0)), streamGraph);
        }
        return upStreamVertex.getOperatorFactory();
    }

    private void setSlotSharingAndCoLocation() {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        setSlotSharing();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        setCoLocation();
    }

    private void setSlotSharing() {
        final Map<String, SlotSharingGroup> specifiedSlotSharingGroups = new HashMap<>();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        final Map<JobVertexID, SlotSharingGroup> vertexRegionSlotSharingGroups = buildVertexRegionSlotSharingGroups();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        for (Map.Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {

            final JobVertex vertex = entry.getValue();
            final String slotSharingGroupKey = streamGraph.getStreamNode(entry.getKey()).getSlotSharingGroup();

            checkNotNull(slotSharingGroupKey, "StreamNode slot sharing group must not be null");

            final SlotSharingGroup effectiveSlotSharingGroup;
            if (slotSharingGroupKey.equals(StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP)) {
                // fallback to the region slot sharing group by default
                effectiveSlotSharingGroup = checkNotNull(vertexRegionSlotSharingGroups.get(vertex.getID()));
            } else {
                effectiveSlotSharingGroup = specifiedSlotSharingGroups.computeIfAbsent(slotSharingGroupKey, k -> {
                    SlotSharingGroup ssg = new SlotSharingGroup();
                    streamGraph.getSlotSharingGroupResource(k).ifPresent(ssg::setResourceProfile);
                    return ssg;
                });
            }

            vertex.setSlotSharingGroup(effectiveSlotSharingGroup);
        }
    }

    /**
     * Maps a vertex to its region slot sharing group. If {@link
     * StreamGraph#isAllVerticesInSameSlotSharingGroupByDefault()} returns true, all regions will be
     * in the same slot sharing group.
     */
    private Map<JobVertexID, SlotSharingGroup> buildVertexRegionSlotSharingGroups() {

        final Map<JobVertexID, SlotSharingGroup> vertexRegionSlotSharingGroups = new HashMap<>();
        final SlotSharingGroup defaultSlotSharingGroup = new SlotSharingGroup();

        streamGraph.getSlotSharingGroupResource(StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP)
                .ifPresent(defaultSlotSharingGroup::setResourceProfile);

        final boolean allRegionsInSameSlotSharingGroup = streamGraph.isAllVerticesInSameSlotSharingGroupByDefault();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        final Iterable<DefaultLogicalPipelinedRegion> regions = DefaultLogicalTopology.fromJobGraph(jobGraph)
                .getAllPipelinedRegions();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        for (DefaultLogicalPipelinedRegion region : regions) {
            final SlotSharingGroup regionSlotSharingGroup;
            if (allRegionsInSameSlotSharingGroup) {
                regionSlotSharingGroup = defaultSlotSharingGroup;
            } else {
                regionSlotSharingGroup = new SlotSharingGroup();
                streamGraph.getSlotSharingGroupResource(StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP)
                        .ifPresent(regionSlotSharingGroup::setResourceProfile);
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            for (LogicalVertex vertex : region.getVertices()) {
                vertexRegionSlotSharingGroups.put(vertex.getId(), regionSlotSharingGroup);
            }
        }

        return vertexRegionSlotSharingGroups;
    }

    private void setCoLocation() {

        // TODO_MA 马中华 注释： 结果容器
        final Map<String, Tuple2<SlotSharingGroup, CoLocationGroupImpl>> coLocationGroups = new HashMap<>();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        for (Map.Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {

            final StreamNode node = streamGraph.getStreamNode(entry.getKey());
            final JobVertex vertex = entry.getValue();
            final SlotSharingGroup sharingGroup = vertex.getSlotSharingGroup();

            // configure co-location constraint
            final String coLocationGroupKey = node.getCoLocationGroup();
            if (coLocationGroupKey != null) {
                if (sharingGroup == null) {
                    throw new IllegalStateException("Cannot use a co-location constraint without a slot sharing group");
                }

                Tuple2<SlotSharingGroup, CoLocationGroupImpl> constraint = coLocationGroups.computeIfAbsent(coLocationGroupKey,
                        k -> new Tuple2<>(sharingGroup, new CoLocationGroupImpl()));

                if (constraint.f0 != sharingGroup) {
                    throw new IllegalStateException("Cannot co-locate operators from different slot sharing groups");
                }

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释：
                 */
                vertex.updateCoLocationGroup(constraint.f1);
                constraint.f1.addVertex(vertex);
            }
        }
    }

    private static void setManagedMemoryFraction(final Map<Integer, JobVertex> jobVertices,
                                                 final Map<Integer, StreamConfig> operatorConfigs,
                                                 final Map<Integer, Map<Integer, StreamConfig>> vertexChainedConfigs,
                                                 final java.util.function.Function<Integer, Map<ManagedMemoryUseCase, Integer>> operatorScopeManagedMemoryUseCaseWeightsRetriever,
                                                 final java.util.function.Function<Integer, Set<ManagedMemoryUseCase>> slotScopeManagedMemoryUseCasesRetriever) {

        // all slot sharing groups in this job
        final Set<SlotSharingGroup> slotSharingGroups = Collections.newSetFromMap(new IdentityHashMap<>());

        // maps a job vertex ID to its head operator ID
        final Map<JobVertexID, Integer> vertexHeadOperators = new HashMap<>();

        // maps a job vertex ID to IDs of all operators in the vertex
        final Map<JobVertexID, Set<Integer>> vertexOperators = new HashMap<>();

        for (Map.Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {
            final int headOperatorId = entry.getKey();
            final JobVertex jobVertex = entry.getValue();

            final SlotSharingGroup jobVertexSlotSharingGroup = jobVertex.getSlotSharingGroup();

            checkState(jobVertexSlotSharingGroup != null, "JobVertex slot sharing group must not be null");
            slotSharingGroups.add(jobVertexSlotSharingGroup);

            vertexHeadOperators.put(jobVertex.getID(), headOperatorId);

            final Set<Integer> operatorIds = new HashSet<>();
            operatorIds.add(headOperatorId);
            operatorIds.addAll(vertexChainedConfigs.getOrDefault(headOperatorId, Collections.emptyMap()).keySet());
            vertexOperators.put(jobVertex.getID(), operatorIds);
        }

        for (SlotSharingGroup slotSharingGroup : slotSharingGroups) {
            setManagedMemoryFractionForSlotSharingGroup(slotSharingGroup, vertexHeadOperators, vertexOperators, operatorConfigs,
                    vertexChainedConfigs, operatorScopeManagedMemoryUseCaseWeightsRetriever,
                    slotScopeManagedMemoryUseCasesRetriever);
        }
    }

    private static void setManagedMemoryFractionForSlotSharingGroup(final SlotSharingGroup slotSharingGroup,
                                                                    final Map<JobVertexID, Integer> vertexHeadOperators,
                                                                    final Map<JobVertexID, Set<Integer>> vertexOperators,
                                                                    final Map<Integer, StreamConfig> operatorConfigs,
                                                                    final Map<Integer, Map<Integer, StreamConfig>> vertexChainedConfigs,
                                                                    final java.util.function.Function<Integer, Map<ManagedMemoryUseCase, Integer>> operatorScopeManagedMemoryUseCaseWeightsRetriever,
                                                                    final java.util.function.Function<Integer, Set<ManagedMemoryUseCase>> slotScopeManagedMemoryUseCasesRetriever) {

        final Set<Integer> groupOperatorIds = slotSharingGroup.getJobVertexIds().stream()
                .flatMap((vid) -> vertexOperators.get(vid).stream()).collect(Collectors.toSet());

        final Map<ManagedMemoryUseCase, Integer> groupOperatorScopeUseCaseWeights = groupOperatorIds.stream()
                .flatMap((oid) -> operatorScopeManagedMemoryUseCaseWeightsRetriever.apply(oid).entrySet().stream())
                .collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.summingInt(Map.Entry::getValue)));

        final Set<ManagedMemoryUseCase> groupSlotScopeUseCases = groupOperatorIds.stream()
                .flatMap((oid) -> slotScopeManagedMemoryUseCasesRetriever.apply(oid).stream()).collect(Collectors.toSet());

        for (JobVertexID jobVertexID : slotSharingGroup.getJobVertexIds()) {
            for (int operatorNodeId : vertexOperators.get(jobVertexID)) {
                final StreamConfig operatorConfig = operatorConfigs.get(operatorNodeId);
                final Map<ManagedMemoryUseCase, Integer> operatorScopeUseCaseWeights = operatorScopeManagedMemoryUseCaseWeightsRetriever.apply(
                        operatorNodeId);
                final Set<ManagedMemoryUseCase> slotScopeUseCases = slotScopeManagedMemoryUseCasesRetriever.apply(operatorNodeId);
                setManagedMemoryFractionForOperator(operatorScopeUseCaseWeights, slotScopeUseCases,
                        groupOperatorScopeUseCaseWeights, groupSlotScopeUseCases, operatorConfig);
            }

            // need to refresh the chained task configs because they are serialized
            final int headOperatorNodeId = vertexHeadOperators.get(jobVertexID);
            final StreamConfig vertexConfig = operatorConfigs.get(headOperatorNodeId);
            vertexConfig.setTransitiveChainedTaskConfigs(vertexChainedConfigs.get(headOperatorNodeId));
        }
    }

    private static void setManagedMemoryFractionForOperator(final Map<ManagedMemoryUseCase, Integer> operatorScopeUseCaseWeights,
                                                            final Set<ManagedMemoryUseCase> slotScopeUseCases,
                                                            final Map<ManagedMemoryUseCase, Integer> groupManagedMemoryWeights,
                                                            final Set<ManagedMemoryUseCase> groupSlotScopeUseCases,
                                                            final StreamConfig operatorConfig) {

        // For each operator, make sure fractions are set for all use cases in the group, even if
        // the operator does not have the use case (set the fraction to 0.0). This allows us to
        // learn which use cases exist in the group from either one of the stream configs.
        for (Map.Entry<ManagedMemoryUseCase, Integer> entry : groupManagedMemoryWeights.entrySet()) {
            final ManagedMemoryUseCase useCase = entry.getKey();
            final int groupWeight = entry.getValue();
            final int operatorWeight = operatorScopeUseCaseWeights.getOrDefault(useCase, 0);
            operatorConfig.setManagedMemoryFractionOperatorOfUseCase(useCase,
                    operatorWeight > 0 ? ManagedMemoryUtils.getFractionRoundedDown(operatorWeight, groupWeight) : 0.0);
        }
        for (ManagedMemoryUseCase useCase : groupSlotScopeUseCases) {
            operatorConfig.setManagedMemoryFractionOperatorOfUseCase(useCase, slotScopeUseCases.contains(useCase) ? 1.0 : 0.0);
        }
    }

    private void configureCheckpointing() {

        // TODO_MA 马中华 注释：
        CheckpointConfig cfg = streamGraph.getCheckpointConfig();

        long interval = cfg.getCheckpointInterval();
        if (interval < MINIMAL_CHECKPOINT_TIME) {
            // interval of max value means disable periodic checkpoint
            interval = Long.MAX_VALUE;
        }

        //  --- configure options ---

        CheckpointRetentionPolicy retentionAfterTermination;
        if (cfg.isExternalizedCheckpointsEnabled()) {
            CheckpointConfig.ExternalizedCheckpointCleanup cleanup = cfg.getExternalizedCheckpointCleanup();
            // Sanity check
            if (cleanup == null) {
                throw new IllegalStateException("Externalized checkpoints enabled, but no cleanup mode configured.");
            }
            retentionAfterTermination = cleanup.deleteOnCancellation() ? CheckpointRetentionPolicy.RETAIN_ON_FAILURE : CheckpointRetentionPolicy.RETAIN_ON_CANCELLATION;
        } else {
            retentionAfterTermination = CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION;
        }

        //  --- configure the master-side checkpoint hooks ---

        final ArrayList<MasterTriggerRestoreHook.Factory> hooks = new ArrayList<>();

        for (StreamNode node : streamGraph.getStreamNodes()) {
            if (node.getOperatorFactory() instanceof UdfStreamOperatorFactory) {
                Function f = ((UdfStreamOperatorFactory) node.getOperatorFactory()).getUserFunction();

                if (f instanceof WithMasterCheckpointHook) {
                    hooks.add(new FunctionMasterCheckpointHookFactory((WithMasterCheckpointHook<?>) f));
                }
            }
        }

        // because the hooks can have user-defined code, they need to be stored as
        // eagerly serialized values
        final SerializedValue<MasterTriggerRestoreHook.Factory[]> serializedHooks;
        if (hooks.isEmpty()) {
            serializedHooks = null;
        } else {
            try {
                MasterTriggerRestoreHook.Factory[] asArray = hooks.toArray(new MasterTriggerRestoreHook.Factory[hooks.size()]);
                serializedHooks = new SerializedValue<>(asArray);
            } catch (IOException e) {
                throw new FlinkRuntimeException("Trigger/restore hook is not serializable", e);
            }
        }

        // because the state backend can have user-defined code, it needs to be stored as
        // eagerly serialized value
        final SerializedValue<StateBackend> serializedStateBackend;
        if (streamGraph.getStateBackend() == null) {
            serializedStateBackend = null;
        } else {
            try {
                serializedStateBackend = new SerializedValue<StateBackend>(streamGraph.getStateBackend());
            } catch (IOException e) {
                throw new FlinkRuntimeException("State backend is not serializable", e);
            }
        }

        // because the checkpoint storage can have user-defined code, it needs to be stored as
        // eagerly serialized value
        final SerializedValue<CheckpointStorage> serializedCheckpointStorage;
        if (streamGraph.getCheckpointStorage() == null) {
            serializedCheckpointStorage = null;
        } else {
            try {
                serializedCheckpointStorage = new SerializedValue<>(streamGraph.getCheckpointStorage());
            } catch (IOException e) {
                throw new FlinkRuntimeException("Checkpoint storage is not serializable", e);
            }
        }

        //  --- done, put it all together ---

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        JobCheckpointingSettings settings = new JobCheckpointingSettings(
                CheckpointCoordinatorConfiguration.builder().setCheckpointInterval(interval)
                        .setCheckpointTimeout(cfg.getCheckpointTimeout())
                        .setMinPauseBetweenCheckpoints(cfg.getMinPauseBetweenCheckpoints())
                        .setMaxConcurrentCheckpoints(cfg.getMaxConcurrentCheckpoints())
                        .setCheckpointRetentionPolicy(retentionAfterTermination)
                        .setExactlyOnce(getCheckpointingMode(cfg) == CheckpointingMode.EXACTLY_ONCE)
                        .setTolerableCheckpointFailureNumber(cfg.getTolerableCheckpointFailureNumber())
                        .setUnalignedCheckpointsEnabled(cfg.isUnalignedCheckpointsEnabled())
                        .setCheckpointIdOfIgnoredInFlightData(cfg.getCheckpointIdOfIgnoredInFlightData())
                        .setAlignedCheckpointTimeout(cfg.getAlignedCheckpointTimeout().toMillis())
                        .setEnableCheckpointsAfterTasksFinish(streamGraph.isEnableCheckpointsAfterTasksFinish()).build(),
                serializedStateBackend, streamGraph.isChangelogStateBackendEnabled(), serializedCheckpointStorage, serializedHooks);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 将 JobCheckpointingSettings 存储在 JobGraph 中
         */
        jobGraph.setSnapshotSettings(settings);
    }

    private static String nameWithChainedSourcesInfo(String operatorName,
                                                     Collection<ChainedSourceInfo> chainedSourceInfos) {
        return chainedSourceInfos.isEmpty() ? operatorName : String.format("%s [%s]", operatorName,
                chainedSourceInfos.stream().map(chainedSourceInfo -> chainedSourceInfo.getOperatorConfig().getOperatorName())
                        .collect(Collectors.joining(", ")));
    }

    /**
     * A private class to help maintain the information of an operator chain during the recursive
     * call in {@link #createChain(Integer, int, OperatorChainInfo, Map)}.
     */
    private static class OperatorChainInfo {
        private final Integer startNodeId;
        private final Map<Integer, byte[]> hashes;
        private final List<Map<Integer, byte[]>> legacyHashes;
        private final Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes;
        private final Map<Integer, ChainedSourceInfo> chainedSources;
        private final List<OperatorCoordinator.Provider> coordinatorProviders;
        private final StreamGraph streamGraph;

        private OperatorChainInfo(int startNodeId,
                                  Map<Integer, byte[]> hashes,
                                  List<Map<Integer, byte[]>> legacyHashes,
                                  Map<Integer, ChainedSourceInfo> chainedSources,
                                  StreamGraph streamGraph) {
            this.startNodeId = startNodeId;
            this.hashes = hashes;
            this.legacyHashes = legacyHashes;
            this.chainedOperatorHashes = new HashMap<>();
            this.coordinatorProviders = new ArrayList<>();
            this.chainedSources = chainedSources;
            this.streamGraph = streamGraph;
        }

        byte[] getHash(Integer streamNodeId) {
            return hashes.get(streamNodeId);
        }

        private Integer getStartNodeId() {
            return startNodeId;
        }

        private List<Tuple2<byte[], byte[]>> getChainedOperatorHashes(int startNodeId) {
            return chainedOperatorHashes.get(startNodeId);
        }

        void addCoordinatorProvider(OperatorCoordinator.Provider coordinator) {
            coordinatorProviders.add(coordinator);
        }

        private List<OperatorCoordinator.Provider> getCoordinatorProviders() {
            return coordinatorProviders;
        }

        Map<Integer, ChainedSourceInfo> getChainedSources() {
            return chainedSources;
        }

        private OperatorID addNodeToChain(int currentNodeId,
                                          String operatorName) {
            List<Tuple2<byte[], byte[]>> operatorHashes = chainedOperatorHashes.computeIfAbsent(startNodeId,
                    k -> new ArrayList<>());

            byte[] primaryHashBytes = hashes.get(currentNodeId);

            for (Map<Integer, byte[]> legacyHash : legacyHashes) {
                operatorHashes.add(new Tuple2<>(primaryHashBytes, legacyHash.get(currentNodeId)));
            }

            streamGraph.getStreamNode(currentNodeId).getCoordinatorProvider(operatorName, new OperatorID(getHash(currentNodeId)))
                    .map(coordinatorProviders::add);
            return new OperatorID(primaryHashBytes);
        }

        private OperatorChainInfo newChain(Integer startNodeId) {
            return new OperatorChainInfo(startNodeId, hashes, legacyHashes, chainedSources, streamGraph);
        }
    }

    private static final class ChainedSourceInfo {
        private final StreamConfig operatorConfig;
        private final StreamConfig.SourceInputConfig inputConfig;

        ChainedSourceInfo(StreamConfig operatorConfig,
                          StreamConfig.SourceInputConfig inputConfig) {
            this.operatorConfig = operatorConfig;
            this.inputConfig = inputConfig;
        }

        public StreamConfig getOperatorConfig() {
            return operatorConfig;
        }

        public StreamConfig.SourceInputConfig getInputConfig() {
            return inputConfig;
        }
    }
}
