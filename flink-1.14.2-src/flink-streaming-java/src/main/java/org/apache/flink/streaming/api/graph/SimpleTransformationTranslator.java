/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.util.graph.StreamGraphUtils;

import java.util.Collection;

import static org.apache.flink.runtime.util.config.memory.ManagedMemoryUtils.validateUseCaseWeightsNotConflict;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A base class for all {@link TransformationTranslator TransformationTranslators} who translate
 * {@link Transformation Transformations} that have a single operator in their runtime
 * implementation. These include most of the currently supported operations.
 *
 * @param <OUT> The type of the output elements of the transformation being translated.
 * @param <T> The type of transformation being translated.
 */
@Internal
public abstract class SimpleTransformationTranslator<OUT, T extends Transformation<OUT>> implements TransformationTranslator<OUT, T> {

    @Override
    public final Collection<Integer> translateForBatch(final T transformation,
                                                       final Context context) {
        checkNotNull(transformation);
        checkNotNull(context);

        final Collection<Integer> transformedIds = translateForBatchInternal(transformation, context);
        configure(transformation, context);

        return transformedIds;
    }

    @Override
    public final Collection<Integer> translateForStreaming(final T transformation,
                                                           final Context context) {
        checkNotNull(transformation);
        checkNotNull(context);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： translateForStreamingInternal 是一个父类方法，必然不同的实现对应到不同的子类
         *  你先关注三个：
         *  1、SourceTransformation
         *  2、OneInputTransformation  一个输入
         *  3、TwoInputTransformation  两个输入
         *  -
         *  不同的 transformation 对应到的 Task 的启动类是不一样
         *  1、 SourceStreakTask
         *  2、 OneInputStreamTask
         *  3、 TwoInputStreamTask
         */
        // TODO_MA 马中华 注释： ds3 = ds1.join(ds2)
        final Collection<Integer> transformedIds = translateForStreamingInternal(transformation, context);
        configure(transformation, context);

        return transformedIds;
    }

    /**
     * Translates a given {@link Transformation} to its runtime implementation for BATCH-style
     * execution.
     *
     * @param transformation The transformation to be translated.
     * @param context The translation context.
     *
     * @return The ids of the "last" {@link StreamNode StreamNodes} in the transformation graph
     *         corresponding to this transformation. These will be the nodes that a potential following
     *         transformation will need to connect to.
     */
    protected abstract Collection<Integer> translateForBatchInternal(final T transformation,
                                                                     final Context context);

    /**
     * Translates a given {@link Transformation} to its runtime implementation for STREAMING-style
     * execution.
     *
     * @param transformation The transformation to be translated.
     * @param context The translation context.
     *
     * @return The ids of the "last" {@link StreamNode StreamNodes} in the transformation graph
     *         corresponding to this transformation. These will be the nodes that a potential following
     *         transformation will need to connect to.
     */
    protected abstract Collection<Integer> translateForStreamingInternal(final T transformation,
                                                                         final Context context);

    private void configure(final T transformation,
                           final Context context) {
        final StreamGraph streamGraph = context.getStreamGraph();
        final int transformationId = transformation.getId();

        StreamGraphUtils.configureBufferTimeout(streamGraph, transformationId, transformation, context.getDefaultBufferTimeout());

        if (transformation.getUid() != null) {
            streamGraph.setTransformationUID(transformationId, transformation.getUid());
        }
        if (transformation.getUserProvidedNodeHash() != null) {
            streamGraph.setTransformationUserHash(transformationId, transformation.getUserProvidedNodeHash());
        }

        StreamGraphUtils.validateTransformationUid(streamGraph, transformation);

        if (transformation.getMinResources() != null && transformation.getPreferredResources() != null) {
            streamGraph.setResources(transformationId, transformation.getMinResources(), transformation.getPreferredResources());
        }

        final StreamNode streamNode = streamGraph.getStreamNode(transformationId);
        if (streamNode != null) {
            validateUseCaseWeightsNotConflict(streamNode.getManagedMemoryOperatorScopeUseCaseWeights(),
                    transformation.getManagedMemoryOperatorScopeUseCaseWeights());
            streamNode.setManagedMemoryUseCaseWeights(transformation.getManagedMemoryOperatorScopeUseCaseWeights(),
                    transformation.getManagedMemorySlotScopeUseCases());
        }
    }
}
