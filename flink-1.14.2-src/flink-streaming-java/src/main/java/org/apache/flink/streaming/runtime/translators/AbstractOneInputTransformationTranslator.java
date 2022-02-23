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

package org.apache.flink.streaming.runtime.translators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.graph.SimpleTransformationTranslator;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A utility base class for one input {@link Transformation transformations} that provides a
 * function for configuring common graph properties.
 */
abstract class AbstractOneInputTransformationTranslator<IN, OUT, OP extends Transformation<OUT>> extends SimpleTransformationTranslator<OUT, OP> {

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 构造 StreamNode 和 StreamEdge
     */
    protected Collection<Integer> translateInternal(final Transformation<OUT> transformation,
                                                    final StreamOperatorFactory<OUT> operatorFactory,
                                                    final TypeInformation<IN> inputType,
                                                    @Nullable final KeySelector<IN, ?> stateKeySelector,
                                                    @Nullable final TypeInformation<?> stateKeyType,
                                                    final Context context) {
        checkNotNull(transformation);
        checkNotNull(operatorFactory);
        checkNotNull(inputType);
        checkNotNull(context);

        final StreamGraph streamGraph = context.getStreamGraph();
        final String slotSharingGroup = context.getSlotSharingGroup();
        final int transformationId = transformation.getId();
        final ExecutionConfig executionConfig = streamGraph.getExecutionConfig();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 第一件大事：
         *  根据一个 Operator 生成一个 StreamNode
         */
        streamGraph.addOperator(transformationId, slotSharingGroup, transformation.getCoLocationGroupKey(), operatorFactory,
                inputType, transformation.getOutputType(), transformation.getName());

        if (stateKeySelector != null) {
            TypeSerializer<?> keySerializer = stateKeyType.createSerializer(executionConfig);
            streamGraph.setOneInputStateKey(transformationId, stateKeySelector, keySerializer);
        }

        int parallelism = transformation.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT ? transformation.getParallelism() : executionConfig.getParallelism();
        streamGraph.setParallelism(transformationId, parallelism);
        streamGraph.setMaxParallelism(transformationId, transformation.getMaxParallelism());

        final List<Transformation<?>> parentTransformations = transformation.getInputs();
        checkState(parentTransformations.size() == 1,
                "Expected exactly one input transformation but found " + parentTransformations.size());

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 第二件大事；
         *  根据输入，生成对应的和上游顶点的 边
         *  一般来说，就是一个输入： ds.xx1.xx3.xx4.xx5.xx6
         *  join  union
         */
        for (Integer inputId : context.getStreamNodeIds(parentTransformations.get(0))) {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 生成边，并且维护关系。 维护和上游顶点的关系
             */
            streamGraph.addEdge(inputId, transformationId, 0);
        }

        return Collections.singleton(transformationId);
    }
}
