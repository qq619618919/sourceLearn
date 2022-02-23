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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.runtime.topology.Result;
import org.apache.flink.runtime.topology.Vertex;

import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/** Common utils for computing pipelined regions. */
// TODO_MA 马中华 注释： 对 图的连通 和 划分做合理的规划
// TODO_MA 马中华 注释： DefaultExecutionTopology 为一个用于调度执行的拓朴，内部维护着执行时需要的任务拓朴信息。
public final class PipelinedRegionComputeUtil {

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：
     */
    static <V extends Vertex<?, ?, V, R>, R extends Result<?, ?, V, R>> Map<V, Set<V>> buildRawRegions(final Iterable<? extends V> topologicallySortedVertices,
                                                                                                       final Function<V, Iterable<R>> getNonReconnectableConsumedResults) {
        // TODO_MA 马中华 注释： 结果容器
        final Map<V, Set<V>> vertexToRegion = new IdentityHashMap<>();

        // iterate all the vertices which are topologically sorted
        // TODO_MA 马中华 注释： 遍历已经 拓扑排序的 JobVertex
        for (V vertex : topologicallySortedVertices) {

            // TODO_MA 马中华 注释： 当前 Region
            Set<V> currentRegion = new HashSet<>();

            // TODO_MA 马中华 注释：
            currentRegion.add(vertex);
            vertexToRegion.put(vertex, currentRegion);

            // Similar to the BLOCKING ResultPartitionType, each vertex connected through
            // PIPELINED_APPROXIMATE is also considered as a single region. This attribute is called "reconnectable".
            // Reconnectable will be removed after FLINK-19895, see also {@link ResultPartitionType#isReconnectable}
            for (R consumedResult : getNonReconnectableConsumedResults.apply(vertex)) {

                final V producerVertex = consumedResult.getProducer();
                final Set<V> producerRegion = vertexToRegion.get(producerVertex);

                if (producerRegion == null) {
                    throw new IllegalStateException(
                            "Producer task " + producerVertex.getId() + " failover region is null" + " while calculating failover region for the consumer task " + vertex.getId() + ". This should be a failover region building bug.");
                }

                // TODO_MA 马中华 注释： 检查它是否与生产者区域相同，如果是，则跳过合并此检查可以显着降低 All-to-All PIPELINED 边缘情况下的计算复杂度
                // check if it is the same as the producer region, if so skip the merge
                // this check can significantly reduce compute complexity in All-to-All PIPELINED edge case
                if (currentRegion != producerRegion) {

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 合并
                     */
                    currentRegion = mergeRegions(currentRegion, producerRegion, vertexToRegion);
                }
            }
        }

        // TODO_MA 马中华 注释： 返回结果容器
        return vertexToRegion;
    }

    static <V extends Vertex<?, ?, V, ?>> Set<V> mergeRegions(final Set<V> region1,
                                                              final Set<V> region2,
                                                              final Map<V, Set<V>> vertexToRegion) {

        // merge the smaller region into the larger one to reduce the cost
        final Set<V> smallerSet;
        final Set<V> largerSet;

        // TODO_MA 马中华 注释：
        if (region1.size() < region2.size()) {
            smallerSet = region1;
            largerSet = region2;
        } else {
            smallerSet = region2;
            largerSet = region1;
        }

        // TODO_MA 马中华 注释：
        for (V v : smallerSet) {
            vertexToRegion.put(v, largerSet);
        }

        // TODO_MA 马中华 注释： 将小 region 合并到 region
        largerSet.addAll(smallerSet);

        return largerSet;
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 合并两个 region
     */
    static <V extends Vertex<?, ?, V, ?>> Set<Set<V>> uniqueRegions(final Map<V, Set<V>> vertexToRegion) {
        final Set<Set<V>> distinctRegions = Collections.newSetFromMap(new IdentityHashMap<>());
        distinctRegions.addAll(vertexToRegion.values());
        return distinctRegions;
    }

    private PipelinedRegionComputeUtil() {
    }
}
