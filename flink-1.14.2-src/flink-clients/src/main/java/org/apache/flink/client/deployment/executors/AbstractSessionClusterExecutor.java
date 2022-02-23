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

package org.apache.flink.client.deployment.executors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientJobClientAdapter;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.function.FunctionUtils;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An abstract {@link PipelineExecutor} used to execute {@link Pipeline pipelines} on an existing
 * (session) cluster.
 *
 * @param <ClusterID> the type of the id of the cluster.
 * @param <ClientFactory> the type of the {@link ClusterClientFactory} used to create/retrieve a
 *         client to the target cluster.
 */
@Internal
public class AbstractSessionClusterExecutor<ClusterID, ClientFactory extends ClusterClientFactory<ClusterID>> implements PipelineExecutor {

    private final ClientFactory clusterClientFactory;

    public AbstractSessionClusterExecutor(@Nonnull final ClientFactory clusterClientFactory) {
        this.clusterClientFactory = checkNotNull(clusterClientFactory);
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 当前方法分为 三个步骤：
     *  1、 完成 StreamGraph 到 JObGraph 的转变
     *  2、 构建一个 RestClient
     *  3、 通过 RestClient 提交 JobGraph 到 JobManager
     *  -
     *  Task 的初始化的，代码特别多  14 个步骤！
     */
    @Override
    public CompletableFuture<JobClient> execute(@Nonnull final Pipeline pipeline,
                                                @Nonnull final Configuration configuration,
                                                @Nonnull final ClassLoader userCodeClassloader) throws Exception {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 将 StreamGraph 变成 JobGraph
         *  pipeline = StreamGraph
         *  -
         *  其实就是判断，StrewamGraph 中的相邻 StreamNode 是否满足一定的条件，如果满足，则这两个 StreamNode 合并成一个
         *  然后称之为 JobVertex， 它就是 JobGraph 中的顶点！ 边就是 JobEdge
         *  StreamGraph: StreamNode => StreamOperator
         *  JobGraph： JobVertex => StreamOperator/OpeartorChain
         */
        final JobGraph jobGraph = PipelineExecutorUtils.getJobGraph(pipeline, configuration);


        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         *  1、通过 clusterClientFactory.createClusterDescriptor 去构造一个 RestClient
         *  2、通过 RestClient 提交 JobGraph
         */


        // TODO_MA 马中华 注释： A + B + C 联合起来完成的事情： 1、RestClusterClient  2、RestClient  3、Netty Client
        // TODO_MA 马中华 注释： 构建一个 ClusterDescriptor
        // TODO_MA 马中华 注释： A
        try (final ClusterDescriptor<ClusterID> clusterDescriptor = clusterClientFactory.createClusterDescriptor(configuration)) {
            // TODO_MA 马中华 注释： StandaloneClusterId
            // TODO_MA 马中华 注释： B
            final ClusterID clusterID = clusterClientFactory.getClusterId(configuration);
            checkState(clusterID != null);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 构建一个 RestClusterClient
             *  C
             *  内部就是： 2、RestClient  3、Netty Client
             *  RestClusterClient ===> RestClient ====> NettyClient
             */
            final ClusterClientProvider<ClusterID> clusterClientProvider = clusterDescriptor.retrieve(clusterID);
            ClusterClient<ClusterID> clusterClient = clusterClientProvider.getClusterClient();

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 真正的提交
             */
            return clusterClient.submitJob(jobGraph).thenApplyAsync(FunctionUtils.uncheckedFunction(jobId -> {
                        ClientUtils.waitUntilJobInitializationFinished(() -> clusterClient.getJobStatus(jobId).get(),
                                () -> clusterClient.requestJobResult(jobId).get(), userCodeClassloader);
                        return jobId;
                    })).thenApplyAsync(
                            jobID -> (JobClient) new ClusterClientJobClientAdapter<>(clusterClientProvider, jobID, userCodeClassloader))
                    .whenCompleteAsync((ignored1, ignored2) -> clusterClient.close());
        }
    }
}
