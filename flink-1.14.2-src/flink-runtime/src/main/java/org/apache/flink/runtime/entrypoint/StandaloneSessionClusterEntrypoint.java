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

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManagerFactory;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;

/** Entry point for the standalone session cluster. */
/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释：StandaloneSessionClusterEntrypoint = A + B + ClusterEntrypoint
 *  A = k8s, standalone, yarn, mesos(flink-1.14 移除支持)
 *  B = Application， Session， Job
 *  -
 *  不管是那种实现，流程都是一样的，只不过这个流程中，相关组件的具体实现类不一样
 *  ClusterEntrypoint 的内部会启动三个非常重要的服务组件：
 *  1、Dipatcher 的服务组件
 *  2、WebMonitorEndpoint 的服务组件
 *  3、ResourceManager 的服务组件(Flink主节点中的， 跟 YARN 的 RM 没有半点关系)
 *  -
 *  关于集群启动有两个重点：
 *  1、standalone
 *  2、on yarn
 *  原理完全一样，就是具体的实现类不一样，而且最大的差别就是 ResourceManager 这个地方不一样
 *  -
 *  ResourceManager 有两种实现：
 *  1、standalone实现：  StandaloneResourcemanager
 *  2、on YARN 实现： ActiveResourceManager
 *  到底区别是什么呢？
 *  StandaloneResourcemanager： 它的从节点是固定的，没有在启动的时候，有伸缩这个概念
 *  ActiveResourceManager： 它的从节点的数量是不固定的，如果这个 slot资源不够，则 ActiveResourceManager
 *  会向 YARN 的 RM 申请 container 来启动 TaskManager， slot 资源就增加了， 这种模式下， TaskManager 的个数和资源总量是动态的
 *  动态表现在连个地方：
 *  1、资源不够： 从 YARN 中申请 COntianer，启动 TaskManager
 *  2、资源闲置： 如果一个 TaskManager 中的 slot 长期未使用，则会被 YARN 回收！
 *  -
 *  最终的结论：StandaloneSessionClusterEntrypoint Flink standalone 集群的主节点的启动类： main()
 */
public class StandaloneSessionClusterEntrypoint extends SessionClusterEntrypoint {

    public StandaloneSessionClusterEntrypoint(Configuration configuration) {
        super(configuration);
    }

    @Override
    protected DefaultDispatcherResourceManagerComponentFactory createDispatcherResourceManagerComponentFactory(
            Configuration configuration) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： DefaultDispatcherResourceManagerComponentFactory
         */
        return DefaultDispatcherResourceManagerComponentFactory.createSessionComponentFactory(

                // TODO_MA 马中华 注释： A1 = StandaloneResourceManagerFactory
                StandaloneResourceManagerFactory.getInstance());
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 阅读 Flink 的源码有这样的感受：
     *  1、代码量特别大
     *  2、任何方法的实现，基本上代码都比较多
     *  3、基本上每个方法的名称很长，参数很多，.....
     *  4、Flink 源码实现中，还涉及到大量的 异步编程，对于代码的执行逻辑是有影响的（代码不一定是从上到下执行了）
     *  -
     *  套路： 总是先关注 核心流程实现，然后抠细节
     *  阅读源码的目标：
     *  1、了解清楚一个 工作机制的详细实现
     *  2、消化吸收源码中的一些优秀的编码套路
     *  一般没有！ 官网中只有大致的接收！ 架构设计思路！
     */
    public static void main(String[] args) {
        // startup checks and logging
        EnvironmentInformation.logEnvironmentInfo(LOG, StandaloneSessionClusterEntrypoint.class.getSimpleName(), args);
        SignalHandler.register(LOG);
        JvmShutdownSafeguard.installAsShutdownHook(LOG);

        final EntrypointClusterConfiguration entrypointClusterConfiguration = ClusterEntrypointUtils.parseParametersOrExit(
                args,
                new EntrypointClusterConfigurationParserFactory(),
                StandaloneSessionClusterEntrypoint.class
        );

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 1、 解析配置文件
         *  masters, workers, zoo.cfg ,  flink-conf.yaml  四个配置文件
         */
        Configuration configuration = loadConfiguration(entrypointClusterConfiguration);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 2、创建一个 入口类的 实例对象。里面什么也没干
         */
        StandaloneSessionClusterEntrypoint entrypoint = new StandaloneSessionClusterEntrypoint(configuration);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 3、重点： 主节点启动
         */
        ClusterEntrypoint.runClusterEntrypoint(entrypoint);
    }
}
