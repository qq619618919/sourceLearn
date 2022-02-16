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

package org.apache.flink.runtime.highavailability.zookeeper;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.ZooKeeperCheckpointRecoveryFactory;
import org.apache.flink.runtime.highavailability.AbstractHaServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.util.ZooKeeperUtils;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator4.org.apache.curator.utils.ZKPaths;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.data.Stat;

import javax.annotation.Nonnull;

import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of the {@link AbstractHaServices} using Apache ZooKeeper. The services store
 * data in ZooKeeper's nodes as illustrated by the following tree structure:
 *
 * <pre>
 * /flink
 *      +/cluster_id_1/leader/resource_manager/latch
 *      |            |                        /connection_info
 *      |            |       /dispatcher/latch
 *      |            |                  /connection_info
 *      |            |       /rest_server/latch
 *      |            |                   /connection_info
 *      |            |
 *      |            |
 *      |            +jobgraphs/job-id-1
 *      |            |         /job-id-2
 *      |            +jobs/job-id-1/leader/latch
 *      |                 |               /connection_info
 *      |                 |        /checkpoints/latest
 *      |                 |                    /latest-1
 *      |                 |                    /latest-2
 *      |                 |       /checkpoint_id_counter
 *      |
 *      +/cluster_id_2/leader/resource_manager/latch
 *      |            |                        /connection_info
 *      |            |       /dispatcher/latch
 *      |            |                  /connection_info
 *      |            |       /rest_server/latch
 *      |            |                   /connection_info
 *      |            |
 *      |            +jobgraphs/job-id-2
 *      |            +jobs/job-id-2/leader/latch
 *      |                 |               /connection_info
 *      |                 |        /checkpoints/latest
 *      |                 |                    /latest-1
 *      |                 |                    /latest-2
 *      |                 |       /checkpoint_id_counter
 * </pre>
 *
 * <p>The root path "/flink" is configurable via the option {@link
 * HighAvailabilityOptions#HA_ZOOKEEPER_ROOT}. This makes sure Flink stores its data under specific
 * subtrees in ZooKeeper, for example to accommodate specific permission.
 *
 * <p>The "cluster_id" part identifies the data stored for a specific Flink "cluster". This
 * "cluster" can be either a standalone or containerized Flink cluster, or it can be job on a
 * framework like YARN (in a "per-job-cluster" mode).
 *
 * <p>In case of a "per-job-cluster" on YARN, the cluster-id is generated and configured
 * automatically by the client or dispatcher that submits the Job to YARN.
 *
 * <p>In the case of a standalone cluster, that cluster-id needs to be configured via {@link
 * HighAvailabilityOptions#HA_CLUSTER_ID}. All nodes with the same cluster id will join the same
 * cluster and participate in the execution of the same set of jobs.
 *
 * // TODO_MA 马中华 注释： ZooKeeperHaServices 就是 FLink 提供的基于 ZK 的一种高可用服务
 */
public class ZooKeeperHaServices extends AbstractHaServices {

    // ------------------------------------------------------------------------

    /** The ZooKeeper client to use. */
    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 内部包装了一个基于 CuratorFramework 的客户端
     *  Curator 是 ZK 的一个 API 框架，提供了很多的丰富的功能。  mybatis jdbc
     */
    private final CuratorFramework client;

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 服务端就是 ZK 集群。 Flink 集群如果搭建 HA 集群的话，必然要基于一个 ZK 集群来搭建
     */

    public ZooKeeperHaServices(CuratorFramework client,
                               Executor executor,
                               Configuration configuration,
                               BlobStoreService blobStoreService) {
        super(configuration, executor, blobStoreService);
        this.client = checkNotNull(client);
    }

    @Override
    public CheckpointRecoveryFactory createCheckpointRecoveryFactory() throws Exception {
        return new ZooKeeperCheckpointRecoveryFactory(ZooKeeperUtils.useNamespaceAndEnsurePath(client,
                ZooKeeperUtils.getJobsPath()
        ), configuration, ioExecutor);
    }

    @Override
    public JobGraphStore createJobGraphStore() throws Exception {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return ZooKeeperUtils.createJobGraphs(client, configuration);
    }

    @Override
    public RunningJobsRegistry createRunningJobsRegistry() {
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return new ZooKeeperRunningJobsRegistry(client, configuration);
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 有四个组件，会调用这个方法
     *  1、 ResourceManager  管理资源和从节点
     *  2、 Dispatcher  接收到 JobGraph 之后负责启动 JobMaster 的
     *  3、 WebMonitorEndpoint  负责接收 restClient 发送过来的请求
     *  4、 JobMaster  负责一个 JOb 的执行，主要包括申请 slot 和 部署 Task
     *  -
     *  事实上就是登记！ 用的是同一个 znode 节点，这个 znode 节点就充当锁
     */
    @Override
    protected LeaderElectionService createLeaderElectionService(String leaderPath) {
        // TODO_MA 马中华 注释： /rest_server
        return ZooKeeperUtils.createLeaderElectionService(client, leaderPath);
    }

    @Override
    protected LeaderRetrievalService createLeaderRetrievalService(String leaderPath) {

        // TODO_MA 马中华 注释：
        return ZooKeeperUtils.createLeaderRetrievalService(client, leaderPath, configuration);
    }

    @Override
    public void internalClose() {
        client.close();
    }

    @Override
    public void internalCleanup() throws Exception {
        cleanupZooKeeperPaths();
    }

    @Override
    public void internalCleanupJobData(JobID jobID) throws Exception {
        deleteZNode(ZooKeeperUtils.getLeaderPathForJob(jobID));
    }

    @Override
    protected String getLeaderPathForResourceManager() {
        // TODO_MA 马中华 注释： /resource_manager
        return ZooKeeperUtils.getLeaderPathForResourceManager();
    }

    @Override
    protected String getLeaderPathForDispatcher() {
        // TODO_MA 马中华 注释： /dispatcher
        return ZooKeeperUtils.getLeaderPathForDispatcher();
    }

    @Override
    public String getLeaderPathForJobManager(final JobID jobID) {
        // TODO_MA 马中华 注释： /leader
        return ZooKeeperUtils.getLeaderPathForJobManager(jobID);
    }

    @Override
    protected String getLeaderPathForRestServer() {
        // TODO_MA 马中华 注释： /rest_server
        return ZooKeeperUtils.getLeaderPathForRestServer();
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /** Cleans up leftover ZooKeeper paths. */
    private void cleanupZooKeeperPaths() throws Exception {
        deleteOwnedZNode();
        tryDeleteEmptyParentZNodes();
    }

    private void deleteOwnedZNode() throws Exception {
        deleteZNode("/");
    }

    private void deleteZNode(String path) throws Exception {
        // delete the HA_CLUSTER_ID znode which is owned by this cluster

        // Since we are using Curator version 2.12 there is a bug in deleting the children
        // if there is a concurrent delete operation. Therefore we need to add this retry
        // logic. See https://issues.apache.org/jira/browse/CURATOR-430 for more information.
        // The retry logic can be removed once we upgrade to Curator version >= 4.0.1.
        boolean zNodeDeleted = false;
        while (!zNodeDeleted) {
            Stat stat = client.checkExists().forPath(path);
            if (stat == null) {
                logger.debug("znode {} has been deleted", path);
                return;
            }
            try {
                client.delete().deletingChildrenIfNeeded().forPath(path);
                zNodeDeleted = true;
            } catch (KeeperException.NoNodeException ignored) {
                // concurrent delete operation. Try again.
                logger.debug("Retrying to delete znode because of other concurrent delete operation.");
            }
        }
    }

    /**
     * Tries to delete empty parent znodes.
     *
     * <p>IMPORTANT: This method can be removed once all supported ZooKeeper versions support the
     * container {@link org.apache.zookeeper.CreateMode}.
     *
     * @throws Exception if the deletion fails for other reason than {@link
     *         KeeperException.NotEmptyException}
     */
    private void tryDeleteEmptyParentZNodes() throws Exception {
        // try to delete the parent znodes if they are empty
        String remainingPath = getParentPath(getNormalizedPath(client.getNamespace()));
        final CuratorFramework nonNamespaceClient = client.usingNamespace(null);

        while (!isRootPath(remainingPath)) {
            try {
                nonNamespaceClient.delete().forPath(remainingPath);
            } catch (KeeperException.NotEmptyException ignored) {
                // We can only delete empty znodes
                break;
            }

            remainingPath = getParentPath(remainingPath);
        }
    }

    private static boolean isRootPath(String remainingPath) {
        return ZKPaths.PATH_SEPARATOR.equals(remainingPath);
    }

    @Nonnull
    private static String getNormalizedPath(String path) {
        return ZKPaths.makePath(path, "");
    }

    @Nonnull
    private static String getParentPath(String path) {
        return ZKPaths.getPathAndNode(path).getPath();
    }
}
