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

package org.apache.zookeeper.server.quorum;

import java.io.IOException;
import javax.management.JMException;
import javax.security.sasl.SaslException;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.audit.ZKAuditProvider;
import org.apache.zookeeper.jmx.ManagedUtil;
import org.apache.zookeeper.metrics.MetricsProvider;
import org.apache.zookeeper.metrics.MetricsProviderLifeCycleException;
import org.apache.zookeeper.metrics.impl.MetricsProviderBootstrap;
import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.ExitCode;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerMetrics;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.admin.AdminServer.AdminServerException;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog.DatadirException;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.util.JvmPauseMonitor;
import org.apache.zookeeper.util.ServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <h2>Configuration file</h2>
 *
 * When the main() method of this class is used to start the program, the first
 * argument is used as a path to the config file, which will be used to obtain
 * configuration information. This file is a Properties file, so keys and
 * values are separated by equals (=) and the key/value pairs are separated
 * by new lines. The following is a general summary of keys used in the
 * configuration file. For full details on this see the documentation in docs/index.html
 * <ol>
 * <li>dataDir - The directory where the ZooKeeper data is stored.</li>
 * <li>dataLogDir - The directory where the ZooKeeper transaction log is stored.</li>
 * <li>clientPort - The port used to communicate with clients.</li>
 * <li>tickTime - The duration of a tick in milliseconds. This is the basic
 * unit of time in ZooKeeper.</li>
 * <li>initLimit - The maximum number of ticks that a follower will wait to
 * initially synchronize with a leader.</li>
 * <li>syncLimit - The maximum number of ticks that a follower will wait for a
 * message (including heartbeats) from the leader.</li>
 * <li>server.<i>id</i> - This is the host:port[:port] that the server with the
 * given id will use for the quorum protocol.</li>
 * </ol>
 * In addition to the config file. There is a file in the data directory called
 * "myid" that contains the server id as an ASCII decimal value.
 */
@InterfaceAudience.Public
public class QuorumPeerMain {

    // TODO_MA 注释： 一句话总结 QuorumPeerMain 的作用： 提供了饿一个 main 方法，用来创建和启动 QuorumPeer

    private static final Logger LOG = LoggerFactory.getLogger(QuorumPeerMain.class);

    private static final String USAGE = "Usage: QuorumPeerMain configfile";

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 一台物理 zookeeper 服务器 就是一个 QP 的抽象
     */
    protected QuorumPeer quorumPeer;

    /**
     * To start the replicated server specify the configuration file name on the command line.
     *
     * @param args path to the configfile
     */
    public static void main(String[] args) {

        // TODO_MA 马中华 注释： args【0】 = zoo.cfg 路径
        // TODO_MA 注释： 构建QuorumPeerMain 实例
        QuorumPeerMain main = new QuorumPeerMain();
        try {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             *  1、初始化
             *  2、启动
             */
            main.initializeAndRun(args);

        } catch(IllegalArgumentException e) {
            LOG.error("Invalid arguments, exiting abnormally", e);
            LOG.info(USAGE);
            System.err.println(USAGE);
            ZKAuditProvider.addServerStartFailureAuditLog();
            ServiceUtils.requestSystemExit(ExitCode.INVALID_INVOCATION.getValue());
        } catch(ConfigException e) {
            LOG.error("Invalid config, exiting abnormally", e);
            System.err.println("Invalid config, exiting abnormally");
            ZKAuditProvider.addServerStartFailureAuditLog();
            ServiceUtils.requestSystemExit(ExitCode.INVALID_INVOCATION.getValue());
        } catch(DatadirException e) {
            LOG.error("Unable to access datadir, exiting abnormally", e);
            System.err.println("Unable to access datadir, exiting abnormally");
            ZKAuditProvider.addServerStartFailureAuditLog();
            ServiceUtils.requestSystemExit(ExitCode.UNABLE_TO_ACCESS_DATADIR.getValue());
        } catch(AdminServerException e) {
            LOG.error("Unable to start AdminServer, exiting abnormally", e);
            System.err.println("Unable to start AdminServer, exiting abnormally");
            ZKAuditProvider.addServerStartFailureAuditLog();
            ServiceUtils.requestSystemExit(ExitCode.ERROR_STARTING_ADMIN_SERVER.getValue());
        } catch(Exception e) {
            LOG.error("Unexpected exception, exiting abnormally", e);
            ZKAuditProvider.addServerStartFailureAuditLog();
            ServiceUtils.requestSystemExit(ExitCode.UNEXPECTED_ERROR.getValue());
        }
        LOG.info("Exiting normally");
        ServiceUtils.requestSystemExit(ExitCode.EXECUTION_FINISHED.getValue());
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 这个方法总共做了三件事：
     *  1、解析 zoo.cfg 和 myid 配置
     *  2、启动一个关于 旧快照数据文件的  定期删除任务
     *  3、启动 QuorumPeer
     *  QuorumPeerMain 就是负责解析 配置文件之后，用来启动一个 QuorumPeer，QuorumPeer 代表了一台 ZooKeeperServer
     */
    protected void initializeAndRun(String[] args) throws ConfigException, IOException, AdminServerException {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： args[0] = $ZOOKEEPER_HOME/conf/zoo.cfg
         *  1、解析 zoo.cfg
         *      properties 文件，最终的解析，是通过 Properties API 来完成解析
         *  2、生成一个 QuorumMaj 的算法实例： 少数服从多数
         *  3、解析 myid
         *      构建一个输入流读取这个文件，读取一行，得到一个数值字符串，转换数值
         *  4、在解析过程中，穿插了大量的参数的校验！
         */
        QuorumPeerConfig config = new QuorumPeerConfig();
        if(args.length == 1) {
            config.parse(args[0]);
        }

        // TODO_MA 注释： zookeeper 的 解析 配置的管理代码的确不够优雅。参考 flink 的实现
        // TODO_MA 注释： config.xx1(key).xx2(value).xx3(jiaoyan).xx4(doc)

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 启动了一个定时任务，定时清理旧快照文件
         */
        // Start and schedule the the purge task
        DatadirCleanupManager purgeMgr = new DatadirCleanupManager(
                config.getDataDir(),    // TODO_MA 马中华 注释： 数据存储目录
                config.getDataLogDir(),         // TODO_MA 马中华 注释： 日志存储目录
                config.getSnapRetainCount(),        // TODO_MA 马中华 注释： 至少要保留的快照文件个数
                config.getPurgeInterval());         // TODO_MA 马中华 注释： 定时任务的时间间隔
        purgeMgr.start();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 从配置中启动 QuorumPeer
         *  main.args[0] = zoo.cfg
         *  最开始，解析 zoo.cfg 得到的配置信息，都被放置在了 QuorumPeerConfig 中
         *  在将来 QuorumPeer 启动的时候，会将 QuorumPeerConfig 中的各个成员变量复制到 QuorumPeer 中
         *  本身 QuorumPeer 就代表了一个 ZK JVM 进程，就是一个 ZooKeeperServer
         *  最终实现的效果就是： 配置在 zoo.cfg 中的配置，最终，都被保存在 QuorumPeer 中了
         *  具体形式： 一个成员变量，一个配置值
         */
        if(args.length == 1 && config.isDistributed()) {
            runFromConfig(config);
        } else {
            LOG.warn("Either no config or no quorum defined in config, running in standalone mode");
            // there is only server in the quorum -- run as standalone
            ZooKeeperServerMain.main(args);
        }
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 三件事
     *  1、启动 真正意义上的 Server， 监听 2181 端口的 NIO 服务端
     *  2、获取 QuorumPeer 实例对象，并且把 QuorumPeerConfig 中的各种成员变量值，复制到 QuorumPeer 里面
     *  3、启动 QuorumPeer
     */
    public void runFromConfig(QuorumPeerConfig config) throws IOException, AdminServerException {
        try {
            ManagedUtil.registerLog4jMBeans();
        } catch(JMException e) {
            LOG.warn("Unable to register log4j JMX control", e);
        }

        LOG.info("Starting quorum peer, myid=" + config.getServerId());
        MetricsProvider metricsProvider;
        try {
            metricsProvider = MetricsProviderBootstrap
                    .startMetricsProvider(config.getMetricsProviderClassName(), config.getMetricsProviderConfiguration());
        } catch(MetricsProviderLifeCycleException error) {
            throw new IOException("Cannot boot MetricsProvider " + config.getMetricsProviderClassName(), error);
        }

        // TODO_MA 注释： 从这儿开始: 准备 QP ， 以及启动 QP

        try {
            ServerMetrics.metricsProviderInitialized(metricsProvider);
            ServerCnxnFactory cnxnFactory = null;
            ServerCnxnFactory secureCnxnFactory = null;

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 创建 ServerCnxnFactory = 默认实现： NIOServerCnxnFactory
             *  内初初始化了四种线程（只有三种， workerthread 是在调用 start() 方法的时初始化的）
             *  此处创建的 NIOServerCnxnFactory 为客户端发送的读写请求提供处理服务的
             */
            if(config.getClientPortAddress() != null) {
                // TODO_MA 注释： 创建 NIOServerCnxnFactory
                cnxnFactory = ServerCnxnFactory.createFactory();
                // TODO_MA 注释： 进行配置启动
                cnxnFactory.configure(config.getClientPortAddress(), config.getMaxClientCnxns(),
                        config.getClientPortListenBacklog(), false);
            }

            // TODO_MA 注释：
            if(config.getSecureClientPortAddress() != null) {
                secureCnxnFactory = ServerCnxnFactory.createFactory();
                secureCnxnFactory.configure(config.getSecureClientPortAddress(), config.getMaxClientCnxns(),
                        config.getClientPortListenBacklog(), true);
            }

            // TODO_MA 注释： 构造了呀一个 QP 实例对象
            // TODO_MA 注释： 最开始在解析 配置的时候，吧各种配置，都存储在 QuorumPeerConfig 对象中
            // TODO_MA 注释： 现在需要从 QuorumPeerConfig 吧各种配置存储到 quorumPeer 中来
            quorumPeer = getQuorumPeer();

            /**
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            quorumPeer.setTxnFactory(new FileTxnSnapLog(config.getDataLogDir(), config.getDataDir()));

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 完成了参数的传递
             *  zoo.cfg  ===> QuorumPeerConfig ===> QuorumPeer
             */
            quorumPeer.enableLocalSessions(config.areLocalSessionsEnabled());
            quorumPeer.enableLocalSessionsUpgrading(config.isLocalSessionsUpgradingEnabled());
            //quorumPeer.setQuorumPeers(config.getAllMembers());
            quorumPeer.setElectionType(config.getElectionAlg());
            quorumPeer.setMyid(config.getServerId());
            quorumPeer.setTickTime(config.getTickTime());
            quorumPeer.setMinSessionTimeout(config.getMinSessionTimeout());
            quorumPeer.setMaxSessionTimeout(config.getMaxSessionTimeout());
            quorumPeer.setInitLimit(config.getInitLimit());
            quorumPeer.setSyncLimit(config.getSyncLimit());
            quorumPeer.setConnectToLearnerMasterLimit(config.getConnectToLearnerMasterLimit());
            quorumPeer.setObserverMasterPort(config.getObserverMasterPort());
            quorumPeer.setConfigFileName(config.getConfigFilename());
            quorumPeer.setClientPortListenBacklog(config.getClientPortListenBacklog());

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            quorumPeer.setZKDatabase(new ZKDatabase(quorumPeer.getTxnFactory()));

            quorumPeer.setQuorumVerifier(config.getQuorumVerifier(), false);
            if(config.getLastSeenQuorumVerifier() != null) {
                quorumPeer.setLastSeenQuorumVerifier(config.getLastSeenQuorumVerifier(), false);
            }
            quorumPeer.initConfigInZKDatabase();
            quorumPeer.setCnxnFactory(cnxnFactory);
            quorumPeer.setSecureCnxnFactory(secureCnxnFactory);
            quorumPeer.setSslQuorum(config.isSslQuorum());
            quorumPeer.setUsePortUnification(config.shouldUsePortUnification());
            quorumPeer.setLearnerType(config.getPeerType());
            quorumPeer.setSyncEnabled(config.getSyncEnabled());
            quorumPeer.setQuorumListenOnAllIPs(config.getQuorumListenOnAllIPs());
            if(config.sslQuorumReloadCertFiles) {
                quorumPeer.getX509Util().enableCertFileReloading();
            }
            quorumPeer.setMultiAddressEnabled(config.isMultiAddressEnabled());
            quorumPeer.setMultiAddressReachabilityCheckEnabled(config.isMultiAddressReachabilityCheckEnabled());
            quorumPeer.setMultiAddressReachabilityCheckTimeoutMs(config.getMultiAddressReachabilityCheckTimeoutMs());

            // sets quorum sasl authentication configurations
            quorumPeer.setQuorumSaslEnabled(config.quorumEnableSasl);
            if(quorumPeer.isQuorumSaslAuthEnabled()) {
                quorumPeer.setQuorumServerSaslRequired(config.quorumServerRequireSasl);
                quorumPeer.setQuorumLearnerSaslRequired(config.quorumLearnerRequireSasl);
                quorumPeer.setQuorumServicePrincipal(config.quorumServicePrincipal);
                quorumPeer.setQuorumServerLoginContext(config.quorumServerLoginContext);
                quorumPeer.setQuorumLearnerLoginContext(config.quorumLearnerLoginContext);
            }
            quorumPeer.setQuorumCnxnThreadsSize(config.quorumCnxnThreadsSize);
            quorumPeer.initialize();

            if(config.jvmPauseMonitorToRun) {
                quorumPeer.setJvmPauseMonitor(new JvmPauseMonitor(config));
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 做了五件大事：
             *  1、冷启动数据恢复
             *  2、NIOServerCnxnFactory 的启动： 内部其实就是把 NIOServerCnxnFactory 内部的四种线程启动好
             *  3、启动 AdminServer（zk-3.4 没有的）
             *  4、准备选举环境 + 创建选举算法实例
             *  5、super.start(); 进入 ZAB 工作模式， 进入 quorumPeer 的 run()
             */
            quorumPeer.start();

            ZKAuditProvider.addZKStartStopAuditLog();
            quorumPeer.join();
        } catch(InterruptedException e) {
            // warn, but generally this is ok
            LOG.warn("Quorum Peer interrupted", e);
        } finally {
            if(metricsProvider != null) {
                try {
                    metricsProvider.stop();
                } catch(Throwable error) {
                    LOG.warn("Error while stopping metrics", error);
                }
            }
        }
    }

    // @VisibleForTesting
    protected QuorumPeer getQuorumPeer() throws SaslException {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return new QuorumPeer();
    }
}
