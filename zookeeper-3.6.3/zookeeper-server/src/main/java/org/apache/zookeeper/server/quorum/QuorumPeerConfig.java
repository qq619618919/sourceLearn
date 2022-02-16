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

import static org.apache.zookeeper.common.NetUtils.formatInetAddr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.Writer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.common.AtomicFileWritingIdiom;
import org.apache.zookeeper.common.AtomicFileWritingIdiom.OutputStreamStatement;
import org.apache.zookeeper.common.AtomicFileWritingIdiom.WriterStatement;
import org.apache.zookeeper.common.ClientX509Util;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.common.StringUtils;
import org.apache.zookeeper.metrics.impl.DefaultMetricsProvider;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.auth.ProviderRegistry;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.auth.QuorumAuth;
import org.apache.zookeeper.server.quorum.flexible.QuorumHierarchical;
import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.JvmPauseMonitor;
import org.apache.zookeeper.server.util.VerifyingFileFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

@InterfaceAudience.Public
public class QuorumPeerConfig {

    private static final Logger LOG = LoggerFactory.getLogger(QuorumPeerConfig.class);
    private static final int UNSET_SERVERID = -1;
    public static final String nextDynamicConfigFileSuffix = ".dynamic.next";

    private static boolean standaloneEnabled = true;
    private static boolean reconfigEnabled = false;

    // 基本上，这些成员变量都是从 zoo.cfg 中执行初始化的。
    protected InetSocketAddress clientPortAddress;
    protected InetSocketAddress secureClientPortAddress;
    protected boolean sslQuorum = false;
    protected boolean shouldUsePortUnification = false;
    protected int observerMasterPort;
    protected boolean sslQuorumReloadCertFiles = false;
    protected File dataDir;
    protected File dataLogDir;
    protected String dynamicConfigFileStr = null;
    protected String configFileStr = null;
    protected int tickTime = ZooKeeperServer.DEFAULT_TICK_TIME;
    protected int maxClientCnxns = 60;
    /**
     * defaults to -1 if not set explicitly
     */
    protected int minSessionTimeout = -1;
    /**
     * defaults to -1 if not set explicitly
     */
    protected int maxSessionTimeout = -1;
    protected String metricsProviderClassName = DefaultMetricsProvider.class.getName();
    protected Properties metricsProviderConfiguration = new Properties();
    protected boolean localSessionsEnabled = false;
    protected boolean localSessionsUpgradingEnabled = false;
    /**
     * defaults to -1 if not set explicitly
     */
    protected int clientPortListenBacklog = -1;

    protected int initLimit;
    protected int syncLimit;
    protected int connectToLearnerMasterLimit;
    protected int electionAlg = 3;
    protected int electionPort = 2182;
    protected boolean quorumListenOnAllIPs = false;

    protected long serverId = UNSET_SERVERID;

    protected QuorumVerifier quorumVerifier = null, lastSeenQuorumVerifier = null;
    protected int snapRetainCount = 3;
    protected int purgeInterval = 0;
    protected boolean syncEnabled = true;

    protected String initialConfig;

    protected LearnerType peerType = LearnerType.PARTICIPANT;

    /**
     * Configurations for the quorumpeer-to-quorumpeer sasl authentication
     */
    protected boolean quorumServerRequireSasl = false;
    protected boolean quorumLearnerRequireSasl = false;
    protected boolean quorumEnableSasl = false;
    protected String quorumServicePrincipal = QuorumAuth.QUORUM_KERBEROS_SERVICE_PRINCIPAL_DEFAULT_VALUE;
    protected String quorumLearnerLoginContext = QuorumAuth.QUORUM_LEARNER_SASL_LOGIN_CONTEXT_DFAULT_VALUE;
    protected String quorumServerLoginContext = QuorumAuth.QUORUM_SERVER_SASL_LOGIN_CONTEXT_DFAULT_VALUE;
    protected int quorumCnxnThreadsSize;

    // multi address related configs
    private boolean multiAddressEnabled = Boolean.parseBoolean(
            System.getProperty(QuorumPeer.CONFIG_KEY_MULTI_ADDRESS_ENABLED,
                    QuorumPeer.CONFIG_DEFAULT_MULTI_ADDRESS_ENABLED));
    private boolean multiAddressReachabilityCheckEnabled = Boolean
            .parseBoolean(System.getProperty(QuorumPeer.CONFIG_KEY_MULTI_ADDRESS_REACHABILITY_CHECK_ENABLED, "true"));
    private int multiAddressReachabilityCheckTimeoutMs = Integer.parseInt(
            System.getProperty(QuorumPeer.CONFIG_KEY_MULTI_ADDRESS_REACHABILITY_CHECK_TIMEOUT_MS,
                    String.valueOf(MultipleAddresses.DEFAULT_TIMEOUT.toMillis())));

    /**
     * Minimum snapshot retain count.
     *
     * @see org.apache.zookeeper.server.PurgeTxnLog#purge(File, File, int)
     */
    private final int MIN_SNAP_RETAIN_COUNT = 3;

    /**
     * JVM Pause Monitor feature switch
     */
    protected boolean jvmPauseMonitorToRun = false;
    /**
     * JVM Pause Monitor warn threshold in ms
     */
    protected long jvmPauseWarnThresholdMs = JvmPauseMonitor.WARN_THRESHOLD_DEFAULT;
    /**
     * JVM Pause Monitor info threshold in ms
     */
    protected long jvmPauseInfoThresholdMs = JvmPauseMonitor.INFO_THRESHOLD_DEFAULT;
    /**
     * JVM Pause Monitor sleep time in ms
     */
    protected long jvmPauseSleepTimeMs = JvmPauseMonitor.SLEEP_TIME_MS_DEFAULT;

    @SuppressWarnings("serial")
    public static class ConfigException extends Exception {

        public ConfigException(String msg) {
            super(msg);
        }

        public ConfigException(String msg, Exception e) {
            super(msg, e);
        }

    }

    /**
     * Parse a ZooKeeper configuration file
     *
     * @param path the patch of the configuration file
     * @throws ConfigException error processing configuration
     */
    public void parse(String path) throws ConfigException {
        LOG.info("Reading configuration from: " + path);

        try {

            // TODO_MA 注释： 把  zoo.cfg 构造一个 file 对象
            File configFile = (new VerifyingFileFactory.Builder(LOG).warnForRelativePath().failForNonExistingPath()
                    .build()).create(path);

            // TODO_MA 注释： 通过 Properties API 执行 zoo.cfg 的读取
            Properties cfg = new Properties();
            FileInputStream in = new FileInputStream(configFile);
            try {
                cfg.load(in);
                configFileStr = path;
            } finally {
                in.close();
            }

            /* Read entire config file as initial configuration */
            initialConfig = new String(Files.readAllBytes(configFile.toPath()));

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 真正执行解析
             *  1、QuorumPeer 对象中，有很多的成员变量，这些成员变量就是zoo.cfg 中的某些配置的值
             *  2、把 Properties 这个 Map 中的 key-value 解析出来，设置到 QuorumPeer
             */
            parseProperties(cfg);

        } catch(IOException e) {
            throw new ConfigException("Error processing " + path, e);
        } catch(IllegalArgumentException e) {
            throw new ConfigException("Error processing " + path, e);
        }

        if(dynamicConfigFileStr != null) {
            try {
                Properties dynamicCfg = new Properties();
                FileInputStream inConfig = new FileInputStream(dynamicConfigFileStr);
                try {
                    dynamicCfg.load(inConfig);
                    if(dynamicCfg.getProperty("version") != null) {
                        throw new ConfigException("dynamic file shouldn't have version inside");
                    }

                    String version = getVersionFromFilename(dynamicConfigFileStr);
                    // If there isn't any version associated with the filename,
                    // the default version is 0.
                    if(version != null) {
                        dynamicCfg.setProperty("version", version);
                    }
                } finally {
                    inConfig.close();
                }
                setupQuorumPeerConfig(dynamicCfg, false);

            } catch(IOException e) {
                throw new ConfigException("Error processing " + dynamicConfigFileStr, e);
            } catch(IllegalArgumentException e) {
                throw new ConfigException("Error processing " + dynamicConfigFileStr, e);
            }
            File nextDynamicConfigFile = new File(configFileStr + nextDynamicConfigFileSuffix);
            if(nextDynamicConfigFile.exists()) {
                try {
                    Properties dynamicConfigNextCfg = new Properties();
                    FileInputStream inConfigNext = new FileInputStream(nextDynamicConfigFile);
                    try {
                        dynamicConfigNextCfg.load(inConfigNext);
                    } finally {
                        inConfigNext.close();
                    }
                    boolean isHierarchical = false;
                    for(Entry<Object, Object> entry : dynamicConfigNextCfg.entrySet()) {
                        String key = entry.getKey().toString().trim();
                        if(key.startsWith("group") || key.startsWith("weight")) {
                            isHierarchical = true;
                            break;
                        }
                    }
                    lastSeenQuorumVerifier = createQuorumVerifier(dynamicConfigNextCfg, isHierarchical);
                } catch(IOException e) {
                    LOG.warn("NextQuorumVerifier is initiated to null");
                }
            }
        }
    }

    // This method gets the version from the end of dynamic file name.
    // For example, "zoo.cfg.dynamic.0" returns initial version "0".
    // "zoo.cfg.dynamic.1001" returns version of hex number "0x1001".
    // If a dynamic file name doesn't have any version at the end of file,
    // e.g. "zoo.cfg.dynamic", it returns null.
    public static String getVersionFromFilename(String filename) {
        int i = filename.lastIndexOf('.');
        if(i < 0 || i >= filename.length()) {
            return null;
        }

        String hexVersion = filename.substring(i + 1);
        try {
            long version = Long.parseLong(hexVersion, 16);
            return Long.toHexString(version);
        } catch(NumberFormatException e) {
            return null;
        }
    }

    /**
     * // TODO_MA 注释： 该方法是用来解析 zoo.cfg 配置文件中的内容的。
     * // TODO_MA 注释： 各位可以思考，该方法存在的问题是什么？可以做如何改进？
     * Parse config from a Properties.
     *
     * @param zkProp Properties to parse from.
     * @throws IOException
     * @throws ConfigException
     *
     * // TODO_MA 马中华 注释： 数据存储目录 和 端口的解析
     * // TODO_MA 马中华 注释： myid 的解析
     *
     * // TODO_MA 马中华 注释： 这个方法的核心逻辑只有一个目标：
     * // TODO_MA 马中华 注释： 吧 Properties 对象中的 key-value 解析设置成 QuorumPeerConfig 的成员变量来保存
     * // TODO_MA 马中华 注释： 之后呢，会把  QuorumPeerConfig 中的每个成员变量的值，然后设置到 QuorumPeer 类的成员变量
     * // TODO_MA 马中华 注释： QuorumPeerConfig 就是一个 容器
     */
    public void parseProperties(Properties zkProp) throws IOException, ConfigException {
        int clientPort = 0;
        int secureClientPort = 0;
        int observerMasterPort = 0;
        String clientPortAddress = null;
        String secureClientPortAddress = null;
        VerifyingFileFactory vff = new VerifyingFileFactory.Builder(LOG).warnForRelativePath().build();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         *  1、if else 写法
         *      代码可读性好，维护难度大，如果增加配置，或者减少配置，都需要进行维护
         *  2、使用类似于 Hadoop Spark 等的 key-value 方法来存入 map
         *      更通用，更易维护；不好校验，不易读
         *  -
         *  当前这个 for 循环每次遍历到的就是一个配置项： key-value
         *  -
         *  我发现的另外一个问题，：改了
         *  原来 解析 myid 配置文件的代码，就在当前这个方法的最后。！
         *  现在改了： 封装了一下。
         */
        for(Entry<Object, Object> entry : zkProp.entrySet()) {

            // TODO_MA 注释： 从 entry 中获取 key-value
            String key = entry.getKey().toString().trim();
            String value = entry.getValue().toString().trim();

            // TODO_MA 注释： 如果可以的话，其实可以使用一个 map 数据结构来存储所有的这些配置
            // TODO_MA 注释： map.put(key, value)；

            // TODO_MA 注释： 如果此时遍历到的 key 是 dataDir， 那么 遍历到的 value ，就设置成为 config 对象的 dataDir 成员变量的值
            if(key.equals("dataDir")) {
                dataDir = vff.create(value);
            }

            else if(key.equals("dataLogDir")) {
                dataLogDir = vff.create(value);
            } else if(key.equals("clientPort")) {
                clientPort = Integer.parseInt(value);
            } else if(key.equals("localSessionsEnabled")) {
                localSessionsEnabled = Boolean.parseBoolean(value);
            } else if(key.equals("localSessionsUpgradingEnabled")) {
                localSessionsUpgradingEnabled = Boolean.parseBoolean(value);
            } else if(key.equals("clientPortAddress")) {
                clientPortAddress = value.trim();
            } else if(key.equals("secureClientPort")) {
                secureClientPort = Integer.parseInt(value);
            } else if(key.equals("secureClientPortAddress")) {
                secureClientPortAddress = value.trim();
            } else if(key.equals("observerMasterPort")) {
                observerMasterPort = Integer.parseInt(value);
            } else if(key.equals("clientPortListenBacklog")) {
                clientPortListenBacklog = Integer.parseInt(value);
            } else if(key.equals("tickTime")) {
                tickTime = Integer.parseInt(value);
            } else if(key.equals("maxClientCnxns")) {
                maxClientCnxns = Integer.parseInt(value);
            } else if(key.equals("minSessionTimeout")) {
                minSessionTimeout = Integer.parseInt(value);
            } else if(key.equals("maxSessionTimeout")) {
                maxSessionTimeout = Integer.parseInt(value);
            } else if(key.equals("initLimit")) {
                initLimit = Integer.parseInt(value);
            } else if(key.equals("syncLimit")) {
                syncLimit = Integer.parseInt(value);
            } else if(key.equals("connectToLearnerMasterLimit")) {
                connectToLearnerMasterLimit = Integer.parseInt(value);
            }

            // TODO_MA 注释： electionAlg = 3, 实现是 FastLeaderElection， 具体算法是 Fast Paxos 实现
            else if(key.equals("electionAlg")) {
                electionAlg = Integer.parseInt(value);
                if(electionAlg != 3) {
                    throw new ConfigException("Invalid electionAlg value. Only 3 is supported.");
                }
            } else if(key.equals("quorumListenOnAllIPs")) {
                quorumListenOnAllIPs = Boolean.parseBoolean(value);
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             *  1、observer + participant（有选举权和被选举权的节点：Leader 和 Follower）只能出现在配置中
             *  2、leader + learner（Follower 和 Observer ）
             */
            else if(key.equals("peerType")) {
                if(value.toLowerCase().equals("observer")) {
                    peerType = LearnerType.OBSERVER;
                } else if(value.toLowerCase().equals("participant")) {
                    peerType = LearnerType.PARTICIPANT;
                } else {
                    throw new ConfigException("Unrecognised peertype: " + value);
                }
            } else if(key.equals("syncEnabled")) {
                syncEnabled = Boolean.parseBoolean(value);
            } else if(key.equals("dynamicConfigFile")) {
                dynamicConfigFileStr = value;
            } else if(key.equals("autopurge.snapRetainCount")) {
                snapRetainCount = Integer.parseInt(value);
            } else if(key.equals("autopurge.purgeInterval")) {
                purgeInterval = Integer.parseInt(value);
            } else if(key.equals("standaloneEnabled")) {
                if(value.toLowerCase().equals("true")) {
                    setStandaloneEnabled(true);
                } else if(value.toLowerCase().equals("false")) {
                    setStandaloneEnabled(false);
                } else {
                    throw new ConfigException(
                            "Invalid option " + value + " for standalone mode. Choose 'true' or 'false.'");
                }
            } else if(key.equals("reconfigEnabled")) {
                if(value.toLowerCase().equals("true")) {
                    setReconfigEnabled(true);
                } else if(value.toLowerCase().equals("false")) {
                    setReconfigEnabled(false);
                } else {
                    throw new ConfigException(
                            "Invalid option " + value + " for reconfigEnabled flag. Choose 'true' or 'false.'");
                }
            } else if(key.equals("sslQuorum")) {
                sslQuorum = Boolean.parseBoolean(value);
            } else if(key.equals("portUnification")) {
                shouldUsePortUnification = Boolean.parseBoolean(value);
            } else if(key.equals("sslQuorumReloadCertFiles")) {
                sslQuorumReloadCertFiles = Boolean.parseBoolean(value);
            } else if((key.startsWith("server.") || key.startsWith("group") || key.startsWith("weight")) && zkProp
                    .containsKey("dynamicConfigFile")) {
                throw new ConfigException("parameter: " + key + " must be in a separate dynamic config file");
            } else if(key.equals(QuorumAuth.QUORUM_SASL_AUTH_ENABLED)) {
                quorumEnableSasl = Boolean.parseBoolean(value);
            } else if(key.equals(QuorumAuth.QUORUM_SERVER_SASL_AUTH_REQUIRED)) {
                quorumServerRequireSasl = Boolean.parseBoolean(value);
            } else if(key.equals(QuorumAuth.QUORUM_LEARNER_SASL_AUTH_REQUIRED)) {
                quorumLearnerRequireSasl = Boolean.parseBoolean(value);
            } else if(key.equals(QuorumAuth.QUORUM_LEARNER_SASL_LOGIN_CONTEXT)) {
                quorumLearnerLoginContext = value;
            } else if(key.equals(QuorumAuth.QUORUM_SERVER_SASL_LOGIN_CONTEXT)) {
                quorumServerLoginContext = value;
            } else if(key.equals(QuorumAuth.QUORUM_KERBEROS_SERVICE_PRINCIPAL)) {
                quorumServicePrincipal = value;
            } else if(key.equals("quorum.cnxn.threads.size")) {
                quorumCnxnThreadsSize = Integer.parseInt(value);
            } else if(key.equals(JvmPauseMonitor.INFO_THRESHOLD_KEY)) {
                jvmPauseInfoThresholdMs = Long.parseLong(value);
            } else if(key.equals(JvmPauseMonitor.WARN_THRESHOLD_KEY)) {
                jvmPauseWarnThresholdMs = Long.parseLong(value);
            } else if(key.equals(JvmPauseMonitor.SLEEP_TIME_MS_KEY)) {
                jvmPauseSleepTimeMs = Long.parseLong(value);
            } else if(key.equals(JvmPauseMonitor.JVM_PAUSE_MONITOR_FEATURE_SWITCH_KEY)) {
                jvmPauseMonitorToRun = Boolean.parseBoolean(value);
            } else if(key.equals("metricsProvider.className")) {
                metricsProviderClassName = value;
            } else if(key.startsWith("metricsProvider.")) {
                String keyForMetricsProvider = key.substring(16);
                metricsProviderConfiguration.put(keyForMetricsProvider, value);
            } else if(key.equals("multiAddress.enabled")) {
                multiAddressEnabled = Boolean.parseBoolean(value);
            } else if(key.equals("multiAddress.reachabilityCheckTimeoutMs")) {
                multiAddressReachabilityCheckTimeoutMs = Integer.parseInt(value);
            } else if(key.equals("multiAddress.reachabilityCheckEnabled")) {
                multiAddressReachabilityCheckEnabled = Boolean.parseBoolean(value);
            } else {
                System.setProperty("zookeeper." + key, value);
            }
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 到此为止解析结束
         *  接下来，执行各种参数的校验！
         */

        if(!quorumEnableSasl && quorumServerRequireSasl) {
            throw new IllegalArgumentException(
                    QuorumAuth.QUORUM_SASL_AUTH_ENABLED + " is disabled, so cannot enable " + QuorumAuth.QUORUM_SERVER_SASL_AUTH_REQUIRED);
        }
        if(!quorumEnableSasl && quorumLearnerRequireSasl) {
            throw new IllegalArgumentException(
                    QuorumAuth.QUORUM_SASL_AUTH_ENABLED + " is disabled, so cannot enable " + QuorumAuth.QUORUM_LEARNER_SASL_AUTH_REQUIRED);
        }
        // If quorumpeer learner is not auth enabled then self won't be able to
        // join quorum. So this condition is ensuring that the quorumpeer learner
        // is also auth enabled while enabling quorum server require sasl.
        if(!quorumLearnerRequireSasl && quorumServerRequireSasl) {
            throw new IllegalArgumentException(
                    QuorumAuth.QUORUM_LEARNER_SASL_AUTH_REQUIRED + " is disabled, so cannot enable " + QuorumAuth.QUORUM_SERVER_SASL_AUTH_REQUIRED);
        }

        // Reset to MIN_SNAP_RETAIN_COUNT if invalid (less than 3)
        // PurgeTxnLog.purge(File, File, int) will not allow to purge less than 3.
        if(snapRetainCount < MIN_SNAP_RETAIN_COUNT) {
            LOG.warn(
                    "Invalid autopurge.snapRetainCount: " + snapRetainCount + ". Defaulting to " + MIN_SNAP_RETAIN_COUNT);
            snapRetainCount = MIN_SNAP_RETAIN_COUNT;
        }

        if(dataDir == null) {
            throw new IllegalArgumentException("dataDir is not set");
        }
        if(dataLogDir == null) {
            dataLogDir = dataDir;
        }

        if(clientPort == 0) {
            LOG.info("clientPort is not set");
            if(clientPortAddress != null) {
                throw new IllegalArgumentException("clientPortAddress is set but clientPort is not set");
            }
        } else if(clientPortAddress != null) {
            this.clientPortAddress = new InetSocketAddress(InetAddress.getByName(clientPortAddress), clientPort);
            LOG.info("clientPortAddress is {}", formatInetAddr(this.clientPortAddress));
        } else {
            this.clientPortAddress = new InetSocketAddress(clientPort);
            LOG.info("clientPortAddress is {}", formatInetAddr(this.clientPortAddress));
        }

        if(secureClientPort == 0) {
            LOG.info("secureClientPort is not set");
            if(secureClientPortAddress != null) {
                throw new IllegalArgumentException("secureClientPortAddress is set but secureClientPort is not set");
            }
        } else if(secureClientPortAddress != null) {
            this.secureClientPortAddress = new InetSocketAddress(InetAddress.getByName(secureClientPortAddress),
                    secureClientPort);
            LOG.info("secureClientPortAddress is {}", formatInetAddr(this.secureClientPortAddress));
        } else {
            this.secureClientPortAddress = new InetSocketAddress(secureClientPort);
            LOG.info("secureClientPortAddress is {}", formatInetAddr(this.secureClientPortAddress));
        }
        if(this.secureClientPortAddress != null) {
            configureSSLAuth();
        }

        if(observerMasterPort <= 0) {
            LOG.info("observerMasterPort is not set");
        } else {
            this.observerMasterPort = observerMasterPort;
            LOG.info("observerMasterPort is {}", observerMasterPort);
        }

        if(tickTime == 0) {
            throw new IllegalArgumentException("tickTime is not set");
        }

        minSessionTimeout = minSessionTimeout == -1 ? tickTime * 2 : minSessionTimeout;
        maxSessionTimeout = maxSessionTimeout == -1 ? tickTime * 20 : maxSessionTimeout;

        if(minSessionTimeout > maxSessionTimeout) {
            throw new IllegalArgumentException("minSessionTimeout must not be larger than maxSessionTimeout");
        }

        LOG.info("metricsProvider.className is {}", metricsProviderClassName);
        try {
            Class.forName(metricsProviderClassName, false, Thread.currentThread().getContextClassLoader());
        } catch(ClassNotFoundException error) {
            throw new IllegalArgumentException("metrics provider class was not found", error);
        }

        // TODO_MA 注释： zookeeper-3.5x 往后提供了一个动态配置，为了避免一个 滚动重启
        // TODO_MA 注释： 滚动重启 有可能造成数据不一致
        // backward compatibility - dynamic configuration in the same file as
        // static configuration params see writeDynamicConfig()
        if(dynamicConfigFileStr == null) {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：  解析 myid
             */
            setupQuorumPeerConfig(zkProp, true);

            if(isDistributed() && isReconfigEnabled()) {
                // we don't backup static config for standalone mode.
                // we also don't backup if reconfig feature is disabled.
                backupOldConfig();
            }
        }
    }

    /**
     * Configure SSL authentication only if it is not configured.
     *
     * @throws ConfigException If authentication scheme is configured but authentication
     *                         provider is not configured.
     */
    public static void configureSSLAuth() throws ConfigException {
        try(ClientX509Util clientX509Util = new ClientX509Util()) {
            String sslAuthProp = ProviderRegistry.AUTHPROVIDER_PROPERTY_PREFIX + System
                    .getProperty(clientX509Util.getSslAuthProviderProperty(), "x509");
            if(System.getProperty(sslAuthProp) == null) {
                if((ProviderRegistry.AUTHPROVIDER_PROPERTY_PREFIX + "x509").equals(sslAuthProp)) {
                    System.setProperty(ProviderRegistry.AUTHPROVIDER_PROPERTY_PREFIX + "x509",
                            "org.apache.zookeeper.server.auth.X509AuthenticationProvider");
                } else {
                    throw new ConfigException("No auth provider configured for the SSL authentication scheme '" + System
                            .getProperty(clientX509Util.getSslAuthProviderProperty()) + "'.");
                }
            }
        }
    }

    /**
     * Backward compatibility -- It would backup static config file on bootup
     * if users write dynamic configuration in "zoo.cfg".
     */
    private void backupOldConfig() throws IOException {
        new AtomicFileWritingIdiom(new File(configFileStr + ".bak"), new OutputStreamStatement() {
            @Override
            public void write(OutputStream output) throws IOException {
                InputStream input = null;
                try {
                    input = new FileInputStream(new File(configFileStr));
                    byte[] buf = new byte[1024];
                    int bytesRead;
                    while((bytesRead = input.read(buf)) > 0) {
                        output.write(buf, 0, bytesRead);
                    }
                } finally {
                    if(input != null) {
                        input.close();
                    }
                }
            }
        });
    }

    /**
     * Writes dynamic configuration file
     */
    public static void writeDynamicConfig(final String dynamicConfigFilename, final QuorumVerifier qv,
            final boolean needKeepVersion) throws IOException {

        new AtomicFileWritingIdiom(new File(dynamicConfigFilename), new WriterStatement() {
            @Override
            public void write(Writer out) throws IOException {
                Properties cfg = new Properties();
                cfg.load(new StringReader(qv.toString()));

                List<String> servers = new ArrayList<String>();
                for(Entry<Object, Object> entry : cfg.entrySet()) {
                    String key = entry.getKey().toString().trim();
                    if(!needKeepVersion && key.startsWith("version")) {
                        continue;
                    }

                    String value = entry.getValue().toString().trim();
                    servers.add(key.concat("=").concat(value));
                }

                Collections.sort(servers);
                out.write(StringUtils.joinStrings(servers, "\n"));
            }
        });
    }

    /**
     * Edit static config file.
     * If there are quorum information in static file, e.g. "server.X", "group",
     * it will remove them.
     * If it needs to erase client port information left by the old config,
     * "eraseClientPortAddress" should be set true.
     * It should also updates dynamic file pointer on reconfig.
     */
    public static void editStaticConfig(final String configFileStr, final String dynamicFileStr,
            final boolean eraseClientPortAddress) throws IOException {
        // Some tests may not have a static config file.
        if(configFileStr == null) {
            return;
        }

        File configFile = (new VerifyingFileFactory.Builder(LOG).warnForRelativePath().failForNonExistingPath().build())
                .create(configFileStr);

        final File dynamicFile = (new VerifyingFileFactory.Builder(LOG).warnForRelativePath().failForNonExistingPath()
                .build()).create(dynamicFileStr);

        final Properties cfg = new Properties();
        FileInputStream in = new FileInputStream(configFile);
        try {
            cfg.load(in);
        } finally {
            in.close();
        }

        new AtomicFileWritingIdiom(new File(configFileStr), new WriterStatement() {
            @Override
            public void write(Writer out) throws IOException {
                for(Entry<Object, Object> entry : cfg.entrySet()) {
                    String key = entry.getKey().toString().trim();

                    if(key.startsWith("server.") || key.startsWith("group") || key.startsWith("weight") || key
                            .startsWith("dynamicConfigFile") || key
                            .startsWith("peerType") || (eraseClientPortAddress && (key.startsWith("clientPort") || key
                            .startsWith("clientPortAddress")))) {
                        // not writing them back to static file
                        continue;
                    }

                    String value = entry.getValue().toString().trim();
                    out.write(key.concat("=").concat(value).concat("\n"));
                }

                // updates the dynamic file pointer
                String dynamicConfigFilePath = PathUtils.normalizeFileSystemPath(dynamicFile.getCanonicalPath());
                out.write("dynamicConfigFile=".concat(dynamicConfigFilePath).concat("\n"));
            }
        });
    }

    public static void deleteFile(String filename) {
        if(filename == null) {
            return;
        }
        File f = new File(filename);
        if(f.exists()) {
            try {
                f.delete();
            } catch(Exception e) {
                LOG.warn("deleting {} failed", filename);
            }
        }
    }

    private static QuorumVerifier createQuorumVerifier(Properties dynamicConfigProp,
            boolean isHierarchical) throws ConfigException {

        if(isHierarchical) {
            return new QuorumHierarchical(dynamicConfigProp);
        } else {
            /*
             * The default QuorumVerifier is QuorumMaj
             */
            //LOG.info("Defaulting to majority quorums");
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： QuorumMaj 默认实现
             *  在构造 QuorumMaj 的时候，对 zoo.cfg 的服务器节点配置做了解析
             */
            return new QuorumMaj(dynamicConfigProp);
        }
    }

    void setupQuorumPeerConfig(Properties prop,
            boolean configBackwardCompatibilityMode) throws IOException, ConfigException {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： QuorumVerifier 这个对象定义了一个算法： 少数服从多数
         *  1、解析
         *  server.2=bigdata02:2888
         *  server.3=bigdata03:2888:3888
         *  server.4=bigdata04:2888:3888:participant
         *  server.5=bigdata05:2888:3888:observer
         */
        quorumVerifier = parseDynamicConfig(prop, electionAlg, true, configBackwardCompatibilityMode);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 解析 myid
         */
        setupMyId();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 校验 2181
         */
        setupClientPort();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        setupPeerType();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 做一些参数的校验
         */
        checkValidity();
    }

    /**
     * Parse dynamic configuration file and return
     * quorumVerifier for new configuration.
     * // TODO_MA 注释： 这个方法两个作用：
     * // TODO_MA 注释： 1、初始化得到一个 少数服从多数/过半通过原则 的一个算法实例
     * // TODO_MA 注释： 2、其实是解析得到一个 all voting observer
     *
     * @param dynamicConfigProp Properties to parse from.
     * @throws IOException
     * @throws ConfigException
     */
    public static QuorumVerifier parseDynamicConfig(Properties dynamicConfigProp, int eAlg, boolean warnings,
            boolean configBackwardCompatibilityMode) throws IOException, ConfigException {
        boolean isHierarchical = false;

        // TODO_MA 马中华 注释： 从 3.5 往后，可以给 zk 节点配置权重
        for(Entry<Object, Object> entry : dynamicConfigProp.entrySet()) {
            String key = entry.getKey().toString().trim();
            if(key.startsWith("group") || key.startsWith("weight")) {
                isHierarchical = true;
            } else if(!configBackwardCompatibilityMode && !key.startsWith("server.") && !key.equals("version")) {
                LOG.info(dynamicConfigProp.toString());
                throw new ConfigException("Unrecognised parameter: " + key);
            }
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        QuorumVerifier qv = createQuorumVerifier(dynamicConfigProp, isHierarchical);

        int numParticipators = qv.getVotingMembers().size();
        int numObservers = qv.getObservingMembers().size();

        if(numParticipators == 0) {
            if(!standaloneEnabled) {
                throw new IllegalArgumentException(
                        "standaloneEnabled = false then " + "number of participants should be >0");
            }
            if(numObservers > 0) {
                throw new IllegalArgumentException("Observers w/o participants is an invalid configuration");
            }
        } else if(numParticipators == 1 && standaloneEnabled) {
            // HBase currently adds a single server line to the config, for
            // b/w compatibility reasons we need to keep this here. If standaloneEnabled
            // is true, the QuorumPeerMain script will create a standalone server instead
            // of a quorum configuration
            LOG.error("Invalid configuration, only one server specified (ignoring)");
            if(numObservers > 0) {
                throw new IllegalArgumentException("Observers w/o quorum is an invalid configuration");
            }
        } else {
            if(warnings) {
                if(numParticipators <= 2) {
                    LOG.warn("No server failure will be tolerated. You need at least 3 servers.");
                } else if(numParticipators % 2 == 0) {
                    LOG.warn("Non-optimial configuration, consider an odd number of servers.");
                }
            }

            for(QuorumServer s : qv.getVotingMembers().values()) {
                if(s.electionAddr == null) {
                    throw new IllegalArgumentException("Missing election port for server: " + s.id);
                }
            }
        }
        return qv;
    }

    private void setupMyId() throws IOException {

        // TODO_MA 注释： 搭建集群的时候，需要在 数据 datadir 目中中，给配置一个 叫做 myid 的文件
        // TODO_MA 注释： 只有一行，存储了当前 server 的 id 编号（1-255之间）
        File myIdFile = new File(dataDir, "myid");

        // TODO_MA 注释：
        // standalone server doesn't need myid file.
        if(!myIdFile.isFile()) {
            return;
        }

        // TODO_MA 注释：
        BufferedReader br = new BufferedReader(new FileReader(myIdFile));
        String myIdString;
        try {
            myIdString = br.readLine();
        } finally {
            br.close();
        }

        // TODO_MA 注释： 在 zookeeper 关于 myid 有三个类似的概念： myid serverid sid
        // TODO_MA 注释： 执行类型转换
        try {
            serverId = Long.parseLong(myIdString);
            MDC.put("myid", myIdString);
        } catch(NumberFormatException e) {
            throw new IllegalArgumentException("serverid " + myIdString + " is not a number");
        }
    }

    private void setupClientPort() throws ConfigException {
        if(serverId == UNSET_SERVERID) {
            return;
        }
        QuorumServer qs = quorumVerifier.getAllMembers().get(serverId);

        if(clientPortAddress != null && qs != null && qs.clientAddr != null) {
            if((!clientPortAddress.getAddress().isAnyLocalAddress() && !clientPortAddress
                    .equals(qs.clientAddr)) || (clientPortAddress.getAddress().isAnyLocalAddress() && clientPortAddress
                    .getPort() != qs.clientAddr.getPort())) {
                throw new ConfigException(
                        "client address for this server (id = " + serverId + ") in static config file is " + clientPortAddress + " is different from client address found in dynamic file: " + qs.clientAddr);
            }
        }
        if(qs != null && qs.clientAddr != null) {
            clientPortAddress = qs.clientAddr;
        }
        if(qs != null && qs.clientAddr == null) {
            qs.clientAddr = clientPortAddress;
            qs.isClientAddrFromStatic = true;
        }
    }

    // TODO_MA 注释： 根据 serverid 确认服务器的类别
    private void setupPeerType() {

        // TODO_MA 注释： 先拿到当前这个 server 的 peerType：  OBSERVER PARTICIPANT
        // Warn about inconsistent peer type
        LearnerType roleByServersList = quorumVerifier.getObservingMembers()
                .containsKey(serverId) ? LearnerType.OBSERVER : LearnerType.PARTICIPANT;

        if(roleByServersList != peerType) {
            LOG.warn("Peer type from servers list ({}) doesn't match peerType ({}). Defaulting to servers list.",
                    roleByServersList, peerType);
            peerType = roleByServersList;
        }
    }

    public void checkValidity() throws IOException, ConfigException {
        if(isDistributed()) {
            if(initLimit == 0) {
                throw new IllegalArgumentException("initLimit is not set");
            }
            if(syncLimit == 0) {
                throw new IllegalArgumentException("syncLimit is not set");
            }
            if(serverId == UNSET_SERVERID) {
                throw new IllegalArgumentException("myid file is missing");
            }
        }
    }

    public InetSocketAddress getClientPortAddress() {
        return clientPortAddress;
    }

    public InetSocketAddress getSecureClientPortAddress() {
        return secureClientPortAddress;
    }

    public int getObserverMasterPort() {
        return observerMasterPort;
    }

    public File getDataDir() {
        return dataDir;
    }

    public File getDataLogDir() {
        return dataLogDir;
    }

    public String getInitialConfig() {
        return initialConfig;
    }

    public int getTickTime() {
        return tickTime;
    }

    public int getMaxClientCnxns() {
        return maxClientCnxns;
    }

    public int getMinSessionTimeout() {
        return minSessionTimeout;
    }

    public int getMaxSessionTimeout() {
        return maxSessionTimeout;
    }

    public String getMetricsProviderClassName() {
        return metricsProviderClassName;
    }

    public Properties getMetricsProviderConfiguration() {
        return metricsProviderConfiguration;
    }

    public boolean areLocalSessionsEnabled() {
        return localSessionsEnabled;
    }

    public boolean isLocalSessionsUpgradingEnabled() {
        return localSessionsUpgradingEnabled;
    }

    public boolean isSslQuorum() {
        return sslQuorum;
    }

    public boolean shouldUsePortUnification() {
        return shouldUsePortUnification;
    }

    public int getClientPortListenBacklog() {
        return clientPortListenBacklog;
    }

    public int getInitLimit() {
        return initLimit;
    }

    public int getSyncLimit() {
        return syncLimit;
    }

    public int getConnectToLearnerMasterLimit() {
        return connectToLearnerMasterLimit;
    }

    public int getElectionAlg() {
        return electionAlg;
    }

    public int getElectionPort() {
        return electionPort;
    }

    public int getSnapRetainCount() {
        return snapRetainCount;
    }

    public int getPurgeInterval() {
        return purgeInterval;
    }

    public boolean getSyncEnabled() {
        return syncEnabled;
    }

    public QuorumVerifier getQuorumVerifier() {
        return quorumVerifier;
    }

    public QuorumVerifier getLastSeenQuorumVerifier() {
        return lastSeenQuorumVerifier;
    }

    public Map<Long, QuorumServer> getServers() {
        // returns all configuration servers -- participants and observers
        return Collections.unmodifiableMap(quorumVerifier.getAllMembers());
    }

    public long getJvmPauseInfoThresholdMs() {
        return jvmPauseInfoThresholdMs;
    }

    public long getJvmPauseWarnThresholdMs() {
        return jvmPauseWarnThresholdMs;
    }

    public long getJvmPauseSleepTimeMs() {
        return jvmPauseSleepTimeMs;
    }

    public boolean isJvmPauseMonitorToRun() {
        return jvmPauseMonitorToRun;
    }

    public long getServerId() {
        return serverId;
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：
     */
    public boolean isDistributed() {
        return quorumVerifier != null && (!standaloneEnabled || quorumVerifier.getVotingMembers().size() > 1);
    }

    public LearnerType getPeerType() {
        return peerType;
    }

    public String getConfigFilename() {
        return configFileStr;
    }

    public Boolean getQuorumListenOnAllIPs() {
        return quorumListenOnAllIPs;
    }

    public boolean isMultiAddressEnabled() {
        return multiAddressEnabled;
    }

    public boolean isMultiAddressReachabilityCheckEnabled() {
        return multiAddressReachabilityCheckEnabled;
    }

    public int getMultiAddressReachabilityCheckTimeoutMs() {
        return multiAddressReachabilityCheckTimeoutMs;
    }

    public static boolean isStandaloneEnabled() {
        return standaloneEnabled;
    }

    public static void setStandaloneEnabled(boolean enabled) {
        standaloneEnabled = enabled;
    }

    public static boolean isReconfigEnabled() {
        return reconfigEnabled;
    }

    public static void setReconfigEnabled(boolean enabled) {
        reconfigEnabled = enabled;
    }

}
