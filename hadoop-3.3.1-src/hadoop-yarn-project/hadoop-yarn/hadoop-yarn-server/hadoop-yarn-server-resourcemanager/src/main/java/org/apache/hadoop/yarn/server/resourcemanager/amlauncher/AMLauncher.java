/**
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

package org.apache.hadoop.yarn.server.resourcemanager.amlauncher;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.client.NMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.security.AMSecretKeys;
import org.apache.hadoop.yarn.server.webproxy.ProxyCA;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * The launch of the AM itself.
 */
public class AMLauncher implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(AMLauncher.class);

    private ContainerManagementProtocol containerMgrProxy;

    private final RMAppAttempt application;
    private final Configuration conf;
    private final AMLauncherEventType eventType;
    private final RMContext rmContext;
    private final Container masterContainer;
    private boolean timelineServiceV2Enabled;

    @SuppressWarnings("rawtypes")
    private final EventHandler handler;

    public AMLauncher(RMContext rmContext, RMAppAttempt application, AMLauncherEventType eventType, Configuration conf) {
        this.application = application;
        this.conf = conf;
        this.eventType = eventType;
        this.rmContext = rmContext;
        this.handler = rmContext.getDispatcher().getEventHandler();
        this.masterContainer = application.getMasterContainer();
        this.timelineServiceV2Enabled = YarnConfiguration.timelineServiceV2Enabled(conf);
    }

    private void connect() throws IOException {
        ContainerId masterContainerID = masterContainer.getId();

        containerMgrProxy = getContainerMgrProxy(masterContainerID);
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： run() 的逻辑，就是调用 launch() 方法！
     */
    private void launch() throws IOException, YarnException {
        connect();
        ContainerId masterContainerID = masterContainer.getId();
        ApplicationSubmissionContext applicationContext = application.getSubmissionContext();
        LOG.info("Setting up container " + masterContainer + " for AM " + application.getAppAttemptId());
        ContainerLaunchContext launchContext = createAMContainerLaunchContext(applicationContext, masterContainerID);

        StartContainerRequest scRequest = StartContainerRequest.newInstance(launchContext, masterContainer.getContainerToken());
        List<StartContainerRequest> list = new ArrayList<StartContainerRequest>();
        list.add(scRequest);
        StartContainersRequest allRequests = StartContainersRequest.newInstance(list);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 这是 RM 发送命令让 NM 启动 Container
         *  这个请求的 request 对象中，是包含了 :  shell 命令 = java MRAppMaster ！
         *  当 NM 接收到这个 RPC 请求的时候，就会去初始化 Contaienr 使用这个 Contianer 的资源来启动 MRAppmaster 这个 JVM
         *  到此为止，ApplicationMaster 的启动就搞定了 。！
         *  -
         *  RM 作为 RPC 客户端， NM 作为 RPC 服务端，之间的通信协议是： ContainerManagementProtocol
         *  作用： RM 发送 RPC 请求让 NM 启动 Container ，在这个 Container 中去启动 ApplicationMaster
         */
        StartContainersResponse response = containerMgrProxy.startContainers(allRequests);

        if (response.getFailedRequests() != null && response.getFailedRequests().containsKey(masterContainerID)) {
            Throwable t = response.getFailedRequests().get(masterContainerID).deSerialize();
            parseAndThrowException(t);
        } else {
            LOG.info("Done launching container " + masterContainer + " for AM " + application.getAppAttemptId());
        }
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 当 AM 启动( java MRAppMaster )好了之后：
     *  1、AM 向 RM 注册
     *  2、AM 维持和 RM 之间的心跳
     *      心跳请求中，就包含了申请 container 的需求
     *  3、RM 会通过心跳响应，来给 AM 返回申请到的 Contianer 的列表
     *  4、发送 RPC 请求给提供 Container 的 NodeManager
     *  5、NM 启动 Container 执行一个 shell命令：java YarnChild
     */

    private void cleanup() throws IOException, YarnException {
        connect();
        ContainerId containerId = masterContainer.getId();
        List<ContainerId> containerIds = new ArrayList<ContainerId>();
        containerIds.add(containerId);
        StopContainersRequest stopRequest = StopContainersRequest.newInstance(containerIds);
        StopContainersResponse response = containerMgrProxy.stopContainers(stopRequest);
        if (response.getFailedRequests() != null && response.getFailedRequests().containsKey(containerId)) {
            Throwable t = response.getFailedRequests().get(containerId).deSerialize();
            parseAndThrowException(t);
        }
    }

    // Protected. For tests.
    protected ContainerManagementProtocol getContainerMgrProxy(final ContainerId containerId) {

        final NodeId node = masterContainer.getNodeId();
        final InetSocketAddress containerManagerConnectAddress = NetUtils.createSocketAddrForHost(node.getHost(), node.getPort());

        final YarnRPC rpc = getYarnRPC();

        UserGroupInformation currentUser = UserGroupInformation.createRemoteUser(containerId.getApplicationAttemptId().toString());

        String user = rmContext.getRMApps().get(containerId.getApplicationAttemptId().getApplicationId()).getUser();
        org.apache.hadoop.yarn.api.records.Token token = rmContext.getNMTokenSecretManager()
                .createNMToken(containerId.getApplicationAttemptId(), node, user);
        currentUser.addToken(ConverterUtils.convertFromYarn(token, containerManagerConnectAddress));

        return NMProxy.createNMProxy(conf, ContainerManagementProtocol.class, currentUser, rpc, containerManagerConnectAddress);
    }

    @VisibleForTesting
    protected YarnRPC getYarnRPC() {
        return YarnRPC.create(conf);  // TODO: Don't create again and again.
    }

    private ContainerLaunchContext createAMContainerLaunchContext(ApplicationSubmissionContext applicationMasterContext,
                                                                  ContainerId containerID) throws IOException {

        // Construct the actual Container
        ContainerLaunchContext container = applicationMasterContext.getAMContainerSpec();

        if (container == null) {
            throw new IOException(containerID + " has been cleaned before launched");
        }
        // Finalize the container
        setupTokens(container, containerID);
        // set the flow context optionally for timeline service v.2
        setFlowContext(container);

        return container;
    }

    @Private
    @VisibleForTesting
    protected void setupTokens(ContainerLaunchContext container, ContainerId containerID) throws IOException {
        Map<String, String> environment = container.getEnvironment();
        environment.put(ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV, application.getWebProxyBase());
        // Set AppSubmitTime to be consumable by the AM.
        ApplicationId applicationId = application.getAppAttemptId().getApplicationId();
        environment.put(ApplicationConstants.APP_SUBMIT_TIME_ENV,
                String.valueOf(rmContext.getRMApps().get(applicationId).getSubmitTime())
        );

        Credentials credentials = new Credentials();
        DataInputByteBuffer dibb = new DataInputByteBuffer();
        ByteBuffer tokens = container.getTokens();
        if (tokens != null) {
            // TODO: Don't do this kind of checks everywhere.
            dibb.reset(tokens);
            credentials.readTokenStorageStream(dibb);
            tokens.rewind();
        }

        // Add AMRMToken
        Token<AMRMTokenIdentifier> amrmToken = createAndSetAMRMToken();
        if (amrmToken != null) {
            credentials.addToken(amrmToken.getService(), amrmToken);
        }

        // Setup Keystore and Truststore
        String httpsPolicy = conf.get(YarnConfiguration.RM_APPLICATION_HTTPS_POLICY,
                YarnConfiguration.DEFAULT_RM_APPLICATION_HTTPS_POLICY
        );
        if (httpsPolicy.equals("LENIENT") || httpsPolicy.equals("STRICT")) {
            ProxyCA proxyCA = rmContext.getProxyCAManager().getProxyCA();
            try {
                String kPass = proxyCA.generateKeyStorePassword();
                byte[] keyStore = proxyCA.createChildKeyStore(applicationId, kPass);
                credentials.addSecretKey(AMSecretKeys.YARN_APPLICATION_AM_KEYSTORE, keyStore);
                credentials.addSecretKey(AMSecretKeys.YARN_APPLICATION_AM_KEYSTORE_PASSWORD,
                        kPass.getBytes(StandardCharsets.UTF_8)
                );
                String tPass = proxyCA.generateKeyStorePassword();
                byte[] trustStore = proxyCA.getChildTrustStore(tPass);
                credentials.addSecretKey(AMSecretKeys.YARN_APPLICATION_AM_TRUSTSTORE, trustStore);
                credentials.addSecretKey(AMSecretKeys.YARN_APPLICATION_AM_TRUSTSTORE_PASSWORD,
                        tPass.getBytes(StandardCharsets.UTF_8)
                );
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        DataOutputBuffer dob = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(dob);
        container.setTokens(ByteBuffer.wrap(dob.getData(), 0, dob.getLength()));
    }

    private void setFlowContext(ContainerLaunchContext container) {
        if (timelineServiceV2Enabled) {
            Map<String, String> environment = container.getEnvironment();
            ApplicationId applicationId = application.getAppAttemptId().getApplicationId();
            RMApp app = rmContext.getRMApps().get(applicationId);

            // initialize the flow in the environment with default values for those
            // that do not specify the flow tags
            // flow name: app name (or app id if app name is missing),
            // flow version: "1", flow run id: start time
            setFlowTags(environment, TimelineUtils.FLOW_NAME_TAG_PREFIX,
                    TimelineUtils.generateDefaultFlowName(app.getName(), applicationId)
            );
            setFlowTags(environment, TimelineUtils.FLOW_VERSION_TAG_PREFIX, TimelineUtils.DEFAULT_FLOW_VERSION);
            setFlowTags(environment, TimelineUtils.FLOW_RUN_ID_TAG_PREFIX, String.valueOf(app.getStartTime()));

            // Set flow context info: the flow context is received via the application
            // tags
            for (String tag : app.getApplicationTags()) {
                String[] parts = tag.split(":", 2);
                if (parts.length != 2 || parts[1].isEmpty()) {
                    continue;
                }
                switch (parts[0].toUpperCase()) {
                    case TimelineUtils.FLOW_NAME_TAG_PREFIX:
                        setFlowTags(environment, TimelineUtils.FLOW_NAME_TAG_PREFIX, parts[1]);
                        break;
                    case TimelineUtils.FLOW_VERSION_TAG_PREFIX:
                        setFlowTags(environment, TimelineUtils.FLOW_VERSION_TAG_PREFIX, parts[1]);
                        break;
                    case TimelineUtils.FLOW_RUN_ID_TAG_PREFIX:
                        setFlowTags(environment, TimelineUtils.FLOW_RUN_ID_TAG_PREFIX, parts[1]);
                        break;
                    default:
                        break;
                }
            }
        }
    }

    private static void setFlowTags(Map<String, String> environment, String tagPrefix, String value) {
        if (!value.isEmpty()) {
            environment.put(tagPrefix, value);
        }
    }

    @VisibleForTesting
    protected Token<AMRMTokenIdentifier> createAndSetAMRMToken() {
        Token<AMRMTokenIdentifier> amrmToken = this.rmContext.getAMRMTokenSecretManager()
                .createAndGetAMRMToken(application.getAppAttemptId());
        ((RMAppAttemptImpl) application).setAMRMToken(amrmToken);
        return amrmToken;
    }

    @SuppressWarnings("unchecked")
    public void run() {
        switch (eventType) {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 启动
             */
            case LAUNCH:
                try {
                    LOG.info("Launching master" + application.getAppAttemptId());
                    launch();
                    handler.handle(new RMAppAttemptEvent(application.getAppAttemptId(), RMAppAttemptEventType.LAUNCHED,
                            System.currentTimeMillis()
                    ));
                } catch (Exception ie) {
                    onAMLaunchFailed(masterContainer.getId(), ie);
                }
                break;

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            case CLEANUP:
                try {
                    LOG.info("Cleaning master " + application.getAppAttemptId());
                    cleanup();
                } catch (IOException ie) {
                    LOG.info("Error cleaning master ", ie);
                } catch (YarnException e) {
                    StringBuilder sb = new StringBuilder("Container ");
                    sb.append(masterContainer.getId().toString()).append(" is not handled by this NodeManager");
                    if (!e.getMessage().contains(sb.toString())) {
                        // Ignoring if container is already killed by Node Manager.
                        LOG.info("Error cleaning master ", e);
                    }
                }
                break;
            default:
                LOG.warn("Received unknown event-type " + eventType + ". Ignoring.");
                break;
        }
    }

    private void parseAndThrowException(Throwable t) throws YarnException, IOException {
        if (t instanceof YarnException) {
            throw (YarnException) t;
        } else if (t instanceof InvalidToken) {
            throw (InvalidToken) t;
        } else {
            throw (IOException) t;
        }
    }

    @SuppressWarnings("unchecked")
    protected void onAMLaunchFailed(ContainerId containerId, Exception ie) {
        String message = "Error launching " + application.getAppAttemptId() + ". Got exception: " + StringUtils.stringifyException(
                ie);
        LOG.info(message);
        handler.handle(new RMAppAttemptEvent(application.getAppAttemptId(), RMAppAttemptEventType.LAUNCH_FAILED, message));
    }
}
