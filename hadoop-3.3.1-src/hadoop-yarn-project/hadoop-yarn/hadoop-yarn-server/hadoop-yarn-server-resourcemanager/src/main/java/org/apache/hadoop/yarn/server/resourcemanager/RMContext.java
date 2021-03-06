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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceContext;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.ConfigurationProvider;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.nodelabels.NodeAttributesManager;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SystemCredentialsForAppsProto;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMDelegatedNodeLabelsUpdater;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystem;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceProfilesManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.monitor.RMAppLifetimeMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.PlacementConstraintManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.AllocationTagsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.distributed.QueueLimitCalculator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.MultiNodeSortingManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.ProxyCAManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMDelegationTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.timelineservice.RMTimelineCollectorManager;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.VolumeManager;

/**
 * Context of the ResourceManager.
 */
public interface RMContext extends ApplicationMasterServiceContext {

    Dispatcher getDispatcher();

    boolean isHAEnabled();

    HAServiceState getHAServiceState();

    RMStateStore getStateStore();

    ConcurrentMap<ApplicationId, RMApp> getRMApps();

    ConcurrentMap<ApplicationId, SystemCredentialsForAppsProto> getSystemCredentialsForApps();

    ConcurrentMap<NodeId, RMNode> getInactiveRMNodes();

    ConcurrentMap<NodeId, RMNode> getRMNodes();

    AMLivelinessMonitor getAMLivelinessMonitor();

    AMLivelinessMonitor getAMFinishingMonitor();

    ContainerAllocationExpirer getContainerAllocationExpirer();

    DelegationTokenRenewer getDelegationTokenRenewer();

    AMRMTokenSecretManager getAMRMTokenSecretManager();

    RMContainerTokenSecretManager getContainerTokenSecretManager();

    NMTokenSecretManagerInRM getNMTokenSecretManager();

    ResourceScheduler getScheduler();

    NodesListManager getNodesListManager();

    ClientToAMTokenSecretManagerInRM getClientToAMTokenSecretManager();

    AdminService getRMAdminService();

    ClientRMService getClientRMService();

    ApplicationMasterService getApplicationMasterService();

    ResourceTrackerService getResourceTrackerService();

    void setClientRMService(ClientRMService clientRMService);

    RMDelegationTokenSecretManager getRMDelegationTokenSecretManager();

    void setRMDelegationTokenSecretManager(RMDelegationTokenSecretManager delegationTokenSecretManager);

    RMApplicationHistoryWriter getRMApplicationHistoryWriter();

    void setRMApplicationHistoryWriter(RMApplicationHistoryWriter rmApplicationHistoryWriter);

    void setSystemMetricsPublisher(SystemMetricsPublisher systemMetricsPublisher);

    SystemMetricsPublisher getSystemMetricsPublisher();

    void setRMTimelineCollectorManager(RMTimelineCollectorManager timelineCollectorManager);

    RMTimelineCollectorManager getRMTimelineCollectorManager();

    ConfigurationProvider getConfigurationProvider();

    boolean isWorkPreservingRecoveryEnabled();

    RMNodeLabelsManager getNodeLabelManager();

    public void setNodeLabelManager(RMNodeLabelsManager mgr);

    NodeAttributesManager getNodeAttributesManager();

    void setNodeAttributesManager(NodeAttributesManager mgr);

    RMDelegatedNodeLabelsUpdater getRMDelegatedNodeLabelsUpdater();

    void setRMDelegatedNodeLabelsUpdater(RMDelegatedNodeLabelsUpdater nodeLabelsUpdater);

    long getEpoch();

    ReservationSystem getReservationSystem();

    boolean isSchedulerReadyForAllocatingContainers();

    Configuration getYarnConfiguration();

    PlacementManager getQueuePlacementManager();

    void setQueuePlacementManager(PlacementManager placementMgr);

    void setLeaderElectorService(EmbeddedElector elector);

    EmbeddedElector getLeaderElectorService();

    QueueLimitCalculator getNodeManagerQueueLimitCalculator();

    void setRMAppLifetimeMonitor(RMAppLifetimeMonitor rmAppLifetimeMonitor);

    RMAppLifetimeMonitor getRMAppLifetimeMonitor();

    String getHAZookeeperConnectionState();

    ResourceManager getResourceManager();

    ResourceProfilesManager getResourceProfilesManager();

    void setResourceProfilesManager(ResourceProfilesManager mgr);

    String getAppProxyUrl(Configuration conf, ApplicationId applicationId);

    AllocationTagsManager getAllocationTagsManager();

    void setAllocationTagsManager(AllocationTagsManager allocationTagsManager);

    PlacementConstraintManager getPlacementConstraintManager();

    void setPlacementConstraintManager(PlacementConstraintManager placementConstraintManager);

    MultiNodeSortingManager<SchedulerNode> getMultiNodeSortingManager();

    void setMultiNodeSortingManager(MultiNodeSortingManager<SchedulerNode> multiNodeSortingManager);

    ProxyCAManager getProxyCAManager();

    void setProxyCAManager(ProxyCAManager proxyCAManager);

    VolumeManager getVolumeManager();

    void setVolumeManager(VolumeManager volumeManager);

    long getTokenSequenceNo();

    void incrTokenSequenceNo();
}
