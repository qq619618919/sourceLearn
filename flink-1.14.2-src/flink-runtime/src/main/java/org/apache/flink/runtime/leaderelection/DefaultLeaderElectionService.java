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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation for leader election service. Composed with different {@link
 * LeaderElectionDriver}, we could perform a leader election for the contender, and then persist the
 * leader information to various storage.
 */
public class DefaultLeaderElectionService implements LeaderElectionService, LeaderElectionEventHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultLeaderElectionService.class);

    private final Object lock = new Object();

    private final LeaderElectionDriverFactory leaderElectionDriverFactory;

    /** The leader contender which applies for leadership. */
    private volatile LeaderContender leaderContender;

    @GuardedBy("lock")
    private volatile UUID issuedLeaderSessionID;

    @GuardedBy("lock")
    private volatile UUID confirmedLeaderSessionID;

    @GuardedBy("lock")
    private volatile String confirmedLeaderAddress;

    @GuardedBy("lock")
    private volatile boolean running;

    private LeaderElectionDriver leaderElectionDriver;

    public DefaultLeaderElectionService(LeaderElectionDriverFactory leaderElectionDriverFactory) {
        this.leaderElectionDriverFactory = checkNotNull(leaderElectionDriverFactory);

        leaderContender = null;

        issuedLeaderSessionID = null;
        confirmedLeaderSessionID = null;
        confirmedLeaderAddress = null;

        this.leaderElectionDriver = null;

        running = false;
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 该方法注意了，会被四个组件调用，就对应到四个不同的 LeaderContender 分别是：
     *  1、ResourceManager
     *  2、Disaptcher
     *  3、WebMonitorEndpoint
     *  4、JobMaster
     *  都是存在于主节点中的一些工作组件。
     *  -
     *  LeaderContender 选举者 = 参与选举的人
     *  111111111111111111111111111111111111111111111111
     */
    @Override
    public final void start(LeaderContender contender) throws Exception {

        checkNotNull(contender, "Contender must not be null.");
        Preconditions.checkState(leaderContender == null, "Contender was already set.");

        synchronized (lock) {

            // TODO_MA 马中华 注释： LeaderContender 有四种实现
            leaderContender = contender;

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： LeaderElectionDriver 是一个通用的用来做选举的代理
             *  具体实现是： ZooKeeperLeaderElectionDriver
             *  这个具体的选举的开启在： ZooKeeperLeaderElectionDriver 的 构造方法里面
             */
            leaderElectionDriver = leaderElectionDriverFactory.createLeaderElectionDriver(this,
                    new LeaderElectionFatalErrorHandler(),
                    leaderContender.getDescription()
            );
            LOG.info("Starting DefaultLeaderElectionService with {}.", leaderElectionDriver);

            running = true;
        }
    }

    @Override
    public final void stop() throws Exception {
        LOG.info("Stopping DefaultLeaderElectionService.");

        synchronized (lock) {
            if (!running) {
                return;
            }
            running = false;
            clearConfirmedLeaderInformation();
        }

        leaderElectionDriver.close();
    }

    @Override
    public void confirmLeadership(UUID leaderSessionID, String leaderAddress) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Confirm leader session ID {} for leader {}.", leaderSessionID, leaderAddress);
        }
        checkNotNull(leaderSessionID);

        synchronized (lock) {
            // TODO_MA 马中华 注释：
            if (hasLeadership(leaderSessionID)) {
                if (running) {
                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释：
                     */
                    confirmLeaderInformation(leaderSessionID, leaderAddress);
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Ignoring the leader session Id {} confirmation, since the "
                                + "LeaderElectionService has already been stopped.", leaderSessionID);
                    }
                }
            } else {
                // Received an old confirmation call
                if (!leaderSessionID.equals(this.issuedLeaderSessionID)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Receive an old confirmation call of leader session ID {}, "
                                + "current issued session ID is {}", leaderSessionID, issuedLeaderSessionID);
                    }
                } else {
                    LOG.warn("The leader session ID {} was confirmed even though the "
                            + "corresponding JobManager was not elected as the leader.", leaderSessionID);
                }
            }
        }
    }

    @Override
    public boolean hasLeadership(@Nonnull UUID leaderSessionId) {
        synchronized (lock) {
            if (running) {
                return leaderElectionDriver.hasLeadership() && leaderSessionId.equals(issuedLeaderSessionID);
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("hasLeadership is called after the service is stopped, returning false.");
                }
                return false;
            }
        }
    }

    /**
     * Returns the current leader session ID or null, if the contender is not the leader.
     *
     * @return The last leader session ID or null, if the contender is not the leader
     */
    @VisibleForTesting
    @Nullable
    public UUID getLeaderSessionID() {
        return confirmedLeaderSessionID;
    }

    @GuardedBy("lock")
    private void confirmLeaderInformation(UUID leaderSessionID, String leaderAddress) {
        confirmedLeaderSessionID = leaderSessionID;
        confirmedLeaderAddress = leaderAddress;
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        leaderElectionDriver.writeLeaderInformation(LeaderInformation.known(confirmedLeaderSessionID,
                confirmedLeaderAddress
        ));
    }

    @GuardedBy("lock")
    private void clearConfirmedLeaderInformation() {
        confirmedLeaderSessionID = null;
        confirmedLeaderAddress = null;
    }

    @Override
    @GuardedBy("lock")
    public void onGrantLeadership() {
        synchronized (lock) {
            if (running) {
                issuedLeaderSessionID = UUID.randomUUID();

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释：
                 */
                clearConfirmedLeaderInformation();

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Grant leadership to contender {} with session ID {}.",
                            leaderContender.getDescription(),
                            issuedLeaderSessionID
                    );
                }

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： leaderContender 有 4 种实现
                 *  1、这个 leaderContender 会调用 DefaultLeaderElectionService 的 start() 来启动选举
                 *  2、如果选举成功，则会回调 leaderContender 的 grantLeadership 这个方法
                 */
                leaderContender.grantLeadership(issuedLeaderSessionID);
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring the grant leadership notification since the {} has " + "already been closed.",
                            leaderElectionDriver
                    );
                }
            }
        }
    }

    @Override
    @GuardedBy("lock")
    public void onRevokeLeadership() {
        synchronized (lock) {
            if (running) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Revoke leadership of {} ({}@{}).",
                            leaderContender.getDescription(),
                            confirmedLeaderSessionID,
                            confirmedLeaderAddress
                    );
                }

                issuedLeaderSessionID = null;
                clearConfirmedLeaderInformation();

                leaderContender.revokeLeadership();

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Clearing the leader information on {}.", leaderElectionDriver);
                }
                // Clear the old leader information on the external storage
                leaderElectionDriver.writeLeaderInformation(LeaderInformation.empty());
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring the revoke leadership notification since the {} " + "has already been closed.",
                            leaderElectionDriver
                    );
                }
            }
        }
    }

    @Override
    @GuardedBy("lock")
    public void onLeaderInformationChange(LeaderInformation leaderInformation) {
        synchronized (lock) {
            if (running) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace(
                            "Leader node changed while {} is the leader with session ID {}. New leader information {}.",
                            leaderContender.getDescription(),
                            confirmedLeaderSessionID,
                            leaderInformation
                    );
                }
                if (confirmedLeaderSessionID != null) {
                    final LeaderInformation confirmedLeaderInfo = LeaderInformation.known(confirmedLeaderSessionID,
                            confirmedLeaderAddress
                    );
                    if (leaderInformation.isEmpty()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Writing leader information by {} since the external storage is empty.",
                                    leaderContender.getDescription()
                            );
                        }
                        leaderElectionDriver.writeLeaderInformation(confirmedLeaderInfo);
                    } else if (!leaderInformation.equals(confirmedLeaderInfo)) {
                        // the data field does not correspond to the expected leader information
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Correcting leader information by {}.", leaderContender.getDescription());
                        }
                        leaderElectionDriver.writeLeaderInformation(confirmedLeaderInfo);
                    }
                }
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring change notification since the {} has " + "already been closed.",
                            leaderElectionDriver
                    );
                }
            }
        }
    }

    private class LeaderElectionFatalErrorHandler implements FatalErrorHandler {

        @Override
        public void onFatalError(Throwable throwable) {
            synchronized (lock) {
                if (!running) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Ignoring error notification since the service has been stopped.");
                    }
                    return;
                }

                if (throwable instanceof LeaderElectionException) {
                    leaderContender.handleError((LeaderElectionException) throwable);
                } else {
                    leaderContender.handleError(new LeaderElectionException(throwable));
                }
            }
        }
    }
}
