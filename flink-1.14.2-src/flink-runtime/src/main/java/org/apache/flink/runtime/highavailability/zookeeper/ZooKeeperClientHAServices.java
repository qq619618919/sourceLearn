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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.highavailability.ClientHighAvailabilityServices;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.util.ZooKeeperUtils;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;

import javax.annotation.Nonnull;

/** ZooKeeper based implementation for {@link ClientHighAvailabilityServices}. */
public class ZooKeeperClientHAServices implements ClientHighAvailabilityServices {

    private final CuratorFramework client;

    private final Configuration configuration;

    public ZooKeeperClientHAServices(@Nonnull CuratorFramework client, @Nonnull Configuration configuration) {
        this.client = client;
        this.configuration = configuration;
    }

    @Override
    public LeaderRetrievalService getClusterRestEndpointLeaderRetriever() {
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： DefaultLeaderRetrievalService
         */
        return ZooKeeperUtils.createLeaderRetrievalService(client,
                ZooKeeperUtils.getLeaderPathForRestServer(),
                configuration
        );
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}
