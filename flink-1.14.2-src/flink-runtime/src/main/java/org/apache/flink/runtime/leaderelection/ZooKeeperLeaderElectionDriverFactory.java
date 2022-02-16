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

import org.apache.flink.runtime.rpc.FatalErrorHandler;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;

/** {@link LeaderElectionDriverFactory} implementation for Zookeeper. */
public class ZooKeeperLeaderElectionDriverFactory implements LeaderElectionDriverFactory {

    // TODO_MA 马中华 注释： 客户端
    private final CuratorFramework client;

    // TODO_MA 马中华 注释： /rest_server
    private final String path;

    public ZooKeeperLeaderElectionDriverFactory(CuratorFramework client, String path) {
        this.client = client;
        this.path = path;
    }

    @Override
    public ZooKeeperLeaderElectionDriver createLeaderElectionDriver(LeaderElectionEventHandler leaderEventHandler,
                                                                    FatalErrorHandler fatalErrorHandler,
                                                                    String leaderContenderDescription) throws Exception {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： ZooKeeperLeaderElectionDriver 专门帮助来完成选举的
         */
        return new ZooKeeperLeaderElectionDriver(client,
                path,
                leaderEventHandler,
                fatalErrorHandler,
                leaderContenderDescription
        );
    }
}