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

package org.apache.zookeeper.server.quorum.flexible;

import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;

/**
 * All quorum validators have to implement a method called
 * containsQuorum, which verifies if a HashSet of server
 * identifiers constitutes a quorum.
 */

public interface QuorumVerifier {

    long getWeight(long id);

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 方法的意义：检查是否超过了半数
     *  1、Set<Long> set 已经收到的投票集合 a
     *  2、getVotingMembers() 方法的结果  b
     *  a / b > 1/2
     */
    boolean containsQuorum(Set<Long> set);

    long getVersion();

    void setVersion(long ver);

    // TODO_MA 注释： 所有的节点
    Map<Long, QuorumServer> getAllMembers();

    // TODO_MA 注释： 除了 Observer 之外的所有节点
    Map<Long, QuorumServer> getVotingMembers();

    // TODO_MA 注释： Observer 节点
    Map<Long, QuorumServer> getObservingMembers();

    boolean equals(Object o);

    String toString();

}
