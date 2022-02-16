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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

/**
 * This class implements a validator for majority quorums. The implementation is
 * straightforward.
 *
 */
public class QuorumMaj implements QuorumVerifier {

    // TODO_MA 注释： 里面包含的是 ZK 集群的所有节点
    private Map<Long, QuorumServer> allMembers = new HashMap<Long, QuorumServer>();

    // TODO_MA 注释： 具有选举权的节点 = 除了 observer 以外的所有节点
    private Map<Long, QuorumServer> votingMembers = new HashMap<Long, QuorumServer>();

    // TODO_MA 注释： observer 节点
    private Map<Long, QuorumServer> observingMembers = new HashMap<Long, QuorumServer>();
    private long version = 0;
    private int half;

    public int hashCode() {
        assert false : "hashCode not designed";
        return 42; // any arbitrary constant will do
    }

    public boolean equals(Object o) {
        if (!(o instanceof QuorumMaj)) {
            return false;
        }
        QuorumMaj qm = (QuorumMaj) o;
        if (qm.getVersion() == version) {
            return true;
        }
        if (allMembers.size() != qm.getAllMembers().size()) {
            return false;
        }
        for (QuorumServer qs : allMembers.values()) {
            QuorumServer qso = qm.getAllMembers().get(qs.id);
            if (qso == null || !qs.equals(qso)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Defines a majority to avoid computing it every time.
     *
     */
    public QuorumMaj(Map<Long, QuorumServer> allMembers) {
        this.allMembers = allMembers;
        for (QuorumServer qs : allMembers.values()) {
            if (qs.type == LearnerType.PARTICIPANT) {
                votingMembers.put(Long.valueOf(qs.id), qs);
            } else {
                observingMembers.put(Long.valueOf(qs.id), qs);
            }
        }

        // TODO_MA 马中华 注释： 一半
        // TODO_MA 马中华 注释： 4 = 2
        // TODO_MA 马中华 注释： 5 = 2
        half = votingMembers.size() / 2;
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 在解析 server 的相关配置
     */
    public QuorumMaj(Properties props) throws ConfigException {

        // TODO_MA 注释：
        for (Entry<Object, Object> entry : props.entrySet()) {

            String key = entry.getKey().toString();
            String value = entry.getValue().toString();

            // TODO_MA 注释： 解析 Server.myid = hostname:port:electionPort:peerType 的数据
            if (key.startsWith("server.")) {
                int dot = key.indexOf('.');
                long sid = Long.parseLong(key.substring(dot + 1));

                // TODO_MA 注释： 任何一台服务器都会被包装成一个 QuorumServer
                QuorumServer qs = new QuorumServer(sid, value);

                // TODO_MA 注释： 所有服务节点
                allMembers.put(Long.valueOf(sid), qs);

                // TODO_MA 注释： 有选举权和被选举权的节点
                if (qs.type == LearnerType.PARTICIPANT) {
                    votingMembers.put(Long.valueOf(sid), qs);
                }

                // TODO_MA 注释： observer节点
                else {
                    observingMembers.put(Long.valueOf(sid), qs);
                }
            } else if (key.equals("version")) {
                version = Long.parseLong(value, 16);
            }
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： votingMembers 中的成员就是  participant ，除了 observer 之外的
         *  votingMembers
         *  observermembers
         */
        half = votingMembers.size() / 2;
    }

    /**
     * Returns weight of 1 by default.
     *
     * @param id
     */
    public long getWeight(long id) {
        return 1;
    }

    public String toString() {
        StringBuilder sw = new StringBuilder();

        for (QuorumServer member : getAllMembers().values()) {
            String key = "server." + member.id;
            String value = member.toString();
            sw.append(key);
            sw.append('=');
            sw.append(value);
            sw.append('\n');
        }
        String hexVersion = Long.toHexString(version);
        sw.append("version=");
        sw.append(hexVersion);
        return sw.toString();
    }

    /**
     * // TODO_MA 注释： 少数服从多数，是否 ack 集合大于 votingMembers 集合的半数
     * Verifies if a set is a majority. Assumes that ackSet contains acks only from votingMembers
     * // TODO_MA 马中华 注释： 写数据
     * // TODO_MA 马中华 注释： 选举
     * // TODO_MA 马中华 注释： 选举确认
     */
    public boolean containsQuorum(Set<Long> ackSet) {

        // TODO_MA 马中华 注释： half 不会变！
        return (ackSet.size() > half);
    }

    public Map<Long, QuorumServer> getAllMembers() {
        return allMembers;
    }

    public Map<Long, QuorumServer> getVotingMembers() {
        return votingMembers;
    }

    public Map<Long, QuorumServer> getObservingMembers() {
        return observingMembers;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long ver) {
        version = ver;
    }

}
