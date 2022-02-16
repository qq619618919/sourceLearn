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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.util.AbstractLivelinessMonitor;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释：
 */
public class NMLivelinessMonitor extends AbstractLivelinessMonitor<NodeId> {

    private EventHandler<Event> dispatcher;

    public NMLivelinessMonitor(Dispatcher d) {
        super("NMLivelinessMonitor");
        this.dispatcher = d.getEventHandler();
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 验活机制
     *  1、其实就是设计一个 for 循环，每隔一段时间，针对注册成功的 NodeManager 执行心跳超时判断
     *  2、如果发现某个 nodemanager 的上一次心跳时间 距离 现在超过了 规定的超时时间，则认为超时了，进行超时处理
     *  3、超时处理，就是调用：expire() 方法
     *  -
     *  步骤1 和 步骤2 这两件事，其实是通用的，抽象出来这个工作机制，放在父类中做实现，
     *  具体的 超时处理逻辑，有 expire() 方法去完成， 具体实现逻辑就交给 子类
     */
    public void serviceInit(Configuration conf) throws Exception {
        // TODO_MA 马中华 注释： yarn.nm.liveness-monitor.expiry-interval-ms = 600000 = 10min
        int expireIntvl = conf.getInt(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS,
                YarnConfiguration.DEFAULT_RM_NM_EXPIRY_INTERVAL_MS
        );
        // TODO_MA 马中华 注释： 10min
        setExpireInterval(expireIntvl);
        // TODO_MA 马中华 注释： 每多久判断一次： 200s
        setMonitorInterval(expireIntvl / 3);
        super.serviceInit(conf);
    }

    @Override
    protected void expire(NodeId id) {
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 提交一个事件！
         *  方式1： 构建一个关键词：RMNodeEvent.class
         *  找到一个类似的代码： xxx.register(RMNodeEvent.class, XXEventHandler)
         *  -
         *  方式2：搜索具体的事件，关键词： RMNodeEventType.EXPIRE
         *  .addTransition(NodeState.RUNNING, NodeState.LOST, RMNodeEventType.EXPIRE,
         *                     new DeactivateNodeTransition(NodeState.LOST)
         *       去看 DeactivateNodeTransition 的 transition()
         */
        dispatcher.handle(new RMNodeEvent(id, RMNodeEventType.EXPIRE));
        // TODO_MA 马中华 注释： 这个事件最终是由： RMNode 这个状态机处理
    }
}