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

package org.apache.flink.runtime.leaderretrieval;

/**
 * This interface has to be implemented by a service which retrieves the current leader and notifies
 * a listener about it.
 *
 * <p>Prior to using this service it has to be started by calling the start method. The start method
 * also takes the {@link LeaderRetrievalListener} as an argument. The service can only be started
 * once.
 *
 * <p>The service should be stopped by calling the stop method.
 * // TODO_MA 马中华 注释： 观察者 设计模式
 */
public interface LeaderRetrievalService {

    /**
     * Starts the leader retrieval service with the given listener to listen for new leaders. This
     * method can only be called once.
     * // TODO_MA 马中华 注释： 启动监听
     *
     * @param listener The leader retrieval listener which will be notified about new leaders.
     * @throws Exception
     * // TODO_MA 马中华 注释： 如果通过这个 LeaderRetrievalService 的 start(listener) 启动了监听之后响应了。
     * // TODO_MA 马中华 注释： 则回调 listener 的notifyLeaderAddress 的方法
     */
    void start(LeaderRetrievalListener listener) throws Exception;

    /**
     * Stops the leader retrieval service.
     *
     * @throws Exception
     */
    void stop() throws Exception;
}
