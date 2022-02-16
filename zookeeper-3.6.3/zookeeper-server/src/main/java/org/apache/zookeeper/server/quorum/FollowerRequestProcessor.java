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

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ZooKeeperCriticalThread;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.txn.ErrorTxn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This RequestProcessor forwards any requests that modify the state of the
 * system to the Leader.
 */
public class FollowerRequestProcessor extends ZooKeeperCriticalThread implements RequestProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(FollowerRequestProcessor.class);

    FollowerZooKeeperServer zks;

    // TODO_MA 注释： 当前 RP 处理完之后，下一个 RP 接着处理
    RequestProcessor nextProcessor;

    // TODO_MA 注释： 队列
    LinkedBlockingQueue<Request> queuedRequests = new LinkedBlockingQueue<Request>();

    boolean finished = false;

    public FollowerRequestProcessor(FollowerZooKeeperServer zks, RequestProcessor nextProcessor) {
        super("FollowerRequestProcessor:" + zks.getServerId(), zks.getZooKeeperServerListener());
        this.zks = zks;
        this.nextProcessor = nextProcessor;
    }

    @Override
    public void run() {
        try {
            while(!finished) {

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 获取提交该 RP 的 Request
                 */
                Request request = queuedRequests.take();
                if(LOG.isTraceEnabled()) {
                    ZooTrace.logRequest(LOG, ZooTrace.CLIENT_REQUEST_TRACE_MASK, 'F', request, "");
                }
                if(request == Request.requestOfDeath) {
                    break;
                }

                // Screen quorum requests against ACLs first
                if(!zks.authWriteRequest(request)) {
                    continue;
                }

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释：
                 */
                // We want to queue the request to be processed before we submit
                // the request to the leader so that we are ready to receive the response
                nextProcessor.processRequest(request);

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 转发请求给 Leader
                 *  下述列表中的 请求类型，都是事务请求
                 */
                // We now ship the request to the leader. As with all
                // other quorum operations, sync also follows this code
                // path, but different from others, we need to keep track
                // of the sync operations this follower has pending, so we
                // add it to pendingSyncs.
                switch(request.type) {

                    // TODO_MA 注释： 如果是同步请求
                    case OpCode.sync:
                        zks.pendingSyncs.add(request);
                        zks.getFollower().request(request);
                        break;
                    case OpCode.create:
                    case OpCode.create2:
                    case OpCode.createTTL:
                    case OpCode.createContainer:
                    case OpCode.delete:
                    case OpCode.deleteContainer:
                    case OpCode.setData:
                    case OpCode.reconfig:
                    case OpCode.setACL:
                    case OpCode.multi:
                    case OpCode.check:
                        zks.getFollower().request(request);
                        break;
                    case OpCode.createSession:
                    case OpCode.closeSession:
                        // Don't forward local sessions to the leader.
                        if(!request.isLocalSession()) {
                            zks.getFollower().request(request);
                        }
                        break;
                }
            }
        } catch(Exception e) {
            handleException(this.getName(), e);
        }
        LOG.info("FollowerRequestProcessor exited loop!");
    }

    public void processRequest(Request request) {

        // TODO_MA 注释：
        processRequest(request, true);
    }

    void processRequest(Request request, boolean checkForUpgrade) {
        if(!finished) {

            // TODO_MA 注释： 处理 session 的事儿
            if(checkForUpgrade) {
                // Before sending the request, check if the request requires a
                // global session and what we have is a local session. If so do
                // an upgrade.
                Request upgradeRequest = null;
                try {
                    upgradeRequest = zks.checkUpgradeSession(request);
                } catch(KeeperException ke) {
                    if(request.getHdr() != null) {
                        request.getHdr().setType(OpCode.error);
                        request.setTxn(new ErrorTxn(ke.code().intValue()));
                    }
                    request.setException(ke);
                    LOG.warn("Error creating upgrade request", ke);
                } catch(IOException ie) {
                    LOG.error("Unexpected error in upgrade", ie);
                }
                if(upgradeRequest != null) {
                    queuedRequests.add(upgradeRequest);
                }
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            queuedRequests.add(request);
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        finished = true;
        queuedRequests.clear();
        queuedRequests.add(Request.requestOfDeath);
        nextProcessor.shutdown();
    }

}
