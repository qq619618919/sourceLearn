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
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.quorum.QuorumCnxManager.Message;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of leader election using TCP. It uses an object of the class
 * QuorumCnxManager to manage connections. Otherwise, the algorithm is push-based
 * as with the other UDP implementations.
 *
 * There are a few parameters that can be tuned to change its behavior. First,
 * finalizeWait determines the amount of time to wait until deciding upon a leader.
 * This is part of the leader election algorithm.
 */

public class FastLeaderElection implements Election {

    private static final Logger LOG = LoggerFactory.getLogger(FastLeaderElection.class);

    /**
     * Determine how much time a process has to wait
     * once it believes that it has reached the end of
     * leader election.
     */
    static final int finalizeWait = 200;

    /**
     * Upper bound on the amount of time between two consecutive
     * notification checks. This impacts the amount of time to get
     * the system up again after long partitions. Currently 60 seconds.
     */

    private static int maxNotificationInterval = 60000;

    /**
     * Lower bound for notification check. The observer don't need to use
     * the same lower bound as participant members
     */
    private static int minNotificationInterval = finalizeWait;

    /**
     * Minimum notification interval, default is equal to finalizeWait
     */
    public static final String MIN_NOTIFICATION_INTERVAL = "zookeeper.fastleader.minNotificationInterval";

    /**
     * Maximum notification interval, default is 60s
     */
    public static final String MAX_NOTIFICATION_INTERVAL = "zookeeper.fastleader.maxNotificationInterval";

    static {
        minNotificationInterval = Integer.getInteger(MIN_NOTIFICATION_INTERVAL, minNotificationInterval);
        LOG.info("{}={}", MIN_NOTIFICATION_INTERVAL, minNotificationInterval);
        maxNotificationInterval = Integer.getInteger(MAX_NOTIFICATION_INTERVAL, maxNotificationInterval);
        LOG.info("{}={}", MAX_NOTIFICATION_INTERVAL, maxNotificationInterval);
    }

    /**
     * Connection manager. Fast leader election uses TCP for
     * communication between peers, and QuorumCnxManager manages
     * such connections.
     */

    QuorumCnxManager manager;

    private SyncedLearnerTracker leadingVoteSet;

    /**
     * Notifications are messages that let other peers know that
     * a given peer has changed its vote, either because it has
     * joined leader election or because it learned of another
     * peer with higher zxid or same zxid and higher server id
     */

    public static class Notification {
        /*
         * Format version, introduced in 3.4.6
         */

        public static final int CURRENTVERSION = 0x2;
        int version;

        /*
         * Proposed leader
         */ long leader;

        /*
         * zxid of the proposed leader
         */ long zxid;

        /*
         * Epoch
         */ long electionEpoch;

        /*
         * current state of sender
         */ QuorumPeer.ServerState state;

        /*
         * Address of sender
         */ long sid;

        QuorumVerifier qv;
        /*
         * epoch of the proposed leader
         */ long peerEpoch;

    }

    static byte[] dummyData = new byte[0];

    /**
     * Messages that a peer wants to send to other peers.
     * These messages can be both Notifications and Acks
     * of reception of notification.
     */
    public static class ToSend {

        enum mType {
            crequest, challenge, notification, ack
        }

        ToSend(mType type, long leader, long zxid, long electionEpoch, ServerState state, long sid, long peerEpoch,
                byte[] configData) {

            this.leader = leader;
            this.zxid = zxid;
            this.electionEpoch = electionEpoch;
            this.state = state;
            this.sid = sid;
            this.peerEpoch = peerEpoch;
            this.configData = configData;
        }

        /*
         * Proposed leader in the case of notification
         */ long leader;

        /*
         * id contains the tag for acks, and zxid for notifications
         */ long zxid;

        /*
         * Epoch
         */ long electionEpoch;

        /*
         * Current state;
         */ QuorumPeer.ServerState state;

        /*
         * Address of recipient
         */ long sid;

        /*
         * Used to send a QuorumVerifier (configuration info)
         */ byte[] configData = dummyData;

        /*
         * Leader epoch
         */ long peerEpoch;

    }

    LinkedBlockingQueue<ToSend> sendqueue;
    LinkedBlockingQueue<Notification> recvqueue;

    /**
     * Multi-threaded implementation of message handler. Messenger
     * implements two sub-classes: WorkReceiver and  WorkSender. The
     * functionality of each is obvious from the name. Each of these
     * spawns a new thread.
     */

    protected class Messenger {

        /**
         * Receives messages from instance of QuorumCnxManager on
         * method run(), and processes such messages.
         */

        class WorkerReceiver extends ZooKeeperThread {

            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerReceiver(QuorumCnxManager manager) {
                super("WorkerReceiver");
                this.stop = false;
                this.manager = manager;
            }

            public void run() {

                Message response;
                while(!stop) {
                    // Sleeps on receive
                    try {

                        /*************************************************
                         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                         *  注释： 从 recevQueue 中获取到一张投票(Message)
                         */
                        response = manager.pollRecvQueue(3000, TimeUnit.MILLISECONDS);
                        if(response == null) {
                            continue;
                        }

                        final int capacity = response.buffer.capacity();

                        // The current protocol and two previous generations all send at least 28 bytes
                        if(capacity < 28) {
                            LOG.error("Got a short response from server {}: {}", response.sid, capacity);
                            continue;
                        }

                        // this is the backwardCompatibility mode in place before ZK-107
                        // It is for a version of the protocol in which we didn't send peer epoch
                        // With peer epoch and version the message became 40 bytes
                        boolean backCompatibility28 = (capacity == 28);

                        // this is the backwardCompatibility mode for no version information
                        boolean backCompatibility40 = (capacity == 40);

                        response.buffer.clear();

                        /*************************************************
                         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                         *  注释： 构建了一个空的 Notification
                         */
                        // Instantiate Notification and set its attributes
                        Notification n = new Notification();

                        int rstate = response.buffer.getInt();
                        long rleader = response.buffer.getLong();
                        long rzxid = response.buffer.getLong();
                        long relectionEpoch = response.buffer.getLong();
                        long rpeerepoch;

                        int version = 0x0;
                        QuorumVerifier rqv = null;

                        try {
                            if(!backCompatibility28) {
                                rpeerepoch = response.buffer.getLong();
                                if(!backCompatibility40) {
                                    /*
                                     * Version added in 3.4.6
                                     */

                                    version = response.buffer.getInt();
                                } else {
                                    LOG.info("Backward compatibility mode (36 bits), server id: {}", response.sid);
                                }
                            } else {
                                LOG.info("Backward compatibility mode (28 bits), server id: {}", response.sid);
                                rpeerepoch = ZxidUtils.getEpochFromZxid(rzxid);
                            }

                            // check if we have a version that includes config. If so extract config info from message.
                            if(version > 0x1) {
                                int configLength = response.buffer.getInt();

                                // we want to avoid errors caused by the allocation of a byte array with negative length
                                // (causing NegativeArraySizeException) or huge length (causing e.g. OutOfMemoryError)
                                if(configLength < 0 || configLength > capacity) {
                                    throw new IOException(String.format(
                                            "Invalid configLength in notification message! sid=%d, capacity=%d, version=%d, configLength=%d",
                                            response.sid, capacity, version, configLength));
                                }

                                byte[] b = new byte[configLength];
                                response.buffer.get(b);

                                synchronized(self) {
                                    try {
                                        rqv = self.configFromString(new String(b));
                                        QuorumVerifier curQV = self.getQuorumVerifier();
                                        if(rqv.getVersion() > curQV.getVersion()) {
                                            LOG.info("{} Received version: {} my version: {}", self.getId(),
                                                    Long.toHexString(rqv.getVersion()),
                                                    Long.toHexString(self.getQuorumVerifier().getVersion()));
                                            if(self.getPeerState() == ServerState.LOOKING) {
                                                LOG.debug("Invoking processReconfig(), state: {}", self.getServerState());
                                                self.processReconfig(rqv, null, null, false);
                                                if(!rqv.equals(curQV)) {
                                                    LOG.info("restarting leader election");
                                                    self.shuttingDownLE = true;
                                                    self.getElectionAlg().shutdown();

                                                    break;
                                                }
                                            } else {
                                                LOG.debug("Skip processReconfig(), state: {}", self.getServerState());
                                            }
                                        }
                                    } catch(IOException | ConfigException e) {
                                        LOG.error("Something went wrong while processing config received from {}",
                                                response.sid);
                                    }
                                }
                            } else {
                                LOG.info("Backward compatibility mode (before reconfig), server id: {}", response.sid);
                            }
                        } catch(BufferUnderflowException | IOException e) {
                            LOG.warn(
                                    "Skipping the processing of a partial / malformed response message sent by sid={} (message length: {})",
                                    response.sid, capacity, e);
                            continue;
                        }
                        /*
                         * If it is from a non-voting server (such as an observer or
                         * a non-voting follower), respond right away.
                         */
                        if(!validVoter(response.sid)) {
                            Vote current = self.getCurrentVote();
                            QuorumVerifier qv = self.getQuorumVerifier();
                            ToSend notmsg = new ToSend(ToSend.mType.notification, current.getId(), current.getZxid(),
                                    logicalclock.get(), self.getPeerState(), response.sid, current.getPeerEpoch(),
                                    qv.toString().getBytes());

                            sendqueue.offer(notmsg);
                        } else {
                            // Receive new message
                            LOG.debug("Receive new notification message. My id = {}", self.getId());

                            // State of peer that sent this message
                            QuorumPeer.ServerState ackstate = QuorumPeer.ServerState.LOOKING;
                            switch(rstate) {
                                case 0:
                                    ackstate = QuorumPeer.ServerState.LOOKING;
                                    break;
                                case 1:
                                    ackstate = QuorumPeer.ServerState.FOLLOWING;
                                    break;
                                case 2:
                                    ackstate = QuorumPeer.ServerState.LEADING;
                                    break;
                                case 3:
                                    ackstate = QuorumPeer.ServerState.OBSERVING;
                                    break;
                                default:
                                    continue;
                            }

                            // TODO_MA 注释： 从 message 中拿到 选票的各种，经过校验之后，全部设置到 Notification 中
                            n.leader = rleader;
                            n.zxid = rzxid;
                            n.electionEpoch = relectionEpoch;
                            n.state = ackstate;
                            n.sid = response.sid;
                            n.peerEpoch = rpeerepoch;
                            n.version = version;
                            n.qv = rqv;
                            /*
                             * Print notification info
                             */
                            LOG.info(
                                    "Notification: my state:{}; n.sid:{}, n.state:{}, n.leader:{}, n.round:0x{}, " + "n.peerEpoch:0x{}, n.zxid:0x{}, message format version:0x{}, n.config version:0x{}",
                                    self.getPeerState(), n.sid, n.state, n.leader, Long.toHexString(n.electionEpoch),
                                    Long.toHexString(n.peerEpoch), Long.toHexString(n.zxid), Long.toHexString(n.version),
                                    (n.qv != null ? (Long.toHexString(n.qv.getVersion())) : "0"));

                            /*
                             * If this server is looking, then send proposed leader
                             */

                            /*************************************************
                             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                             *  注释： 如果对方服务器也是 looking 状态，证明对方也在执行选举
                             *  此时，将构建好的 Notification 加入到 recvqueue 队列中
                             */
                            if(self.getPeerState() == QuorumPeer.ServerState.LOOKING) {

                                /*************************************************
                                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                                 *  注释： 到此为止，可以这么理解：
                                 *  一台服务器终于把一张票发送给了另外一台服务器
                                 */
                                recvqueue.offer(n);

                                /*
                                 * Send a notification back if the peer that sent this
                                 * message is also looking and its logical clock is
                                 * lagging behind.
                                 */
                                if((ackstate == QuorumPeer.ServerState.LOOKING) && (n.electionEpoch < logicalclock
                                        .get())) {
                                    Vote v = getVote();
                                    QuorumVerifier qv = self.getQuorumVerifier();
                                    ToSend notmsg = new ToSend(ToSend.mType.notification, v.getId(), v.getZxid(),
                                            logicalclock.get(), self.getPeerState(), response.sid, v.getPeerEpoch(),
                                            qv.toString().getBytes());
                                    sendqueue.offer(notmsg);
                                }
                            } else {
                                /*
                                 * If this server is not looking, but the one that sent the ack
                                 * is looking, then send back what it believes to be the leader.
                                 */
                                Vote current = self.getCurrentVote();
                                if(ackstate == QuorumPeer.ServerState.LOOKING) {
                                    if(self.leader != null) {
                                        if(leadingVoteSet != null) {
                                            self.leader.setLeadingVoteSet(leadingVoteSet);
                                            leadingVoteSet = null;
                                        }
                                        self.leader.reportLookingSid(response.sid);
                                    }


                                    LOG.debug(
                                            "Sending new notification. My id ={} recipient={} zxid=0x{} leader={} config version = {}",
                                            self.getId(), response.sid, Long.toHexString(current.getZxid()),
                                            current.getId(), Long.toHexString(self.getQuorumVerifier().getVersion()));

                                    QuorumVerifier qv = self.getQuorumVerifier();
                                    ToSend notmsg = new ToSend(ToSend.mType.notification, current.getId(),
                                            current.getZxid(), current.getElectionEpoch(), self.getPeerState(),
                                            response.sid, current.getPeerEpoch(), qv.toString().getBytes());
                                    sendqueue.offer(notmsg);
                                }
                            }
                        }
                    } catch(InterruptedException e) {
                        LOG.warn("Interrupted Exception while waiting for new message", e);
                    }
                }
                LOG.info("WorkerReceiver is down");
            }

        }

        /**
         * This worker simply dequeues a message to send and
         * and queues it on the manager's queue.
         */

        class WorkerSender extends ZooKeeperThread {

            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerSender(QuorumCnxManager manager) {
                super("WorkerSender");
                this.stop = false;
                this.manager = manager;
            }

            public void run() {
                while(!stop) {
                    try {

                        /*************************************************
                         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                         *  注释： 获取选票 ToSend 对象
                         */
                        ToSend m = sendqueue.poll(3000, TimeUnit.MILLISECONDS);
                        if(m == null) {
                            continue;
                        }

                        /*************************************************
                         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                         *  注释： 发送选票给所有服务器节点
                         */
                        process(m);
                    } catch(InterruptedException e) {
                        break;
                    }
                }
                LOG.info("WorkerSender is down");
            }

            /**
             * Called by run() once there is a new message to send.
             *
             * @param m message to send
             */
            void process(ToSend m) {

                // TODO_MA 注释： 将 ToSend 构建成 ByteBuffer
                ByteBuffer requestBuffer = buildMsg(m.state.ordinal(), m.leader, m.zxid, m.electionEpoch, m.peerEpoch,
                        m.configData);

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 执行 ByteBuffer 的发送
                 *  m.sid = 对方服务器的 myid
                 */
                manager.toSend(m.sid, requestBuffer);
            }

        }

        WorkerSender ws;
        WorkerReceiver wr;
        Thread wsThread = null;
        Thread wrThread = null;

        /**
         * Constructor of class Messenger.
         *
         * @param manager Connection manager
         */
        Messenger(QuorumCnxManager manager) {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            this.ws = new WorkerSender(manager);
            this.wsThread = new Thread(this.ws, "WorkerSender[myid=" + self.getId() + "]");
            this.wsThread.setDaemon(true);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            this.wr = new WorkerReceiver(manager);
            this.wrThread = new Thread(this.wr, "WorkerReceiver[myid=" + self.getId() + "]");
            this.wrThread.setDaemon(true);
        }

        /**
         * Starts instances of WorkerSender and WorkerReceiver
         */
        void start() {

            // TODO_MA 注释：
            this.wsThread.start();

            // TODO_MA 注释：
            this.wrThread.start();
        }

        /**
         * Stops instances of WorkerSender and WorkerReceiver
         */
        void halt() {
            this.ws.stop = true;
            this.wr.stop = true;
        }

    }

    QuorumPeer self;
    Messenger messenger;

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 逻辑时钟 = 选举轮次！
     *  每个节点，在执行第一轮选举的时候，logicalclock 这个值都是 1
     *  -
     *  epoch 靠齐！ epoch 叫做 任期（康熙 乾隆 ）
     *  上一个 leader 死掉了，下一个leader 被选举出来，则这个 epoch +1
     *  但凡发现，我们已经进行到了 logicalclock = 6， 则 logicalclock = 5 的所有票都作废！
     *  选票PK ： epoch + zxid + myid
     */
    AtomicLong logicalclock = new AtomicLong(); /* Election instance */

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 当前服务器节点，推举谁成为 leader ，
     *  这三个信息，就是这个 leader 的信息
     */
    long proposedLeader;    // TODO_MA 注释： myid
    long proposedZxid;      // TODO_MA 注释： zxid
    long proposedEpoch;     // TODO_MA 注释： 逻辑时钟 = 选举伦次

    // TODO_MA 注释： 每次从对方接受到一张票的时候，就会执行一次 选票的PK
    // TODO_MA 注释： 如果对方的票更优，则把对方票的信息，存储到这三个成员变量
    // TODO_MA 注释： 直到选举结束了之后，这三个成员变量，都会被存储到 Vote 中

    /**
     * Returns the current vlue of the logical clock counter
     */
    public long getLogicalClock() {
        return logicalclock.get();
    }

    static ByteBuffer buildMsg(int state, long leader, long zxid, long electionEpoch, long epoch) {
        byte[] requestBytes = new byte[40];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        /*
         * Building notification packet to send, this is called directly only in tests
         */

        requestBuffer.clear();
        requestBuffer.putInt(state);
        requestBuffer.putLong(leader);
        requestBuffer.putLong(zxid);
        requestBuffer.putLong(electionEpoch);
        requestBuffer.putLong(epoch);
        requestBuffer.putInt(0x1);

        return requestBuffer;
    }

    static ByteBuffer buildMsg(int state, long leader, long zxid, long electionEpoch, long epoch, byte[] configData) {
        byte[] requestBytes = new byte[44 + configData.length];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        /*
         * Building notification packet to send
         */

        requestBuffer.clear();
        requestBuffer.putInt(state);
        requestBuffer.putLong(leader);
        requestBuffer.putLong(zxid);
        requestBuffer.putLong(electionEpoch);
        requestBuffer.putLong(epoch);
        requestBuffer.putInt(Notification.CURRENTVERSION);
        requestBuffer.putInt(configData.length);
        requestBuffer.put(configData);

        return requestBuffer;
    }

    /**
     * Constructor of FastLeaderElection. It takes two parameters, one
     * is the QuorumPeer object that instantiated this object, and the other
     * is the connection manager. Such an object should be created only once
     * by each peer during an instance of the ZooKeeper service.
     *
     * @param self    QuorumPeer that created this object
     * @param manager Connection manager
     */
    public FastLeaderElection(QuorumPeer self, QuorumCnxManager manager) {
        this.stop = false;
        this.manager = manager;

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        starter(self, manager);
    }

    /**
     * This method is invoked by the constructor. Because it is a
     * part of the starting procedure of the object that must be on
     * any constructor of this class, it is probably best to keep as
     * a separate method. As we have a single constructor currently,
     * it is not strictly necessary to have it separate.
     *
     * @param self    QuorumPeer that created this object
     * @param manager Connection manager
     */
    private void starter(QuorumPeer self, QuorumCnxManager manager) {
        this.self = self;
        proposedLeader = -1;
        proposedZxid = -1;

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        sendqueue = new LinkedBlockingQueue<ToSend>();
        recvqueue = new LinkedBlockingQueue<Notification>();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        this.messenger = new Messenger(manager);
    }

    /**
     * This method starts the sender and receiver threads.
     */
    public void start() {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        this.messenger.start();
    }

    private void leaveInstance(Vote v) {
        LOG.debug("About to leave FLE instance: leader={}, zxid=0x{}, my id={}, my state={}", v.getId(),
                Long.toHexString(v.getZxid()), self.getId(), self.getPeerState());
        recvqueue.clear();
    }

    public QuorumCnxManager getCnxManager() {
        return manager;
    }

    volatile boolean stop;

    public void shutdown() {
        stop = true;
        proposedLeader = -1;
        proposedZxid = -1;
        leadingVoteSet = null;
        LOG.debug("Shutting down connection manager");
        manager.halt();
        LOG.debug("Shutting down messenger");
        messenger.halt();
        LOG.debug("FLE is down");
    }

    /**
     * Send notifications to all peers upon a change in our vote
     */
    private void sendNotifications() {

        // TODO_MA 注释： 获取定胜算法： QuorumMaj
        // TODO_MA 注释： 遍历每一台服务器，给每一台服务器发送选票
        for(long sid : self.getCurrentAndNextConfigVoters()) {
            QuorumVerifier qv = self.getQuorumVerifier();

            // TODO_MA 注释： 构建待发送的选票对象
            ToSend notmsg = new ToSend(ToSend.mType.notification, proposedLeader, proposedZxid, logicalclock.get(),
                    QuorumPeer.ServerState.LOOKING, sid, proposedEpoch, qv.toString().getBytes());

            LOG.debug(
                    "Sending Notification: {} (n.leader), 0x{} (n.zxid), 0x{} (n.round), {} (recipient)," + " {} (myid), 0x{} (n.peerEpoch) ",
                    proposedLeader, Long.toHexString(proposedZxid), Long.toHexString(logicalclock.get()), sid,
                    self.getId(), Long.toHexString(proposedEpoch));

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 将选票加入 发送队列： sendqueue
             *  WorkerSender 线程消费该队列
             */
            sendqueue.offer(notmsg);
        }
    }

    /**
     * Check if a pair (server id, zxid) succeeds our
     * current vote.
     */
    protected boolean totalOrderPredicate(long newId, long newZxid, long newEpoch, long curId, long curZxid,
            long curEpoch) {
        LOG.debug("id: {}, proposed id: {}, zxid: 0x{}, proposed zxid: 0x{}", newId, curId, Long.toHexString(newZxid),
                Long.toHexString(curZxid));

        if(self.getQuorumVerifier().getWeight(newId) == 0) {
            return false;
        }

        /*
         * We return true if one of the following three cases hold:
         * 1- New epoch is higher
         * 2- New epoch is the same as current epoch, but new zxid is higher
         * 3- New epoch is the same as current epoch, new zxid is the same
         *  as current zxid, but server id is higher.
         */

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 三个条件
         *  1、逻辑时钟 epoch logicallock
         *  2、zxid
         *  3、myid
         */
        return ((newEpoch > curEpoch) ||
                ((newEpoch == curEpoch) && ((newZxid > curZxid) ||
                        ((newZxid == curZxid) && (newId > curId)))));
    }

    /**
     * // TODO_MA 注释： 给定一组投票，返回 SyncedLearnerTracker，用于确定是否足以宣布选举回合结束。
     * Given a set of votes, return the SyncedLearnerTracker which is used to
     * determines if have sufficient to declare the end of the election round.
     *
     * @param votes Set of votes
     * @param vote  Identifier of the vote received last
     * @return the SyncedLearnerTracker with vote details
     */
    protected SyncedLearnerTracker getVoteTracker(Map<Long, Vote> votes, Vote vote) {

        SyncedLearnerTracker voteSet = new SyncedLearnerTracker();

        voteSet.addQuorumVerifier(self.getQuorumVerifier());

        if(self.getLastSeenQuorumVerifier() != null && self.getLastSeenQuorumVerifier().getVersion() > self
                .getQuorumVerifier().getVersion()) {
            voteSet.addQuorumVerifier(self.getLastSeenQuorumVerifier());
        }

        /*
         * First make the views consistent. Sometimes peers will have different
         * zxids for a server depending on timing.
         */
        for(Map.Entry<Long, Vote> entry : votes.entrySet()) {
            if(vote.equals(entry.getValue())) {
                voteSet.addAck(entry.getKey());
            }
        }

        return voteSet;
    }

    /**
     * In the case there is a leader elected, and a quorum supporting
     * this leader, we have to check if the leader has voted and acked
     * that it is leading. We need this check to avoid that peers keep
     * electing over and over a peer that has crashed and it is no
     * longer leading.
     *
     * @param votes         set of votes
     * @param leader        leader id
     * @param electionEpoch epoch id
     */
    protected boolean checkLeader(Map<Long, Vote> votes, long leader, long electionEpoch) {

        boolean predicate = true;

        /*
         * If everyone else thinks I'm the leader, I must be the leader.
         * The other two checks are just for the case in which I'm not the
         * leader. If I'm not the leader and I haven't received a message
         * from leader stating that it is leading, then predicate is false.
         */

        if(leader != self.getId()) {
            if(votes.get(leader) == null) {
                predicate = false;
            } else if(votes.get(leader).getState() != ServerState.LEADING) {
                predicate = false;
            }
        } else if(logicalclock.get() != electionEpoch) {
            predicate = false;
        }

        return predicate;
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 更新推举的 leader 的信息
     *  这三个参数： long leader, long zxid, long epoch 都是自己的信息
     */
    synchronized void updateProposal(long leader, long zxid, long epoch) {
        LOG.debug("Updating proposal: {} (newleader), 0x{} (newzxid), {} (oldleader), 0x{} (oldzxid)", leader,
                Long.toHexString(zxid), proposedLeader, Long.toHexString(proposedZxid));

        // TODO_MA 注释： 这三个成员变量保存的就是当前节点推举的 Leader 节点的信息
        // TODO_MA 注释： 当执行第一轮选举的时候，必然是每个节点都是推举自己
        // TODO_MA 注释： 所以在这种情况下，这三个成员变量保存的都是自己的信息
        proposedLeader = leader;
        proposedZxid = zxid;
        proposedEpoch = epoch;

        // TODO_MA 注释： 当第一轮选举不出来 leader 的时候，在后续的轮次中我们会执行选票的更新
        // TODO_MA 注释： 拿 我推举的 leader 的信息， 和 对方节点推举的 leader 的信息做对比
        // TODO_MA 注释： 这个逻辑： 1、epoch   2、zxid    3、myid
        // TODO_MA 注释： 先比较 epoch ， 获胜的是 epoch 大的 leader 推举信息
        // TODO_MA 注释： 再比较 zxid ， 获胜的是 zxid 大的
        // TODO_MA 注释： 当 zxid 一样的时候，则比较 myid，由于每个 server 节点的 myid 一定不一样，
        // TODO_MA 注释： 所以这个比较规则一定能比较出来更适合当 leader 的节点

        // TODO_MA 注释： zxid 是包含两个信息： epoch + txid
        // TODO_MA 注释： epoch = 任期 = 44 45 46 , 康熙 乾隆 雍正 光绪

        // TODO_MA 注释： 如果如果涉及到选票更新：
        // TODO_MA 注释： ServerA （100,25,2）  ServerB （100,24,3）==> （100,25,2）
    }

    public synchronized Vote getVote() {
        return new Vote(proposedLeader, proposedZxid, proposedEpoch);
    }

    /**
     * A learning state can be either FOLLOWING or OBSERVING.
     * This method simply decides which one depending on the
     * role of the server.
     *
     * @return ServerState
     */
    private ServerState learningState() {
        if(self.getLearnerType() == LearnerType.PARTICIPANT) {
            LOG.debug("I am a participant: {}", self.getId());
            return ServerState.FOLLOWING;
        } else {
            LOG.debug("I am an observer: {}", self.getId());
            return ServerState.OBSERVING;
        }
    }

    /**
     * Returns the initial vote value of server identifier.
     *
     * @return long
     */
    private long getInitId() {
        // TODO_MA 注释： VotingMembers 有选举权的服务器节点的集合
        if(self.getQuorumVerifier().getVotingMembers().containsKey(self.getId())) {
            return self.getId();
        } else {
            return Long.MIN_VALUE;
        }
    }

    /**
     * Returns initial last logged zxid.
     *
     * @return long
     */
    private long getInitLastLoggedZxid() {
        if(self.getLearnerType() == LearnerType.PARTICIPANT) {
            return self.getLastLoggedZxid();
        } else {
            return Long.MIN_VALUE;
        }
    }

    /**
     * Returns the initial vote value of the peer epoch.
     *
     * @return long
     */
    private long getPeerEpoch() {
        if(self.getLearnerType() == LearnerType.PARTICIPANT) {
            try {
                return self.getCurrentEpoch();
            } catch(IOException e) {
                RuntimeException re = new RuntimeException(e.getMessage());
                re.setStackTrace(e.getStackTrace());
                throw re;
            }
        } else {
            return Long.MIN_VALUE;
        }
    }

    /**
     * Update the peer state based on the given proposedLeader. Also update
     * the leadingVoteSet if it becomes the leader.
     */
    private void setPeerState(long proposedLeader, SyncedLearnerTracker voteSet) {

        // TODO_MA 注释： 根据选举结果来更新状态
        ServerState ss = (proposedLeader == self.getId()) ? ServerState.LEADING : learningState();

        // TODO_MA 注释： 更新 QuorumPeer 的状态
        self.setPeerState(ss);
        if(ss == ServerState.LEADING) {
            leadingVoteSet = voteSet;
        }
    }

    /**
     * Starts a new round of leader election. Whenever our QuorumPeer
     * changes its state to LOOKING, this method is invoked, and it
     * sends notifications to all other peers.
     */
    public Vote lookForLeader() throws InterruptedException {
        try {
            self.jmxLeaderElectionBean = new LeaderElectionBean();
            MBeanRegistry.getInstance().register(self.jmxLeaderElectionBean, self.jmxLocalPeerBean);
        } catch(Exception e) {
            LOG.warn("Failed to register with JMX", e);
            self.jmxLeaderElectionBean = null;
        }

        // TODO_MA 注释： 选举开始时间
        self.start_fle = Time.currentElapsedTime();
        try {
            /*
             * The votes from the current leader election are stored in recvset. In other words, a vote v is in recvset
             * if v.electionEpoch == logicalclock. The current participant uses recvset to deduce on whether a majority
             * of participants has voted for it.
             */
            // TODO_MA 注释： 存储投票的集合
            // TODO_MA 注释： long key =  serverID
            // TODO_MA 注释： Vote value = 对应的 serverID 的选票
            // TODO_MA 注释： 如果 zk 有 7 台服务器，则 recvset 最多有 7 个
            Map<Long, Vote> recvset = new HashMap<Long, Vote>();

            /*
             * The votes from previous leader elections, as well as the votes from the current leader election are
             * stored in outofelection. Note that notifications in a LOOKING state are not stored in outofelection.
             * Only FOLLOWING or LEADING notifications are stored in outofelection. The current participant could use
             * outofelection to learn which participant is the leader if it arrives late (i.e., higher logicalclock than
             * the electionEpoch of the received notifications) in a leader election.
             */
            // TODO_MA 注释： 存储了上一次选举和这一次选举的票（leading 或者 following 的选票存储在这里）
            Map<Long, Vote> outofelection = new HashMap<Long, Vote>();

            int notTimeout = minNotificationInterval;

            synchronized(this) {
                // TODO_MA 注释： 第一个步骤： 自增选举轮次
                logicalclock.incrementAndGet();

                // TODO_MA 注释： 第二个步骤： 更新选票
                // TODO_MA 注释： 选票中的信息，都是自己的
                // TODO_MA 注释： epoch， zxid， myid
                updateProposal(getInitId(), getInitLastLoggedZxid(), getPeerEpoch());
            }

            // 准备选票
            // currentVote = new Vote(myid, getLastLoggedZxid(), getCurrentEpoch());

            // TODO_MA 注释： 第三个步骤： 发送选票给所有其他服务器节点
            LOG.info("New election. My id = {}, proposed zxid=0x{}", self.getId(), Long.toHexString(proposedZxid));
            sendNotifications();

            SyncedLearnerTracker voteSet;

            /*
             * Loop in which we exchange notifications until we find a leader
             */

            // TODO_MA 注释： 进入循环，直到选出来 Leader 就退出，或者 当前节点关闭了
            while((self.getPeerState() == ServerState.LOOKING) && (!stop)) {
                /*
                 * Remove next notification from queue, times out after 2 times
                 * the termination time
                 */
                // TODO_MA 注释： 第四个步骤： 获取一个合法投票
                Notification n = recvqueue.poll(notTimeout, TimeUnit.MILLISECONDS);

                // TODO_MA 注释： 从此往后的代码，就是执行选举的逻辑处理

                /*
                 * Sends more notifications if haven't received enough.
                 * Otherwise processes new notification.
                 */
                // TODO_MA 注释： 如果 n = null, 意味着，可能选票并没有发送出去。
                // TODO_MA 注释： 如果如果之前发了，则重发，如果之前没发，则联系其他服务器进行发送
                if(n == null) {
                    if(manager.haveDelivered()) {
                        sendNotifications();
                    } else {
                        manager.connectAll();
                    }

                    /*
                     * Exponential backoff
                     * 超时时间是一个这样的序列： 0.2   0.4   0.8  1.6  ..... 60s
                     */
                    int tmpTimeOut = notTimeout * 2;
                    notTimeout = Math.min(tmpTimeOut, maxNotificationInterval);
                    LOG.info("Notification time out: {}", notTimeout);
                }

                // TODO_MA 注释： 如果 n != null， 则校验它的合法性
                // TODO_MA 注释： 投票节点 n.sid 和 推举的leader节点 n.leader 必须都是 participant 成员
                // TODO_MA 注释： n.sid 发票过来的 server 的 myid = myid1
                // TODO_MA 注释： n.leader 发票过来的 server 推举的 leader 的 myid = myid2
                // TODO_MA 注释： 验证 myid1 有没有选举权， 验证 myid2 有没有被选举权
                else if(validVoter(n.sid) && validVoter(n.leader)) {

                    // TODO_MA 注释： 这个地方就体现出来了 follower 和 observer 的区别了

                    // TODO_MA 注释： 根据对方的状态来判断
                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释：
                     *  switch(n.state) {
                     *      case LOOKING:
                     *      case LEADING:
                     *      case OBSERVING:
                     *      case FOLLOWING:
                     */
                    /*
                     * Only proceed if the vote comes from a replica in the current or next
                     * voting view for a replica in the current or next voting view.
                     * // TODO_MA 注释： n.state 就是对方给我发票的那个服务器的状态
                     * // TODO_MA 注释： 如果 zk1  zk2 zk3 zk4 先启动， 能选出来leader，假设是 3
                     * // TODO_MA 注释： 当 zk3 成为leader 之后， zk5 启动了
                     * // TODO_MA 注释： zk5 的状态是 LOOKING。 zk1 zk2  zk4 是 FOLLOWING,  zk3 是 leading
                     *
                     * // TODO_MA 注释： 大家一起启动！ 大家都是 LOOKING
                     *
                     * // TODO_MA 注释： 如果此时从 对方接收到 票。 对方有可能是 leader 有可能是 follower，也有可能是在执行选举
                     * // TODO_MA 注释： 判断对方的状态是什么
                     * // TODO_MA 注释： 如果对方是 follower 或者 leader， 则他们返回的信息，就是当前 Leader 的信息
                     * // TODO_MA 注释： 如果对方是 LOOKING 状态，则意味着对方也在执行选举。
                     */
                    switch(n.state) {

                        // TODO_MA 注释： 如果对方为 LOOKING， 证明对方，也是在执行选举
                        case LOOKING:
                            if(getInitLastLoggedZxid() == -1) {
                                LOG.debug("Ignoring notification as our zxid is -1");
                                break;
                            }
                            if(n.zxid == -1) {
                                LOG.debug("Ignoring notification from member with -1 zxid {}", n.sid);
                                break;
                            }

                            /*************************************************
                             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                             *  注释： 第五个步骤： 逻辑时钟校准
                             */

                            // TODO_MA 注释： 第5.1步骤： 如果对方逻辑时钟，大于当前节点
                            // If notification > current, replace and send messages out
                            if(n.electionEpoch > logicalclock.get()) {

                                // TODO_MA 注释： 更新逻辑时钟
                                logicalclock.set(n.electionEpoch);
                                // TODO_MA 注释： 清空所有投票
                                recvset.clear();

                                // TODO_MA 注释： 校验投票，如果对方的选票更优，则更新自己的选票和对方的一样
                                // TODO_MA 注释： 前三个参数 n.leader, n.zxid, n.peerEpoch 是对方推举的 的 leader
                                // TODO_MA 注释： 后三个参数 getInitId(), getInitLastLoggedZxid(), getPeerEpoch() 我推举的
                                if(totalOrderPredicate(n.leader, n.zxid, n.peerEpoch, getInitId(),
                                        getInitLastLoggedZxid(), getPeerEpoch())) {
                                    updateProposal(n.leader, n.zxid, n.peerEpoch);
                                } else {
                                    updateProposal(getInitId(), getInitLastLoggedZxid(), getPeerEpoch());
                                }

                                // TODO_MA 注释： 更新了自己的选票之后，继续发
                                sendNotifications();
                            }

                            // TODO_MA 注释： 第5.2步骤： 如果对方的逻辑时钟比我的小，则忽略对方的投票
                            else if(n.electionEpoch < logicalclock.get()) {
                                LOG.debug(
                                        "Notification election epoch is smaller than logicalclock. n.electionEpoch = 0x{}, logicalclock=0x{}",
                                        Long.toHexString(n.electionEpoch), Long.toHexString(logicalclock.get()));
                                break;
                            }

                            // TODO_MA 注释： 只要某个节点，发现，有节点比它推举的leader更优
                            // TODO_MA 注释： 则当前节点就一定会更新自己的选票为对方节点推举的leader的信息
                            // TODO_MA 注释： 然后继续广播一次
                            // TODO_MA 注释： 其实，只要两两节点之间的数据发送是一定到达的情况，
                            // TODO_MA 注释： 那么其实所有节点接收到的票的集合是一样的
                            // TODO_MA 注释： 只要某一个节点能判断出来选举算法可以结束了，则其他所有节点都可以判断出来

                            // TODO_MA 注释： 第5.3步骤： 终于统一逻辑时钟了。
                            // TODO_MA 注释： 第六个步骤： 选票PK。统一了逻辑中，还需要判断，是不是对方的选票更优，如果是，则更新
                            else if(totalOrderPredicate(n.leader, n.zxid, n.peerEpoch, proposedLeader, proposedZxid,
                                    proposedEpoch)) {
                                updateProposal(n.leader, n.zxid, n.peerEpoch);
                                sendNotifications();
                            }

                            // TODO_MA 注释： 到这儿，是已经统一了逻辑时钟

                            LOG.debug(
                                    "Adding vote: from={}, proposed leader={}, proposed zxid=0x{}, proposed election epoch=0x{}",
                                    n.sid, n.leader, Long.toHexString(n.zxid), Long.toHexString(n.electionEpoch));

                            // TODO_MA 注释： 第八个步骤： 选票归档。将投票加入到接收集合中
                            // don't care about the version if it's in LOOKING state
                            recvset.put(n.sid, new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch));

                            // TODO_MA 注释： 1张票：  1 => 2    n.sid = 1, n.leader = 2
                            // TODO_MA 注释： 2张票：  2 => 2
                            // TODO_MA 注释： 3张票：  3 => 3

                            // TODO_MA 注释： 假设 第一张票是刚加进来的，那么 n.leader = 2, 2 的所有的票都放入到 voteSet 中

                            // TODO_MA 注释： 第九个步骤： 唱票
                            // TODO_MA 注释： 根据当前投票节点推举的 leader 来获取该 推举leader的投票集合
                            // TODO_MA 注释： 这句代码是从 recvset 获取刚才这个 n 这个票推举的 leader 的选票的集合
                            voteSet = getVoteTracker(recvset,
                                    new Vote(proposedLeader, proposedZxid, logicalclock.get(), proposedEpoch));
                            // TODO_MA 注释： 总共 5 个节点
                            // TODO_MA 注释： 1 2 3 2 3
                            // TODO_MA 注释： 2
                            // TODO_MA 注释： 3

                            // TODO_MA 注释： 判断这个集合，是否超过了半数
                            if(voteSet.hasAllQuorums()) {

                                // TODO_MA 注释： 总共五个节点： 1 2 2 2
                                // Verify if there is any change in the proposed leader
                                while((n = recvqueue.poll(finalizeWait, TimeUnit.MILLISECONDS)) != null) {

                                    // TODO_MA 注释： 如果发现有更优投票，则退出该循环
                                    if(totalOrderPredicate(n.leader, n.zxid, n.peerEpoch, proposedLeader, proposedZxid,
                                            proposedEpoch)) {
                                        recvqueue.put(n);
                                        break;
                                    }
                                }

                                // TODO_MA 注释： 当 n = null 的时候，证明 recvqueue 中已经没有多余投票了，而且也没有更优投票
                                /*
                                 * This predicate is true once we don't read any new relevant message from the
                                 * reception queue
                                 */
                                if(n == null) {

                                    // TODO_MA 注释： 第十个步骤： 切换状态
                                    setPeerState(proposedLeader, voteSet);

                                    // TODO_MA 注释： 存储最终胜选投票
                                    Vote endVote = new Vote(proposedLeader, proposedZxid, logicalclock.get(),
                                            proposedEpoch);

                                    // TODO_MA 注释： 清空 recvqueue 队列
                                    leaveInstance(endVote);

                                    // TODO_MA 注释： 到此为止，选举结束！
                                    // TODO_MA 注释： 最后得到的是： 一个 endVote（存储的是：  被推举成为leader 的服务器的信息）
                                    return endVote;
                                }
                            }
                            break;

                        // TODO_MA 注释： 如果对方是 observer，不管
                        case OBSERVING:
                            LOG.debug("Notification from observer: {}", n.sid);
                            break;

                        // TODO_MA 注释： 如果对方是 following 或者 leading 证明，对方已经通过获取的投票抉择出来了谁是 leader 了
                        case FOLLOWING:
                        case LEADING:
                            /*
                             * Consider all notifications from the same epoch together.
                             * // TODO_MA 注释： 如果逻辑时钟一样，则自己也存储投票，检查leader是否合法，切换自己的状态
                             */
                            if(n.electionEpoch == logicalclock.get()) {
                                // TODO_MA 注释： 当接受到来自于 Leader 或者 Follower 的选票。
                                // TODO_MA 注释： 而不是来自于 LOOKING 的选票，则直接就是 Leader 的信息，直接加入 recvset
                                recvset.put(n.sid, new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state));
                                voteSet = getVoteTracker(recvset,
                                        new Vote(n.version, n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state));

                                // TODO_MA 注释： 接着执行判断
                                // TODO_MA 注释： 判断是否有超过半数节点，返回的这个 leader 是同一个，如果是，则这个leader 就是合法leader
                                // TODO_MA 注释： 当前这个这个节点，也就自动追随这个leader，不用再继续选举了。
                                if(voteSet.hasAllQuorums() && checkLeader(recvset, n.leader, n.electionEpoch)) {

                                    // TODO_MA 注释： 切换
                                    setPeerState(n.leader, voteSet);

                                    // TODO_MA 注释： endVote 存储就是此时选举算法选举结束之后，成功当选leader 的节点的信息
                                    Vote endVote = new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch);
                                    leaveInstance(endVote);

                                    // TODO_MA 注释： 这个地方有 return 就意味着 这个 lookForLeader 选举方法结束了。
                                    // TODO_MA 注释： 选举出来了 Leader
                                    return endVote;
                                }
                            }

                            /*
                             * // TODO_MA 注释： 确认，是不是大家都在追随同一个领导
                             * Before joining an established ensemble, verify that a majority are following the same
                             * leader.
                             *
                             * Note that the outofelection map also stores votes from the current leader election.
                             * See ZOOKEEPER-1732 for more information.
                             */
                            outofelection.put(n.sid,
                                    new Vote(n.version, n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state));
                            voteSet = getVoteTracker(outofelection,
                                    new Vote(n.version, n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state));

                            if(voteSet.hasAllQuorums() && checkLeader(outofelection, n.leader, n.electionEpoch)) {
                                synchronized(this) {
                                    logicalclock.set(n.electionEpoch);
                                    setPeerState(n.leader, voteSet);
                                }
                                Vote endVote = new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch);
                                leaveInstance(endVote);
                                return endVote;
                            }
                            break;
                        default:
                            LOG.warn("Notification state unrecoginized: {} (n.state), {}(n.sid)", n.state, n.sid);
                            break;
                    }
                } else {
                    if(!validVoter(n.leader)) {
                        LOG.warn("Ignoring notification for non-cluster member sid {} from sid {}", n.leader, n.sid);
                    }
                    if(!validVoter(n.sid)) {
                        LOG.warn("Ignoring notification for sid {} from non-quorum member sid {}", n.leader, n.sid);
                    }
                }
            }
            return null;
        } finally {
            try {
                if(self.jmxLeaderElectionBean != null) {
                    MBeanRegistry.getInstance().unregister(self.jmxLeaderElectionBean);
                }
            } catch(Exception e) {
                LOG.warn("Failed to unregister with JMX", e);
            }
            self.jmxLeaderElectionBean = null;
            LOG.debug("Number of connection processing threads: {}", manager.getConnectionThreadCount());
        }
    }

    /**
     * Check if a given sid is represented in either the current or
     * the next voting view
     *
     * @param sid Server identifier
     * @return boolean
     */
    private boolean validVoter(long sid) {
        return self.getCurrentAndNextConfigVoters().contains(sid);
    }

}
