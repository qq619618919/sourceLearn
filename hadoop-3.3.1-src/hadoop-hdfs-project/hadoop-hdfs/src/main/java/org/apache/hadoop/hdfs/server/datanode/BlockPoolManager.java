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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;
import org.slf4j.Logger;

/**
 * Manages the BPOfferService objects for the data node.
 * Creation, removal, starting, stopping, shutdown on BPOfferService
 * objects must be done via APIs in this class.
 */
@InterfaceAudience.Private
class BlockPoolManager {
    private static final Logger LOG = DataNode.LOG;

    // TODO_MA 马中华 注释： 根据 nameservice 索引 BPOfferService
    private final Map<String, BPOfferService> bpByNameserviceId = Maps.newHashMap();

    // TODO_MA 马中华 注释： 根据 blockpoolid 索引 BPOfferService
    private final Map<String, BPOfferService> bpByBlockPoolId = Maps.newHashMap();

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 一个 BPOfferService 对应到一个 名称空间
     *  联邦集群中，会有多个 名称空间，则对应到有多个 BPOfferService
     */
    private final List<BPOfferService> offerServices = new CopyOnWriteArrayList<>();

    private final DataNode dn;

    //This lock is used only to ensure exclusion of refreshNamenodes
    private final Object refreshNamenodesLock = new Object();

    BlockPoolManager(DataNode dn) {
        this.dn = dn;
    }

    synchronized void addBlockPool(BPOfferService bpos) {
        Preconditions.checkArgument(offerServices.contains(bpos), "Unknown BPOS: %s", bpos);
        if (bpos.getBlockPoolId() == null) {
            throw new IllegalArgumentException("Null blockpool id");
        }
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： bpid ==> blockpool
         */
        bpByBlockPoolId.put(bpos.getBlockPoolId(), bpos);
    }

    /**
     * Returns a list of BPOfferService objects. The underlying list
     * implementation is a CopyOnWriteArrayList so it can be safely
     * iterated while BPOfferServices are being added or removed.
     *
     * Caution: The BPOfferService returned could be shutdown any time.
     */
    synchronized List<BPOfferService> getAllNamenodeThreads() {
        return Collections.unmodifiableList(offerServices);
    }

    synchronized BPOfferService get(String bpid) {
        return bpByBlockPoolId.get(bpid);
    }

    synchronized void remove(BPOfferService t) {
        offerServices.remove(t);
        if (t.hasBlockPoolId()) {
            // It's possible that the block pool never successfully registered
            // with any NN, so it was never added it to this map
            bpByBlockPoolId.remove(t.getBlockPoolId());
        }

        boolean removed = false;
        for (Iterator<BPOfferService> it = bpByNameserviceId.values().iterator(); it.hasNext() && !removed; ) {
            BPOfferService bpos = it.next();
            if (bpos == t) {
                it.remove();
                LOG.info("Removed " + bpos);
                removed = true;
            }
        }

        if (!removed) {
            LOG.warn("Couldn't remove BPOS " + t + " from bpByNameserviceId map");
        }
    }

    void shutDownAll(List<BPOfferService> bposList) throws InterruptedException {
        for (BPOfferService bpos : bposList) {
            bpos.stop(); //interrupts the threads
        }
        //now join
        for (BPOfferService bpos : bposList) {
            bpos.join();
        }
    }

    synchronized void startAll() throws IOException {
        try {
            UserGroupInformation.getLoginUser().doAs(new PrivilegedExceptionAction<Object>() {
                @Override
                public Object run() throws Exception {

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 启动每个 BPOfferService
                     *  BPOfferService 就是给一个 联邦提供服务
                     *  如果你的集群有 3 个联邦，有三对 anmenode，datanode 中就会生成 3 个 BPOfferService
                     *  6 个 BPServiceActor 组件来完成对接
                     */
                    for (BPOfferService bpos : offerServices) {
                        bpos.start();
                    }
                    return null;
                }
            });
        } catch (InterruptedException ex) {
            IOException ioe = new IOException();
            ioe.initCause(ex.getCause());
            throw ioe;
        }
    }

    void joinAll() {
        for (BPOfferService bpos : this.getAllNamenodeThreads()) {
            bpos.join();
        }
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 重要工作，DataNode 启动之后的三件重要工作：
     *  1、注册
     *  2、心跳
     *  3、数据块汇报
     */
    void refreshNamenodes(Configuration conf) throws IOException {
        LOG.info("Refresh request received for nameservices: " + conf.get(DFSConfigKeys.DFS_NAMESERVICES));

        Map<String, Map<String, InetSocketAddress>> newAddressMap = null;
        Map<String, Map<String, InetSocketAddress>> newLifelineAddressMap = null;
        try {
            newAddressMap = DFSUtil.getNNServiceRpcAddressesForCluster(conf);
            newLifelineAddressMap = DFSUtil.getNNLifelineRpcAddressesForCluster(conf);
        } catch (IOException ioe) {
            LOG.warn("Unable to get NameNode addresses.", ioe);
        }

        if (newAddressMap == null || newAddressMap.isEmpty()) {
            throw new IOException("No services to connect, missing NameNode " + "address.");
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 调用内部实现
         */
        synchronized (refreshNamenodesLock) {
            doRefreshNamenodes(newAddressMap, newLifelineAddressMap);
        }
    }

    private void doRefreshNamenodes(Map<String, Map<String, InetSocketAddress>> addrMap,
                                    Map<String, Map<String, InetSocketAddress>> lifelineAddrMap) throws IOException {
        assert Thread.holdsLock(refreshNamenodesLock);

        Set<String> toRefresh = Sets.newLinkedHashSet();
        Set<String> toAdd = Sets.newLinkedHashSet();
        Set<String> toRemove;

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 创建和启动 BPOfferService
         *  简单来说： 一个 NameService 启动一个 BPOfferService
         *  一个 NameService 中有多个 NameNode, 对应到 BPOfferService 中的多个 BPServiceActor
         */
        synchronized (this) {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 对于每个新的名称服务，弄清楚它是现有 NS 的一组 NN 的更新，还是全新的名称服务。
             */
            // Step 1. For each of the new nameservices, figure out whether
            // it's an update of the set of NNs for an existing NS, or an entirely new nameservice.
            for (String nameserviceId : addrMap.keySet()) {
                if (bpByNameserviceId.containsKey(nameserviceId)) {
                    toRefresh.add(nameserviceId);
                } else {
                    toAdd.add(nameserviceId);
                }
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 我们目前拥有但不再存在的任何名称服务都需要删除。
             */
            // Step 2. Any nameservices we currently have but are no longer present need to be removed.
            toRemove = Sets.newHashSet(Sets.difference(bpByNameserviceId.keySet(), addrMap.keySet()));

            assert toRefresh.size() + toAdd.size() == addrMap.size() : "toAdd: " + Joiner.on(",").useForNull("<default>")
                    .join(toAdd) + "  toRemove: " + Joiner.on(",").useForNull("<default>")
                    .join(toRemove) + "  toRefresh: " + Joiner.on(",").useForNull("<default>").join(toRefresh);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 启动新的名称服务
             */
            // Step 3. Start new nameservices
            if (!toAdd.isEmpty()) {
                LOG.info("Starting BPOfferServices for nameservices: " + Joiner.on(",").useForNull("<default>").join(toAdd));

                // TODO_MA 马中华 注释： 遍历 需要初始化的 ns 一个联邦 一个 ns 生成一个 BPOfferService
                for (String nsToAdd : toAdd) {
                    Map<String, InetSocketAddress> nnIdToAddr = addrMap.get(nsToAdd);
                    Map<String, InetSocketAddress> nnIdToLifelineAddr = lifelineAddrMap.get(nsToAdd);
                    ArrayList<InetSocketAddress> addrs = Lists.newArrayListWithCapacity(nnIdToAddr.size());
                    ArrayList<String> nnIds = Lists.newArrayListWithCapacity(nnIdToAddr.size());
                    ArrayList<InetSocketAddress> lifelineAddrs = Lists.newArrayListWithCapacity(nnIdToAddr.size());
                    for (String nnId : nnIdToAddr.keySet()) {
                        addrs.add(nnIdToAddr.get(nnId));
                        nnIds.add(nnId);
                        lifelineAddrs.add(nnIdToLifelineAddr != null ? nnIdToLifelineAddr.get(nnId) : null);
                    }

                    // TODO_MA 马中华 注释： 创建 BPOfferService
                    BPOfferService bpos = createBPOS(nsToAdd, nnIds, addrs, lifelineAddrs);
                    bpByNameserviceId.put(nsToAdd, bpos);
                    offerServices.add(bpos);
                }
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 启动 BPOfferService
             */
            startAll();
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 关闭旧的名称服务。这发生在 synchronized(this) 锁之外，因为它们需要从另一个线程回调到 .remove()
         */
        // Step 4. Shut down old nameservices. This happens outside
        // of the synchronized(this) lock since they need to call
        // back to .remove() from another thread
        if (!toRemove.isEmpty()) {
            LOG.info("Stopping BPOfferServices for nameservices: " + Joiner.on(",").useForNull("<default>").join(toRemove));

            for (String nsToRemove : toRemove) {
                BPOfferService bpos = bpByNameserviceId.get(nsToRemove);
                bpos.stop();
                bpos.join();
                // they will call remove on their own
            }
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 更新 NN 列表已更改的名称服务
         *  如果有新增加的 namenode， 都启动对应的 BPServiceActor
         */
        // Step 5. Update nameservices whose NN list has changed
        if (!toRefresh.isEmpty()) {
            LOG.info("Refreshing list of NNs for nameservices: " + Joiner.on(",").useForNull("<default>").join(toRefresh));

            for (String nsToRefresh : toRefresh) {
                BPOfferService bpos = bpByNameserviceId.get(nsToRefresh);
                Map<String, InetSocketAddress> nnIdToAddr = addrMap.get(nsToRefresh);
                Map<String, InetSocketAddress> nnIdToLifelineAddr = lifelineAddrMap.get(nsToRefresh);
                ArrayList<InetSocketAddress> addrs = Lists.newArrayListWithCapacity(nnIdToAddr.size());
                ArrayList<InetSocketAddress> lifelineAddrs = Lists.newArrayListWithCapacity(nnIdToAddr.size());
                ArrayList<String> nnIds = Lists.newArrayListWithCapacity(nnIdToAddr.size());
                for (String nnId : nnIdToAddr.keySet()) {
                    addrs.add(nnIdToAddr.get(nnId));
                    lifelineAddrs.add(nnIdToLifelineAddr != null ? nnIdToLifelineAddr.get(nnId) : null);
                    nnIds.add(nnId);
                }
                try {
                    UserGroupInformation.getLoginUser().doAs(new PrivilegedExceptionAction<Object>() {
                        @Override
                        public Object run() throws Exception {
                            bpos.refreshNNList(nsToRefresh, nnIds, addrs, lifelineAddrs);
                            return null;
                        }
                    });
                } catch (InterruptedException ex) {
                    IOException ioe = new IOException();
                    ioe.initCause(ex.getCause());
                    throw ioe;
                }
            }
        }
    }

    /**
     * Extracted out for test purposes.
     */
    protected BPOfferService createBPOS(final String nameserviceId, List<String> nnIds, List<InetSocketAddress> nnAddrs,
                                        List<InetSocketAddress> lifelineNnAddrs) {
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return new BPOfferService(nameserviceId, nnIds, nnAddrs, lifelineNnAddrs, dn);
    }
}
