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

package org.apache.flink.runtime.blob;

import java.io.Closeable;

/** A simple store and retrieve binary large objects (BLOBs). */
/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 这是个文件服务/大对象的顶级抽象
 *  1、服务端： BlobServer（BIO 服务端 ServerSocket）
 *  2、客户端： BlobClient（BIO 客户端：Socket）
 *  他们之间的通过 BIO Socket 通信来做
 *  -
 *  作用：
 *  1、当提交了一个 job 的话，这个 job 至少对应到一个 jar 包，同时还有依赖 jar 包，还有其他的有些资料
 *  WebMonitorEndpoint 接收到 RestClient 提交过来的 JObGraph 之后，会吧这个 job 的相关资料，都上传到 BlobServer
 *  -
 *  当 JobMaster 部署 Task 的时候，不需要把这些东西直接发给 Task  而是 Task 接收到部署任务的时候，自己去 BlobServer 中
 *  去下载这个 Task 对应的那些 BlobKey 的文件即可
 *  提供这个文件服务器，其实就是把 push 变成了 poll
 *  这个东西， spark 中也有一个类似的。做文件中转。 如果 Task 发生了 迁移。 那个地方什么时候启动了Task 自己去获取相关生产资料
 */
public interface BlobService extends Closeable {

    /**
     * Returns a BLOB service for accessing permanent BLOBs.
     *  // TODO_MA 马中华 注释： 永久性的文件服务。文件存进来了之后，不手动删除，则一直保存
     * @return BLOB service
     */
    PermanentBlobService getPermanentBlobService();

    /**
     * Returns a BLOB service for accessing transient BLOBs.
     *  // TODO_MA 马中华 注释： 提供了一种 短暂性的 文件存储服务
     *  // TODO_MA 马中华 注释： 文件存在过期，内部通过一个定时任务来做
     *  // TODO_MA 马中华 注释： 定时任务每隔一段时间执行一次检查，filter出来过期的文件，执行删除
     * @return BLOB service
     */
    TransientBlobService getTransientBlobService();

    /**
     * Returns the port of the BLOB server that this BLOB service is working with.
     *
     * @return the port of the blob server.
     */
    int getPort();
}
