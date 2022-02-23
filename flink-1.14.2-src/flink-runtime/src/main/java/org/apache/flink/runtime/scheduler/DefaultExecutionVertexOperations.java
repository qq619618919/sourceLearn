/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;

import java.util.concurrent.CompletableFuture;

class DefaultExecutionVertexOperations implements ExecutionVertexOperations {

    // TODO_MA 马中华 注释： SlotExecutionVertexAssignment
    // TODO_MA 马中华 注释： Slot = LogicalSlot
    // TODO_MA 马中华 注释： ExecutionVertex (ExecutionGraph 中一个顶点，需要部署一个 Task)
    @Override
    public void deploy(final ExecutionVertex executionVertex) throws JobException {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： ExecutionVertex 就是 ExecutionGraph 中的一个顶点
         *  当时：在创建 ExecutionVertex 的时候，内部有一个 Executioin 的对象
         */
        executionVertex.deploy();
    }
    // TODO_MA 马中华 注释： 这个顶点使用哪个Slot呢?

    @Override
    public CompletableFuture<?> cancel(final ExecutionVertex executionVertex) {
        return executionVertex.cancel();
    }

    @Override
    public void markFailed(final ExecutionVertex executionVertex, final Throwable cause) {
        executionVertex.markFailed(cause);
    }
}
