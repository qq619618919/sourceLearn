/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskexecutor;

import java.util.concurrent.CompletableFuture;

/**
 * Simple adapter for {@link TaskExecutor} to adapt to {@link
 * TaskManagerRunner.TaskExecutorService}.
 */
public class TaskExecutorToServiceAdapter implements TaskManagerRunner.TaskExecutorService {

    private final TaskExecutor taskExecutor;

    private TaskExecutorToServiceAdapter(TaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    @Override
    public void start() {

        // TODO_MA 马中华 注释： 回到 TaskExecutor 的 onStart() 方法
        // TODO_MA 马中华 注释： TaskExecutor 是一个 RpcEndpoint
        // TODO_MA 马中华 注释： 此时启动一个 RpcEndpoint，然后跳转到 TaskExecutor 的 onStart();
        taskExecutor.start();
    }

    @Override
    public CompletableFuture<Void> getTerminationFuture() {
        return taskExecutor.getTerminationFuture();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return taskExecutor.closeAsync();
    }

    public static TaskExecutorToServiceAdapter createFor(TaskExecutor taskExecutor) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return new TaskExecutorToServiceAdapter(taskExecutor);
    }
}
