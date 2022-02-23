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

package org.apache.flink.runtime.registration;

import org.apache.flink.runtime.rpc.RpcGateway;

import org.slf4j.Logger;

import java.io.Serializable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * // TODO_MA 马中华 注释： 该实用程序类实现了从一个组件到另一个组件的 RPC 连接的基础，
 * // TODO_MA 马中华 注释： 例如从 TaskExecutor 到 ResourceManager 的 RPC 连接。
 * // TODO_MA 马中华 注释： 这个 {@code RegisteredRpcConnection} 实现注册和获取目标网关。
 * This utility class implements the basis of RPC connecting from one component to another
 * component, for example the RPC connection from TaskExecutor to ResourceManager. This {@code
 * RegisteredRpcConnection} implements registration and get target gateway.
 *
 * // TODO_MA 马中华 注释： 注册可以访问在成功注册后完成的未来。
 * // TODO_MA 马中华 注释： RPC 连接可以关闭，例如当它尝试注册的目标失去领导者状态时。
 * <p>The registration gives access to a future that is completed upon successful registration. The
 * RPC connection can be closed, for example when the target where it tries to register at looses
 * leader status.
 *
 * @param <F> The type of the fencing token
 * @param <G> The type of the gateway to connect to.
 * @param <S> The type of the successful registration responses.
 * @param <R> The type of the registration rejection responses.
 */
public abstract class RegisteredRpcConnection<F extends Serializable, G extends RpcGateway, S extends RegistrationResponse.Success, R extends RegistrationResponse.Rejection> {

    private static final AtomicReferenceFieldUpdater<RegisteredRpcConnection, RetryingRegistration> REGISTRATION_UPDATER = AtomicReferenceFieldUpdater.newUpdater(
            RegisteredRpcConnection.class, RetryingRegistration.class, "pendingRegistration");

    /** The logger for all log messages of this class. */
    protected final Logger log;

    /** The fencing token fo the remote component. */
    private final F fencingToken;

    /** The target component Address, for example the ResourceManager Address. */
    private final String targetAddress;

    /**
     * Execution context to be used to execute the on complete action of the
     * ResourceManagerRegistration.
     */
    private final Executor executor;

    /** The Registration of this RPC connection. */
    private volatile RetryingRegistration<F, G, S, R> pendingRegistration;

    /** The gateway to register, it's null until the registration is completed. */
    private volatile G targetGateway;

    /** Flag indicating that the RPC connection is closed. */
    private volatile boolean closed;

    // ------------------------------------------------------------------------

    public RegisteredRpcConnection(Logger log,
                                   String targetAddress,
                                   F fencingToken,
                                   Executor executor) {
        this.log = checkNotNull(log);
        this.targetAddress = checkNotNull(targetAddress);
        this.fencingToken = checkNotNull(fencingToken);
        this.executor = checkNotNull(executor);
    }

    // ------------------------------------------------------------------------
    //  Life cycle
    // ------------------------------------------------------------------------

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 启动的时候，调用
     *  接下来，这是一个通用的注册机制！
     *  1\ TaskExecutor 启动好了之后，需要向 ResourceManager 注册
     *  2\ JobMaster    启动好了之后，需要向 ResourceManager 注册
     *  关于注册机制，就是通过： RegisteredRpcConnection 来实现的
     *  -
     *  A： 生成一个代表一次注册行为的抽象对象： RetryingRegistion
     *  B： 真正执行注册：发送 RPC 请求给 主节点，接收到响应
     *  C： 拿到注册结果，然后执行 注册成功或失败的 相应回调
     *  D： 解析到这个响应，解析响应，生成注册结果
     *  -
     *  A B C D  是看到的代码的顺序
     *  A B D C  是代码的执行顺序
     */
    public void start() {
        checkState(!closed, "The RPC connection is already closed");
        checkState(!isConnected() && pendingRegistration == null, "The RPC connection is already started");

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 第一个步骤： 创建注册对象： RetryingRegistration 尝试做一次注册的一个动作的抽象
         *  跟之前的那个 TaskExecutorRegistration 是不一样的： 只是包含了一些必要的从节点的信息
         *  -
         *  内部做了两件事：
         *  1、生成一个 RetryingRegistration   A
         *  2、构建了一个回调逻辑   C
         */
        final RetryingRegistration<F, G, S, R> newRegistration = createNewRegistration();

        if (REGISTRATION_UPDATER.compareAndSet(this, null, newRegistration)) {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 第二个步骤： 开启注册流程  B
             *  内部还有另外一个动作叫做： D：解析响应，生成 注册结果
             */
            newRegistration.startRegistration();
        } else {
            // concurrent start operation
            newRegistration.cancel();
        }
        // TODO_MA 马中华 注释： 真正的执行顺序是: A, B, C
        // TODO_MA 马中华 注释： 真正的执行顺序是：A, B, D, C
    }

    /**
     * Tries to reconnect to the {@link #targetAddress} by cancelling the pending registration and
     * starting a new pending registration.
     *
     * @return {@code false} if the connection has been closed or a concurrent modification has
     *         happened; otherwise {@code true}
     */
    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 重连的时候调用
     */
    public boolean tryReconnect() {
        checkState(isConnected(), "Cannot reconnect to an unknown destination.");

        if (closed) {
            return false;
        } else {
            final RetryingRegistration<F, G, S, R> currentPendingRegistration = pendingRegistration;

            if (currentPendingRegistration != null) {
                currentPendingRegistration.cancel();
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            final RetryingRegistration<F, G, S, R> newRegistration = createNewRegistration();

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            if (REGISTRATION_UPDATER.compareAndSet(this, currentPendingRegistration, newRegistration)) {
                newRegistration.startRegistration();
            } else {
                // concurrent modification
                newRegistration.cancel();
                return false;
            }

            // double check for concurrent close operations
            if (closed) {
                newRegistration.cancel();

                return false;
            } else {
                return true;
            }
        }
    }

    /**
     * This method generate a specific Registration, for example TaskExecutor Registration at the
     * ResourceManager.
     */
    protected abstract RetryingRegistration<F, G, S, R> generateRegistration();

    /** This method handle the Registration Response. */
    protected abstract void onRegistrationSuccess(S success);

    /**
     * This method handles the Registration rejection.
     *
     * @param rejection rejection containing additional information about the rejection
     */
    protected abstract void onRegistrationRejection(R rejection);

    /** This method handle the Registration failure. */
    protected abstract void onRegistrationFailure(Throwable failure);

    /** Close connection. */
    public void close() {
        closed = true;

        // make sure we do not keep re-trying forever
        if (pendingRegistration != null) {
            pendingRegistration.cancel();
        }
    }

    public boolean isClosed() {
        return closed;
    }

    // ------------------------------------------------------------------------
    //  Properties
    // ------------------------------------------------------------------------

    public F getTargetLeaderId() {
        return fencingToken;
    }

    public String getTargetAddress() {
        return targetAddress;
    }

    /** Gets the RegisteredGateway. This returns null until the registration is completed. */
    public G getTargetGateway() {
        return targetGateway;
    }

    public boolean isConnected() {
        return targetGateway != null;
    }

    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        String connectionInfo = "(ADDRESS: " + targetAddress + " FENCINGTOKEN: " + fencingToken + ")";

        if (isConnected()) {
            connectionInfo = "RPC connection to " + targetGateway.getClass().getSimpleName() + " " + connectionInfo;
        } else {
            connectionInfo = "RPC connection to " + connectionInfo;
        }

        if (isClosed()) {
            connectionInfo += " is closed";
        } else if (isConnected()) {
            connectionInfo += " is established";
        } else {
            connectionInfo += " is connecting";
        }

        return connectionInfo;
    }

    // ------------------------------------------------------------------------
    //  Internal methods
    // ------------------------------------------------------------------------

    private RetryingRegistration<F, G, S, R> createNewRegistration() {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：  A 生成链接实例
         */
        RetryingRegistration<F, G, S, R> newRegistration = checkNotNull(generateRegistration());

        CompletableFuture<RetryingRegistration.RetryingRegistrationResult<G, S, R>> future = newRegistration.getFuture();

        // TODO_MA 马中华 注释： RetryingRegistration 代表一次注册行为，必然是这个行为完成了之后，才回调
        // TODO_MA 马中华 注释： 这个注册行为，事实上是包含了 A, B , D 三个动作的
        // TODO_MA 马中华 注释： A ： 生成一个抽象对象，表示这一次注册行为
        // TODO_MA 马中华 注释： B ： 真正执行
        // TODO_MA 马中华 注释： D ： 解析注册响应，生成注册结果

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： C 依据注册的结果来执行 成功回调 或者 失败回调
         */
        future.whenCompleteAsync((RetryingRegistration.RetryingRegistrationResult<G, S, R> result, Throwable failure) -> {
            if (failure != null) {
                if (failure instanceof CancellationException) {
                    // we ignore cancellation exceptions because they originate from
                    // cancelling the RetryingRegistration
                    log.debug("Retrying registration towards {} was cancelled.", targetAddress);
                } else {
                    // this future should only ever fail if there is a bug, not if the registration is declined
                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 注册失败的回调
                     */
                    onRegistrationFailure(failure);
                }
            } else {

                // TODO_MA 马中华 注释： 注册成功
                if (result.isSuccess()) {
                    targetGateway = result.getGateway();

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 注册成功的回调
                     */
                    onRegistrationSuccess(result.getSuccess());
                }

                // TODO_MA 马中华 注释： 注册被拒绝
                else if (result.isRejection()) {

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释：
                     */
                    onRegistrationRejection(result.getRejection());
                } else {
                    throw new IllegalArgumentException(String.format("Unknown retrying registration response: %s.", result));
                }
            }
        }, executor);

        return newRegistration;
    }
}
