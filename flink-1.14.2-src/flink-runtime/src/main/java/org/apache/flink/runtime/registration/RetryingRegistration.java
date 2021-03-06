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

import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This utility class implements the basis of registering one component at another component, for
 * example registering the TaskExecutor at the ResourceManager. This {@code RetryingRegistration}
 * implements both the initial address resolution and the retries-with-backoff strategy.
 *
 * <p>The registration gives access to a future that is completed upon successful registration. The
 * registration can be canceled, for example when the target where it tries to register at looses
 * leader status.
 *
 * @param <F> The type of the fencing token
 * @param <G> The type of the gateway to connect to.
 * @param <S> The type of the successful registration responses.
 * @param <R> The type of the registration rejection responses.
 */
public abstract class RetryingRegistration<F extends Serializable, G extends RpcGateway, S extends RegistrationResponse.Success, R extends RegistrationResponse.Rejection> {

    // ------------------------------------------------------------------------
    // Fields
    // ------------------------------------------------------------------------

    private final Logger log;

    private final RpcService rpcService;

    private final String targetName;

    private final Class<G> targetType;

    private final String targetAddress;

    private final F fencingToken;

    private final CompletableFuture<RetryingRegistrationResult<G, S, R>> completionFuture;

    private final RetryingRegistrationConfiguration retryingRegistrationConfiguration;

    private volatile boolean canceled;

    // ------------------------------------------------------------------------

    public RetryingRegistration(Logger log,
                                RpcService rpcService,
                                String targetName,
                                Class<G> targetType,
                                String targetAddress,
                                F fencingToken,
                                RetryingRegistrationConfiguration retryingRegistrationConfiguration) {

        this.log = checkNotNull(log);
        this.rpcService = checkNotNull(rpcService);
        this.targetName = checkNotNull(targetName);
        this.targetType = checkNotNull(targetType);
        this.targetAddress = checkNotNull(targetAddress);
        this.fencingToken = checkNotNull(fencingToken);
        this.retryingRegistrationConfiguration = checkNotNull(retryingRegistrationConfiguration);

        this.completionFuture = new CompletableFuture<>();
    }

    // ------------------------------------------------------------------------
    //  completion and cancellation
    // ------------------------------------------------------------------------

    public CompletableFuture<RetryingRegistrationResult<G, S, R>> getFuture() {
        return completionFuture;
    }

    /** Cancels the registration procedure. */
    public void cancel() {
        canceled = true;
        completionFuture.cancel(false);
    }

    /**
     * Checks if the registration was canceled.
     *
     * @return True if the registration was canceled, false otherwise.
     */
    public boolean isCanceled() {
        return canceled;
    }

    // ------------------------------------------------------------------------
    //  registration
    // ------------------------------------------------------------------------

    protected abstract CompletableFuture<RegistrationResponse> invokeRegistration(G gateway,
                                                                                  F fencingToken,
                                                                                  long timeoutMillis) throws Exception;

    /**
     * This method resolves the target address to a callable gateway and starts the registration
     * after that.
     */
    @SuppressWarnings("unchecked")
    public void startRegistration() {
        if (canceled) {
            // we already got canceled
            return;
        }

        try {
            // trigger resolution of the target address to a callable gateway
            final CompletableFuture<G> rpcGatewayFuture;

            /*************************************************
             * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
             *  ????????? ??????
             *  ???????????? ????????? TaskExeuctor ??? ????????? ResourceManager ???????????????
             *  XXXXConnectioin ??????????????????????????????????????????????????????????????????
             */
            if (FencedRpcGateway.class.isAssignableFrom(targetType)) {
                rpcGatewayFuture = (CompletableFuture<G>) rpcService.connect(targetAddress,
                        fencingToken,
                        targetType.asSubclass(FencedRpcGateway.class)
                );
            } else {
                rpcGatewayFuture = rpcService.connect(targetAddress, targetType);
            }

            // upon success, start the registration attempts
            CompletableFuture<Void> rpcGatewayAcceptFuture = rpcGatewayFuture.thenAcceptAsync((G rpcGateway) -> {
                log.info("Resolved {} address, beginning registration", targetName);
                /*************************************************
                 * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
                 *  ????????? ???????????????cluster.registration.initial-timeout = 0.1s
                 *  -
                 *  ???????????????????????????????????????????????????????????????
                 *  1??????????????????????????? ?????????????????? initTimeout ???0.1s  ?????????????????? maxTimeOut???30s
                 *  2???currentTimeout = initTimeout
                 *  2????????????????????????????????? currentTimeout = Math.min(currentTimeout * 2, maxTimeOut)
                 */
                register(rpcGateway, 1, retryingRegistrationConfiguration.getInitialRegistrationTimeoutMillis());
            }, rpcService.getScheduledExecutor());

            // upon failure, retry, unless this is cancelled
            // TODO_MA ????????? ????????? ????????????????????????????????????????????????????????????????????????????????????
            rpcGatewayAcceptFuture.whenCompleteAsync((Void v, Throwable failure) -> {
                if (failure != null && !canceled) {
                    final Throwable strippedFailure = ExceptionUtils.stripCompletionException(failure);
                    if (log.isDebugEnabled()) {
                        log.debug("Could not resolve {} address {}, retrying in {} ms.",
                                targetName,
                                targetAddress,
                                retryingRegistrationConfiguration.getErrorDelayMillis(),
                                strippedFailure
                        );
                    } else {
                        log.info("Could not resolve {} address {}, retrying in {} ms: {}",
                                targetName,
                                targetAddress,
                                retryingRegistrationConfiguration.getErrorDelayMillis(),
                                strippedFailure.getMessage()
                        );
                    }

                    /*************************************************
                     * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
                     *  ????????? ??????
                     *  ?????? cluster.registration.error-delay = 10s
                     */
                    startRegistrationLater(retryingRegistrationConfiguration.getErrorDelayMillis());
                }
            }, rpcService.getScheduledExecutor());
        } catch (Throwable t) {
            completionFuture.completeExceptionally(t);
            cancel();
        }
    }

    /**
     * This method performs a registration attempt and triggers either a success notification or a
     * retry, depending on the result.
     */
    @SuppressWarnings("unchecked")
    private void register(final G gateway, final int attempt, final long timeoutMillis) {
        // eager check for canceling to avoid some unnecessary work
        if (canceled) {
            return;
        }

        try {
            log.debug("Registration at {} attempt {} (timeout={}ms)", targetName, attempt, timeoutMillis);
            /*************************************************
             * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
             *  ????????? B  ????????????
             *  RegistrationResponse  = ????????????
             *  -
             *  ???????????????  ????????? ??? ???????????? ???????????? RPC ??????????????? ??????????????????????????????????????????????????????????????????????????????????????????????????????
             *  RegistrationResponse = xxxGateway.registerXXXX(XXXRegistion)
             */
            CompletableFuture<RegistrationResponse> registrationFuture = invokeRegistration(gateway,
                    fencingToken,
                    timeoutMillis
            );

            // TODO_MA ????????? ?????????  D  ?????????????????????????????????
            // if the registration was successful, let the TaskExecutor know
            // TODO_MA ????????? ????????? ????????????????????????????????????
            CompletableFuture<Void> registrationAcceptFuture = registrationFuture.thenAcceptAsync((RegistrationResponse result) -> {

                /*************************************************
                 * TODO_MA ????????? https://blog.csdn.net/zhongqi2513
                 *  ????????? ??????????????????
                 */
                if (!isCanceled()) {

                    // TODO_MA ????????? ????????? ??????
                    if (result instanceof RegistrationResponse.Success) {
                        log.debug("Registration with {} at {} was successful.", targetName, targetAddress);
                        S success = (S) result;
                        completionFuture.complete(RetryingRegistrationResult.success(gateway, success));
                    }

                    // TODO_MA ????????? ????????? ??????
                    else if (result instanceof RegistrationResponse.Rejection) {
                        log.debug("Registration with {} at {} was rejected.", targetName, targetAddress);
                        R rejection = (R) result;
                        completionFuture.complete(RetryingRegistrationResult.rejection(rejection));
                    }

                    // TODO_MA ????????? ????????? ??????
                    else {
                        // registration failure
                        if (result instanceof RegistrationResponse.Failure) {
                            RegistrationResponse.Failure failure = (RegistrationResponse.Failure) result;
                            log.info("Registration failure at {} occurred.", targetName, failure.getReason());
                        } else {
                            log.error("Received unknown response to registration attempt: {}", result);
                        }

                        log.info("Pausing and re-attempting registration in {} ms",
                                retryingRegistrationConfiguration.getRefusedDelayMillis()
                        );
                        // TODO_MA ????????? ????????? ????????????
                        registerLater(gateway,
                                1,
                                retryingRegistrationConfiguration.getInitialRegistrationTimeoutMillis(),
                                retryingRegistrationConfiguration.getRefusedDelayMillis()
                        );
                    }
                }
            }, rpcService.getScheduledExecutor());

            // upon failure, retry
            // TODO_MA ????????? ????????? ?????????????????????????????????
            registrationAcceptFuture.whenCompleteAsync((Void v, Throwable failure) -> {
                if (failure != null && !isCanceled()) {

                    // TODO_MA ????????? ????????? ?????????????????????????????????....
                    if (ExceptionUtils.stripCompletionException(failure) instanceof TimeoutException) {
                        // we simply have not received a response in time. maybe the timeout
                        // was
                        // very low (initial fast registration attempts), maybe the target
                        // endpoint is
                        // currently down.
                        if (log.isDebugEnabled()) {
                            log.debug("Registration at {} ({}) attempt {} timed out after {} ms",
                                    targetName,
                                    targetAddress,
                                    attempt,
                                    timeoutMillis
                            );
                        }

                        // TODO_MA ????????? ????????? ????????? ???????????? ????????? ????????? = cluster.registration.max-timeout = 30s
                        long newTimeoutMillis = Math.min(2 * timeoutMillis,
                                retryingRegistrationConfiguration.getMaxRegistrationTimeoutMillis()
                        );

                        // TODO_MA ????????? ????????? ???????????? + 1
                        register(gateway, attempt + 1, newTimeoutMillis);
                    } else {
                        // a serious failure occurred. we still should not give up, but keep
                        // trying
                        log.error("Registration at {} failed due to an error", targetName, failure);
                        log.info("Pausing and re-attempting registration in {} ms",
                                retryingRegistrationConfiguration.getErrorDelayMillis()
                        );

                        registerLater(gateway,
                                1,
                                retryingRegistrationConfiguration.getInitialRegistrationTimeoutMillis(),
                                retryingRegistrationConfiguration.getErrorDelayMillis()
                        );
                    }
                }
            }, rpcService.getScheduledExecutor());
        } catch (Throwable t) {
            completionFuture.completeExceptionally(t);
            cancel();
        }
    }

    private void registerLater(final G gateway, final int attempt, final long timeoutMillis, long delay) {
        rpcService.scheduleRunnable(new Runnable() {
            @Override
            public void run() {
                register(gateway, attempt, timeoutMillis);
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    private void startRegistrationLater(final long delay) {
        rpcService.scheduleRunnable(this::startRegistration, delay, TimeUnit.MILLISECONDS);
    }

    static final class RetryingRegistrationResult<G, S, R> {
        @Nullable
        private final G gateway;

        @Nullable
        private final S success;

        @Nullable
        private final R rejection;

        private RetryingRegistrationResult(@Nullable G gateway, @Nullable S success, @Nullable R rejection) {
            this.gateway = gateway;
            this.success = success;
            this.rejection = rejection;
        }

        boolean isSuccess() {
            return success != null && gateway != null;
        }

        boolean isRejection() {
            return rejection != null;
        }

        public G getGateway() {
            Preconditions.checkState(isSuccess());
            return gateway;
        }

        public R getRejection() {
            Preconditions.checkState(isRejection());
            return rejection;
        }

        public S getSuccess() {
            Preconditions.checkState(isSuccess());
            return success;
        }

        static <G extends RpcGateway, S extends RegistrationResponse.Success, R extends RegistrationResponse.Rejection> RetryingRegistrationResult<G, S, R> success(
                G gateway,
                S success) {
            return new RetryingRegistrationResult<>(gateway, success, null);
        }

        static <G extends RpcGateway, S extends RegistrationResponse.Success, R extends RegistrationResponse.Rejection> RetryingRegistrationResult<G, S, R> rejection(
                R rejection) {
            return new RetryingRegistrationResult<>(null, null, rejection);
        }
    }
}
