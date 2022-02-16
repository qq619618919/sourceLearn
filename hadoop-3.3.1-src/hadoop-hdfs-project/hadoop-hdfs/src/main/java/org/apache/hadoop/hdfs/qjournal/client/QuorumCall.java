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
package org.apache.hadoop.hdfs.qjournal.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.StopWatch;
import org.apache.hadoop.util.Timer;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.FutureCallback;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Futures;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.thirdparty.protobuf.Message;
import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * Represents a set of calls for which a quorum of results is needed.
 * @param <KEY> a key used to identify each of the outgoing calls
 * @param <RESULT> the type of the call result
 */
class QuorumCall<KEY, RESULT> {

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 存储 写入日志成功的 JournalNode 的反馈信息
     */
    private final Map<KEY, RESULT> successes = Maps.newHashMap();

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 存储写入日志 异常的 JourNalNode 信息
     */
    private final Map<KEY, Throwable> exceptions = Maps.newHashMap();

    /**
     * Interval, in milliseconds, at which a log message will be made
     * while waiting for a quorum call.
     */
    private static final int WAIT_PROGRESS_INTERVAL_MILLIS = 1000;

    /**
     * Start logging messages at INFO level periodically after waiting for
     * this fraction of the configured timeout for any call.
     */
    private static final float WAIT_PROGRESS_INFO_THRESHOLD = 0.3f;
    /**
     * Start logging messages at WARN level after waiting for this
     * fraction of the configured timeout for any call.
     */
    private static final float WAIT_PROGRESS_WARN_THRESHOLD = 0.7f;
    private final StopWatch quorumStopWatch;
    private final Timer timer;
    private final List<ListenableFuture<RESULT>> allCalls;

    static <KEY, RESULT> QuorumCall<KEY, RESULT> create(Map<KEY, ? extends ListenableFuture<RESULT>> calls, Timer timer) {

        final QuorumCall<KEY, RESULT> qr = new QuorumCall<KEY, RESULT>(timer);

        for (final Entry<KEY, ? extends ListenableFuture<RESULT>> e : calls.entrySet()) {
            Preconditions.checkArgument(e.getValue() != null, "null future for key: " + e.getKey());
            qr.addCall(e.getValue());
            Futures.addCallback(e.getValue(), new FutureCallback<RESULT>() {
                @Override
                public void onFailure(Throwable t) {
                    // TODO_MA 马中华 注释： 失败的回调
                    qr.addException(e.getKey(), t);
                }

                @Override
                public void onSuccess(RESULT res) {
                    // TODO_MA 马中华 注释： 成功的回调
                    qr.addResult(e.getKey(), res);
                }
            }, MoreExecutors.directExecutor());
        }
        return qr;
    }

    static <KEY, RESULT> QuorumCall<KEY, RESULT> create(Map<KEY, ? extends ListenableFuture<RESULT>> calls) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return create(calls, new Timer());
    }

    /**
     * Not intended for outside use.
     */
    private QuorumCall() {
        this(new Timer());
    }

    private QuorumCall(Timer timer) {
        // Only instantiated from factory method above
        this.timer = timer;
        this.quorumStopWatch = new StopWatch(timer);
        this.allCalls = new ArrayList<>();
    }

    private void addCall(ListenableFuture<RESULT> call) {
        allCalls.add(call);
    }

    /**
     * Used in conjunction with {@link #getQuorumTimeoutIncreaseMillis(long, int)}
     * to check for pauses.
     */
    private void restartQuorumStopWatch() {
        quorumStopWatch.reset().start();
    }

    /**
     * // TODO_MA 马中华 注释： 检查自上次以来的暂停（例如 GC）
     * Check for a pause (e.g. GC) since the last time
     * {@link #restartQuorumStopWatch()} was called. If detected, return the
     * length of the pause; else, -1.
     * @param offset Offset the elapsed time by this amount; use if some amount
     *               of pause was expected
     * @param millis Total length of timeout in milliseconds
     * @return Length of pause, if detected, else -1
     */
    private long getQuorumTimeoutIncreaseMillis(long offset, int millis) {

        // TODO_MA 马中华 注释： 秒表记录的时间
        // TODO_MA 马中华 注释： now 方法跟我的getElapsedMillis 方法是一个效果 用来获取秒表的计时时间
        long elapsed = quorumStopWatch.now(TimeUnit.MILLISECONDS);

        // TODO_MA 马中华 注释： 统计 gc 时间
        long pauseTime = elapsed + offset;

        // TODO_MA 马中华 注释： 如果 gc 时间超过 6s ，则输出日志，返回停顿时间
        // TODO_MA 马中华 注释： 如果 秒表的计时时间如果超过了 6s 钟，就认为发生了 full GC
        if (pauseTime > (millis * WAIT_PROGRESS_INFO_THRESHOLD)) {
            QuorumJournalManager.LOG.info(
                    "Pause detected while waiting for " + "QuorumCall response; increasing timeout threshold by pause time " + "of " + pauseTime + " ms.");
            return pauseTime;
        } else {
            return -1;
        }
    }


    /**
     * Wait for the quorum to achieve a certain number of responses.
     *
     * Note that, even after this returns, more responses may arrive,
     * causing the return value of other methods in this class to change.
     *
     * @param minResponses return as soon as this many responses have been
     * received, regardless of whether they are successes or exceptions
     * @param minSuccesses return as soon as this many successful (non-exception)
     * responses have been received
     * @param maxExceptions return as soon as this many exception responses
     * have been received. Pass 0 to return immediately if any exception is received.
     * @param millis the number of milliseconds to wait for
     * @throws InterruptedException if the thread is interrupted while waiting
     * @throws TimeoutException if the specified timeout elapses before
     * achieving the desired conditions
     */
    public synchronized void waitFor(int minResponses, int minSuccesses, int maxExceptions, int millis,
                                     String operationName) throws InterruptedException, TimeoutException {

        // TODO_MA 马中华 注释： 此时时间，记录开始时间
        long st = timer.monotonicNow();

        // TODO_MA 马中华 注释： 下次日志打印时间  20s * 0.3 = 6s
        long nextLogTime = st + (long) (millis * WAIT_PROGRESS_INFO_THRESHOLD);

        // TODO_MA 马中华 注释： 超时时间，也就是结束时间
        long et = st + millis;

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 等待过半！
         */
        while (true) {

            /*************************************************
             * TODO_MA_Z 马中华 https://blog.csdn.net/zhongqi2513
             *  重要注释： 秒表开始计时！
             *  stopWatch.start();
             */

            // TODO_MA 马中华 注释： 开启秒表
            restartQuorumStopWatch();
            checkAssertionErrors();

            // TODO_MA 马中华 注释： 统计响应，如果收到所有响应了
            // TODO_MA 马中华 注释： 只有当成功和失败，各占一半，还差最后一票的情况下才走这儿
            if (minResponses > 0 && countResponses() >= minResponses) return;

            // TODO_MA 马中华 注释： 统计成功反馈，成功数量 大于等于 3
            // TODO_MA 马中华 注释： 绝大部分情况下走这儿，正常退出。
            if (minSuccesses > 0 && countSuccesses() >= minSuccesses) return;

            // TODO_MA 马中华 注释： 统计异常反馈，失败数量 大于等于 3
            if (maxExceptions >= 0 && countExceptions() > maxExceptions) return;


            // TODO_MA 马中华 注释： full GC 发生在这儿！
            // TODO_MA 马中华 注释： now - st 的结果就包含了 full GC 的时间


            // TODO_MA 马中华 注释： 如果此时时间大于 nextLogTime 该记录日志了
            long now = timer.monotonicNow();
            if (now > nextLogTime) {

                // TODO_MA 马中华 注释： 计算已经等待了的时间
                long waited = now - st;

                String msg = String.format("Waited %s ms (timeout=%s ms) for a response for %s", waited, millis, operationName);
                if (!successes.isEmpty()) {
                    msg += ". Succeeded so far: [" + Joiner.on(",").join(successes.keySet()) + "]";
                }
                if (!exceptions.isEmpty()) {
                    msg += ". Exceptions so far: [" + getExceptionMapString() + "]";
                }
                if (successes.isEmpty() && exceptions.isEmpty()) {
                    msg += ". No responses yet.";
                }

                // TODO_MA 马中华 注释： 14s 之后，就 warn
                if (waited > millis * WAIT_PROGRESS_WARN_THRESHOLD) {
                    // TODO_MA 马中华 注释： 14 - 20
                    QuorumJournalManager.LOG.warn(msg);
                } else {
                    // TODO_MA 马中华 注释： 14s 内就 info
                    // TODO_MA 马中华 注释： 0-14
                    QuorumJournalManager.LOG.info(msg);
                }

                // TODO_MA 马中华 注释： 等待一秒
                // TODO_MA 马中华 注释： 意思就是说，前 6s 打印一次 info 日志
                // TODO_MA 马中华 注释： 之后每隔 1s 打印一次 info 日志
                // TODO_MA 马中华 注释： 从 14s 开始，就每隔 1s 打印 warn 日志
                // TODO_MA 马中华 注释： 6s: info
                // TODO_MA 马中华 注释： 7s: info
                // TODO_MA 马中华 注释： 8s: info
                // TODO_MA 马中华 注释： 9s: info
                // TODO_MA 马中华 注释： 10s: info
                // TODO_MA 马中华 注释： 11s: info
                // TODO_MA 马中华 注释： 12s: info
                // TODO_MA 马中华 注释： 13s: info
                // TODO_MA 马中华 注释： 14s: info
                // TODO_MA 马中华 注释： 15s: warn
                // TODO_MA 马中华 注释： 16s: warn
                // TODO_MA 马中华 注释： 17s: warn
                // TODO_MA 马中华 注释： 18s: warn
                // TODO_MA 马中华 注释： 19s: warn
                // TODO_MA 马中华 注释： 20s: warn
                nextLogTime = now + WAIT_PROGRESS_INTERVAL_MILLIS;

                // TODO_MA 马中华 注释： HBase 的重试时间
                // TODO_MA 马中华 注释： 1 1 2 4 8 16 32 60 60 60 60 .... 60
            }

            // TODO_MA 马中华 注释： 最终得到的结论：  如果 active namenode 写 journal 如果超时，
            // TODO_MA 马中华 注释： 则 active namenode 关机了！

            // TODO_MA 马中华 注释： 1、正常关机： 真的是 5 个 jounalnode 节点死了 3
            // TODO_MA 马中华 注释： 2、异常关机： 有什么情况可能是一些异常原因导致 JVM 关机呢？

            // TODO_MA 马中华 注释： Full GC

            // TODO_MA 马中华 注释： 假设代码执行到了这儿，突然 JVM 发生了 Full GC
            // TODO_MA 马中华 注释： Active NanemNode 中的所有元数据，都在内存中有一份完整的，50G
            // TODO_MA 马中华 注释： 如果 JVM 发生 Full GC 除了垃圾回收线程在工作以外，其他的所有的工作线程都停止
            // TODO_MA 马中华 注释： 这个现象： STW = stop the world

            // TODO_MA 马中华 注释： full GC = 30s

            // TODO_MA 马中华 注释： 如果超时
            // TODO_MA 马中华 注释： rem 就是还需要等待的时间


            /*************************************************
             * TODO_MA_Z 马中华 https://blog.csdn.net/zhongqi2513
             *  重要注释： 秒表结束
             *  stopWatch.stop()
             *  maybeGCTime = stopWatch.getElaspedTime()
             *  if (maybeGCTime > 1000){
             *      et = et + maybeGCTime;
             *  }
             */

            long rem = et - now;
            if (rem <= 0) {
                // TODO_MA 马中华 注释：  如果在重新启动 stopWatch 后发生 full GC，则增加超时
                // Increase timeout if a full GC occurred after restarting stopWatch
                long timeoutIncrease = getQuorumTimeoutIncreaseMillis(0, millis);
                // TODO_MA 马中华 注释： 增加超时时间，也就是 full GC 的时间不算在20s的等待时间内
                if (timeoutIncrease > 0) {
                    et += timeoutIncrease;
                } else {
                    // TODO_MA 马中华 注释： 抛出 TimeOut 异常
                    throw new TimeoutException();
                }
            }

            // TODO_MA 马中华 注释： 重置秒表
            restartQuorumStopWatch();

            // TODO_MA 马中华 注释： 计算需要等待的时间
            // TODO_MA 马中华 注释： 第一个参数：距离超时的时间
            // TODO_MA 马中华 注释： 第二个参数：距离下一次日志的时间
            rem = Math.min(rem, nextLogTime - now);
            // TODO_MA 马中华 注释： 等待 至少 1s
            rem = Math.max(rem, 1);

            wait(rem);

            // TODO_MA 马中华 注释： 记录 Full GC 时间，延长 et 超时时间
            // Increase timeout if a full GC occurred after restarting stopWatch
            long timeoutIncrease = getQuorumTimeoutIncreaseMillis(-rem, millis);
            if (timeoutIncrease > 0) {
                et += timeoutIncrease;
            }
        }
    }

    /**
     * Cancel any outstanding calls.
     */
    void cancelCalls() {
        for (ListenableFuture<RESULT> call : allCalls) {
            call.cancel(true);
        }
    }

    /**
     * Check if any of the responses came back with an AssertionError.
     * If so, it re-throws it, even if there was a quorum of responses.
     * This code only runs if assertions are enabled for this class,
     * otherwise it should JIT itself away.
     *
     * This is done since AssertionError indicates programmer confusion
     * rather than some kind of expected issue, and thus in the context
     * of test cases we'd like to actually fail the test case instead of
     * continuing through.
     */
    private synchronized void checkAssertionErrors() {
        boolean assertsEnabled = false;
        assert assertsEnabled = true; // sets to true if enabled
        if (assertsEnabled) {
            for (Throwable t : exceptions.values()) {
                if (t instanceof AssertionError) {
                    throw (AssertionError) t;
                } else if (t instanceof RemoteException && ((RemoteException) t).getClassName()
                        .equals(AssertionError.class.getName())) {
                    throw new AssertionError(t);
                }
            }
        }
    }

    private synchronized void addResult(KEY k, RESULT res) {
        successes.put(k, res);
        notifyAll();
    }

    private synchronized void addException(KEY k, Throwable t) {
        exceptions.put(k, t);
        notifyAll();
    }

    /**
     * @return the total number of calls for which a response has been received,
     * regardless of whether it threw an exception or returned a successful
     * result.
     */
    public synchronized int countResponses() {
        return successes.size() + exceptions.size();
    }

    /**
     * @return the number of calls for which a non-exception response has been
     * received.
     */
    public synchronized int countSuccesses() {
        return successes.size();
    }

    /**
     * @return the number of calls for which an exception response has been
     * received.
     */
    public synchronized int countExceptions() {
        return exceptions.size();
    }

    /**
     * @return the map of successful responses. A copy is made such that this
     * map will not be further mutated, even if further results arrive for the
     * quorum.
     */
    public synchronized Map<KEY, RESULT> getResults() {
        return Maps.newHashMap(successes);
    }

    public synchronized void rethrowException(String msg) throws QuorumException {
        Preconditions.checkState(!exceptions.isEmpty());
        throw QuorumException.create(msg, successes, exceptions);
    }

    public static <K> String mapToString(Map<K, ? extends Message> map) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<K, ? extends Message> e : map.entrySet()) {
            if (!first) {
                sb.append("\n");
            }
            first = false;
            sb.append(e.getKey()).append(": ").append(TextFormat.shortDebugString(e.getValue()));
        }
        return sb.toString();
    }

    /**
     * Return a string suitable for displaying to the user, containing
     * any exceptions that have been received so far.
     */
    private String getExceptionMapString() {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<KEY, Throwable> e : exceptions.entrySet()) {
            if (!first) {
                sb.append(", ");
            }
            first = false;
            sb.append(e.getKey()).append(": ").append(e.getValue().getLocalizedMessage());
        }
        return sb.toString();
    }
}
