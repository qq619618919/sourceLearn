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

package org.apache.zookeeper.server;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper.common.Time;

/**
 * ExpiryQueue tracks elements in time sorted fixed duration buckets.
 * It's used by SessionTrackerImpl to expire sessions and NIOServerCnxnFactory to expire connections.
 * // TODO_MA 注释： 两个作用：
 * // TODO_MA 注释： 1、SessionTrackerImpl to expire sessions
 * // TODO_MA 注释： 2、NIOServerCnxnFactory to expire connections
 */
public class ExpiryQueue<E> {

    // TODO_MA 注释： E 是管理对象，比如 Session, value Long 是超时时间
    private final ConcurrentHashMap<E, Long> elemMap = new ConcurrentHashMap<E, Long>();

    /**
     * The maximum number of buckets is equal to max timeout/expirationInterval,
     * so the expirationInterval should not be too small compared to the
     * max timeout that this expiry queue needs to maintain.
     * // TODO_MA 注释： 最核心的数据结构 key = 超时时间， value = 需要进行超时处理的一个集合
     */
    private final ConcurrentHashMap<Long, Set<E>> expiryMap = new ConcurrentHashMap<Long, Set<E>>();

    private final AtomicLong nextExpirationTime = new AtomicLong();

    // TODO_MA 注释： 默认 10 s，桶间隔，意味着，每隔 10s 执行一个 Set 的过期
    private final int expirationInterval;

    public ExpiryQueue(int expirationInterval) {
        this.expirationInterval = expirationInterval;
        nextExpirationTime.set(roundToNextInterval(Time.currentElapsedTime()));
    }

    private long roundToNextInterval(long time) {
        return (time / expirationInterval + 1) * expirationInterval;
    }

    /**
     * Removes element from the queue.
     *
     * @param elem element to remove
     * @return time at which the element was set to expire, or null if
     * it wasn't present
     */
    public Long remove(E elem) {
        Long expiryTime = elemMap.remove(elem);
        if(expiryTime != null) {
            Set<E> set = expiryMap.get(expiryTime);
            if(set != null) {
                set.remove(elem);
                // We don't need to worry about removing empty sets,
                // they'll eventually be removed when they expire.
            }
        }
        return expiryTime;
    }

    /**
     * Adds or updates expiration time for element in queue, rounding the
     * timeout to the expiry interval bucketed used by this queue.
     *
     * @param elem    element to add/update
     * @param timeout timout in milliseconds
     * @return time at which the element is now set to expire if
     * changed, or null if unchanged
     */
    public Long update(E elem, int timeout) {

        // TODO_MA 注释： 获取当前 Session elem 的超时时间
        Long prevExpiryTime = elemMap.get(elem);

        // TODO_MA 注释： 计算 它对应的 session bucket
        long now = Time.currentElapsedTime();
        Long newExpiryTime = roundToNextInterval(now + timeout);

        // TODO_MA 注释： 如果前后两次 expireTime 的更新依然处于同一个桶，则不做任何操作
        if(newExpiryTime.equals(prevExpiryTime)) {
            // No change, so nothing to update
            return null;
        }

        // TODO_MA 注释： 根据桶对应的 ExpiryTime 找到 存储会话的 set 集合， 这个 set 集合就是一个所谓的 桶
        // TODO_MA 注释： 所以其实 Session 管理，就是把所有 Session 分散成多个桶。每隔一段时间，对一个桶的所有 Session 执行过期处理
        // First add the elem to the new expiry time bucket in expiryMap.
        Set<E> set = expiryMap.get(newExpiryTime);

        // TODO_MA 注释： 如果桶为空，则创建桶，加入到 expiryMap 中
        if(set == null) {
            // Construct a ConcurrentHashSet using a ConcurrentHashMap
            set = Collections.newSetFromMap(new ConcurrentHashMap<E, Boolean>());
            // Put the new set in the map, but only if another thread
            // hasn't beaten us to it
            Set<E> existingSet = expiryMap.putIfAbsent(newExpiryTime, set);
            if(existingSet != null) {
                set = existingSet;
            }
        }
        // TODO_MA 注释： 将该 Session 加入到新的桶中
        set.add(elem);

        // TODO_MA 注释： 更新 elemMap 中的桶信息
        // Map the elem to the new expiry time. If a different previous
        // mapping was present, clean up the previous expiry bucket.
        prevExpiryTime = elemMap.put(elem, newExpiryTime);
        if(prevExpiryTime != null && !newExpiryTime.equals(prevExpiryTime)) {
            Set<E> prevSet = expiryMap.get(prevExpiryTime);
            if(prevSet != null) {
                prevSet.remove(elem);
            }
        }
        return newExpiryTime;
    }

    /**
     * @return milliseconds until next expiration time, or 0 if has already past
     */
    public long getWaitTime() {
        long now = Time.currentElapsedTime();
        long expirationTime = nextExpirationTime.get();
        return now < expirationTime ? (expirationTime - now) : 0L;
    }

    /**
     * Remove the next expired set of elements from expireMap. This method needs
     * to be called frequently enough by checking getWaitTime(), otherwise there
     * will be a backlog of empty sets queued up in expiryMap.
     *
     * @return next set of expired elements, or an empty set if none are ready
     */
    public Set<E> poll() {
        long now = Time.currentElapsedTime();
        long expirationTime = nextExpirationTime.get();
        if(now < expirationTime) {
            return Collections.emptySet();
        }

        Set<E> set = null;
        long newExpirationTime = expirationTime + expirationInterval;
        if(nextExpirationTime.compareAndSet(expirationTime, newExpirationTime)) {
            set = expiryMap.remove(expirationTime);
        }
        if(set == null) {
            return Collections.emptySet();
        }
        return set;
    }

    public void dump(PrintWriter pwriter) {
        pwriter.print("Sets (");
        pwriter.print(expiryMap.size());
        pwriter.print(")/(");
        pwriter.print(elemMap.size());
        pwriter.println("):");
        ArrayList<Long> keys = new ArrayList<Long>(expiryMap.keySet());
        Collections.sort(keys);
        for(long time : keys) {
            Set<E> set = expiryMap.get(time);
            if(set != null) {
                pwriter.print(set.size());
                pwriter.print(" expire at ");
                pwriter.print(Time.elapsedTimeToDate(time));
                pwriter.println(":");
                for(E elem : set) {
                    pwriter.print("\t");
                    pwriter.println(elem.toString());
                }
            }
        }
    }

    /**
     * Returns an unmodifiable view of the expiration time -&gt; elements mapping.
     */
    public Map<Long, Set<E>> getExpiryMap() {
        return Collections.unmodifiableMap(expiryMap);
    }

}

