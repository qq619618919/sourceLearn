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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmaster.SlotRequestId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The provider serves physical slot requests. */
public class PhysicalSlotProviderImpl implements PhysicalSlotProvider {
    private static final Logger LOG = LoggerFactory.getLogger(PhysicalSlotProviderImpl.class);

    private final SlotSelectionStrategy slotSelectionStrategy;

    private final SlotPool slotPool;

    public PhysicalSlotProviderImpl(SlotSelectionStrategy slotSelectionStrategy,
                                    SlotPool slotPool) {
        this.slotSelectionStrategy = checkNotNull(slotSelectionStrategy);
        this.slotPool = checkNotNull(slotPool);
        slotPool.disableBatchSlotRequestTimeoutCheck();
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 此时代码还在 JobMaster 里面
     *  1、先从 slotpool 中申请可用的
     *  2、如果没有可用的，则发送请求给 ResourceManager申请一个新的
     *  Pool 的作用：
     */
    @Override
    public CompletableFuture<PhysicalSlotRequest.Result> allocatePhysicalSlot(PhysicalSlotRequest physicalSlotRequest) {
        SlotRequestId slotRequestId = physicalSlotRequest.getSlotRequestId();
        SlotProfile slotProfile = physicalSlotRequest.getSlotProfile();
        ResourceProfile resourceProfile = slotProfile.getPhysicalSlotResourceProfile();

        LOG.debug("Received slot request [{}] with resource requirements: {}", slotRequestId, resourceProfile);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 第一：先尝试从 slotpool 申请可用的 slot
         *  Some 有
         *  None 没有
         */
        Optional<PhysicalSlot> availablePhysicalSlot = tryAllocateFromAvailable(slotRequestId, slotProfile);

        CompletableFuture<PhysicalSlot> slotFuture;

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 第二，如果申请不到，则从 ResourceManager 处申请新的 Slot
         */
        slotFuture = availablePhysicalSlot.map(CompletableFuture::completedFuture).orElseGet(

                // TODO_MA 马中华 注释： 发送 RPC 请求给 ResourceManager 申请一个新的
                () -> requestNewSlot(slotRequestId, resourceProfile, physicalSlotRequest.willSlotBeOccupiedIndefinitely()));

        return slotFuture.thenApply(physicalSlot -> new PhysicalSlotRequest.Result(slotRequestId, physicalSlot));
    }

    private Optional<PhysicalSlot> tryAllocateFromAvailable(SlotRequestId slotRequestId,
                                                            SlotProfile slotProfile) {
        Collection<SlotSelectionStrategy.SlotInfoAndResources> slotInfoList = slotPool.getAvailableSlotsInformation().stream()
                .map(SlotSelectionStrategy.SlotInfoAndResources::fromSingleSlot).collect(Collectors.toList());

        Optional<SlotSelectionStrategy.SlotInfoAndLocality> selectedAvailableSlot = slotSelectionStrategy.selectBestSlotForProfile(
                slotInfoList, slotProfile);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return selectedAvailableSlot.flatMap(slotInfoAndLocality -> slotPool.allocateAvailableSlot(slotRequestId,
                slotInfoAndLocality.getSlotInfo().getAllocationId(), slotProfile.getPhysicalSlotResourceProfile()));
    }

    private CompletableFuture<PhysicalSlot> requestNewSlot(SlotRequestId slotRequestId,
                                                           ResourceProfile resourceProfile,
                                                           boolean willSlotBeOccupiedIndefinitely) {
        if (willSlotBeOccupiedIndefinitely) {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            return slotPool.requestNewAllocatedSlot(slotRequestId, resourceProfile, null);
        } else {
            return slotPool.requestNewAllocatedBatchSlot(slotRequestId, resourceProfile);
        }
    }

    @Override
    public void cancelSlotRequest(SlotRequestId slotRequestId,
                                  Throwable cause) {
        slotPool.releaseSlot(slotRequestId, cause);
    }
}
