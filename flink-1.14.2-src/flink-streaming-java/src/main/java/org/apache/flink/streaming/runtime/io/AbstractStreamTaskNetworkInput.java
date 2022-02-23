/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.EndOfChannelStateEvent;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.watermarkstatus.StatusWatermarkValve;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Base class for network-based StreamTaskInput where each channel has a designated {@link
 * RecordDeserializer} for spanning records. Specific implementation bind it to a specific {@link
 * RecordDeserializer}.
 */
public abstract class AbstractStreamTaskNetworkInput<T, R extends RecordDeserializer<DeserializationDelegate<StreamElement>>> implements StreamTaskInput<T> {
    protected final CheckpointedInputGate checkpointedInputGate;
    protected final DeserializationDelegate<StreamElement> deserializationDelegate;
    protected final TypeSerializer<T> inputSerializer;
    protected final Map<InputChannelInfo, R> recordDeserializers;
    protected final Map<InputChannelInfo, Integer> flattenedChannelIndices = new HashMap<>();
    /** Valve that controls how watermarks and watermark statuses are forwarded. */
    protected final StatusWatermarkValve statusWatermarkValve;

    protected final int inputIndex;
    private InputChannelInfo lastChannel = null;
    private R currentRecordDeserializer = null;

    public AbstractStreamTaskNetworkInput(CheckpointedInputGate checkpointedInputGate,
                                          TypeSerializer<T> inputSerializer,
                                          StatusWatermarkValve statusWatermarkValve,
                                          int inputIndex,
                                          Map<InputChannelInfo, R> recordDeserializers) {
        super();
        this.checkpointedInputGate = checkpointedInputGate;
        deserializationDelegate = new NonReusingDeserializationDelegate<>(new StreamElementSerializer<>(inputSerializer));
        this.inputSerializer = inputSerializer;

        for (InputChannelInfo i : checkpointedInputGate.getChannelInfos()) {
            flattenedChannelIndices.put(i, flattenedChannelIndices.size());
        }

        this.statusWatermarkValve = checkNotNull(statusWatermarkValve);
        this.inputIndex = inputIndex;
        this.recordDeserializers = checkNotNull(recordDeserializers);
    }

    @Override
    public DataInputStatus emitNext(DataOutput<T> output) throws Exception {

        while (true) {

            // get the stream element from the deserializer
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            if (currentRecordDeserializer != null) {
                RecordDeserializer.DeserializationResult result;
                try {
                    result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
                } catch (IOException e) {
                    throw new IOException(String.format("Can't get next record for channel %s", lastChannel), e);
                }
                if (result.isBufferConsumed()) {
                    currentRecordDeserializer = null;
                }

                if (result.isFullRecord()) {

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释：
                     */
                    processElement(deserializationDelegate.getInstance(), output);
                    return DataInputStatus.MORE_AVAILABLE;
                }
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            Optional<BufferOrEvent> bufferOrEvent = checkpointedInputGate.pollNext();

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            if (bufferOrEvent.isPresent()) {
                // return to the mailbox after receiving a checkpoint barrier to avoid processing of
                // data after the barrier before checkpoint is performed for unaligned checkpoint mode

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 处理 Buffer
                 */
                if (bufferOrEvent.get().isBuffer()) {
                    processBuffer(bufferOrEvent.get());
                }

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 处理 Event
                 */
                else {
                    return processEvent(bufferOrEvent.get());
                }
            } else {
                if (checkpointedInputGate.isFinished()) {
                    checkState(checkpointedInputGate.getAvailableFuture().isDone(),
                            "Finished BarrierHandler should be available"
                    );
                    return DataInputStatus.END_OF_INPUT;
                }
                return DataInputStatus.NOTHING_AVAILABLE;
            }
        }
    }

    private void processElement(StreamElement recordOrMark, DataOutput<T> output) throws Exception {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 处理数据
         */
        if (recordOrMark.isRecord()) {
            output.emitRecord(recordOrMark.asRecord());
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 处理 Watermark
         */
        else if (recordOrMark.isWatermark()) {
            statusWatermarkValve.inputWatermark(recordOrMark.asWatermark(),
                    flattenedChannelIndices.get(lastChannel),
                    output
            );
        } else if (recordOrMark.isLatencyMarker()) {
            output.emitLatencyMarker(recordOrMark.asLatencyMarker());
        } else if (recordOrMark.isWatermarkStatus()) {
            statusWatermarkValve.inputWatermarkStatus(recordOrMark.asWatermarkStatus(),
                    flattenedChannelIndices.get(lastChannel),
                    output
            );
        } else {
            throw new UnsupportedOperationException("Unknown type of StreamElement");
        }
    }

    protected DataInputStatus processEvent(BufferOrEvent bufferOrEvent) {
        // Event received
        final AbstractEvent event = bufferOrEvent.getEvent();
        if (event.getClass() == EndOfData.class) {
            if (checkpointedInputGate.hasReceivedEndOfData()) {
                return DataInputStatus.END_OF_DATA;
            }
        } else if (event.getClass() == EndOfPartitionEvent.class) {
            // release the record deserializer immediately,
            // which is very valuable in case of bounded stream
            releaseDeserializer(bufferOrEvent.getChannelInfo());
            if (checkpointedInputGate.isFinished()) {
                return DataInputStatus.END_OF_INPUT;
            }
        } else if (event.getClass() == EndOfChannelStateEvent.class) {
            if (checkpointedInputGate.allChannelsRecovered()) {
                return DataInputStatus.END_OF_RECOVERY;
            }
        }
        return DataInputStatus.MORE_AVAILABLE;
    }

    protected void processBuffer(BufferOrEvent bufferOrEvent) throws IOException {
        lastChannel = bufferOrEvent.getChannelInfo();
        checkState(lastChannel != null);

        currentRecordDeserializer = getActiveSerializer(bufferOrEvent.getChannelInfo());
        checkState(currentRecordDeserializer != null, "currentRecordDeserializer has already been released");

        currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
    }

    protected R getActiveSerializer(InputChannelInfo channelInfo) {
        return recordDeserializers.get(channelInfo);
    }

    @Override
    public int getInputIndex() {
        return inputIndex;
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        if (currentRecordDeserializer != null) {
            return AVAILABLE;
        }
        return checkpointedInputGate.getAvailableFuture();
    }

    @Override
    public void close() throws IOException {
        // release the deserializers . this part should not ever fail
        for (InputChannelInfo channelInfo : new ArrayList<>(recordDeserializers.keySet())) {
            releaseDeserializer(channelInfo);
        }
    }

    protected void releaseDeserializer(InputChannelInfo channelInfo) {
        R deserializer = recordDeserializers.get(channelInfo);
        if (deserializer != null) {
            // recycle buffers and clear the deserializer.
            deserializer.clear();
            recordDeserializers.remove(channelInfo);
        }
    }
}
