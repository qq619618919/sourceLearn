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

package org.apache.hadoop.fs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.DataChecksum;
import org.apache.htrace.core.TraceScope;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Checksum;

/**
 * This is a generic output stream for generating checksums for
 * data before it is written to the underlying stream
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
@InterfaceStability.Unstable
abstract public class FSOutputSummer extends OutputStream implements StreamCapabilities {
    // data checksum
    private final DataChecksum sum;
    // internal buffer for storing data before it is checksumed
    private byte buf[];
    // internal buffer for storing checksum
    private byte checksum[];
    // The number of valid bytes in the buffer.
    private int count;

    // TODO_MA 马中华 注释： 我们希望这个值是 3 的倍数，因为本机代码同时校验 3 个块。
    // TODO_MA 马中华 注释： 选择的值 9 在限制 JNI 调用次数和相对频繁地刷新到底层流之间取得了平衡。
    // We want this value to be a multiple of 3 because the native code checksums
    // 3 chunks simultaneously. The chosen value of 9 strikes a balance between
    // limiting the number of JNI calls and flushing to the underlying stream
    // relatively frequently.
    private static final int BUFFER_NUM_CHUNKS = 9;

    protected FSOutputSummer(DataChecksum sum) {
        this.sum = sum;
        // TODO_MA 马中华 注释： 9 倍 512 字节
        this.buf = new byte[sum.getBytesPerChecksum() * BUFFER_NUM_CHUNKS];
        // TODO_MA 马中华 注释： 9 倍 4 字节
        this.checksum = new byte[getChecksumSize() * BUFFER_NUM_CHUNKS];
        this.count = 0;
    }

    /* write the data chunk in <code>b</code> staring at <code>offset</code> with
     * a length of <code>len > 0</code>, and its checksum
     */
    protected abstract void writeChunk(byte[] b, int bOffset, int bLen, byte[] checksum, int checksumOffset,
                                       int checksumLen) throws IOException;

    /**
     * Check if the implementing OutputStream is closed and should no longer
     * accept writes. Implementations should do nothing if this stream is not
     * closed, and should throw an {@link IOException} if it is closed.
     *
     * @throws IOException if this stream is already closed.
     */
    protected abstract void checkClosed() throws IOException;

    /** Write one byte */
    @Override
    public synchronized void write(int b) throws IOException {
        buf[count++] = (byte) b;
        if (count == buf.length) {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            flushBuffer();
        }
    }

    /**
     * Writes <code>len</code> bytes from the specified byte array
     * starting at offset <code>off</code> and generate a checksum for
     * each data chunk.
     *
     * <p> This method stores bytes from the given array into this
     * stream's buffer before it gets checksumed. The buffer gets checksumed
     * and flushed to the underlying output stream when all data
     * in a checksum chunk are in the buffer.  If the buffer is empty and
     * requested length is at least as large as the size of next checksum chunk
     * size, this method will checksum and write the chunk directly
     * to the underlying output stream.  Thus it avoids unnecessary data copy.
     *
     * @param      b     the data.
     * @param      off   the start offset in the data.
     * @param      len   the number of bytes to write.
     * @exception IOException  if an I/O error occurs.
     */
    @Override
    public synchronized void write(byte b[], int off, int len) throws IOException {
        checkClosed();
        if (off < 0 || len < 0 || off > b.length - len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 调用具体实现来进行数据写出
         */
        for (int n = 0; n < len; n += write1(b, off + n, len - n)) {
        }
    }

    /**
     * Write a portion of an array, flushing to the underlying
     * stream at most once if necessary.
     */
    private int write1(byte b[], int off, int len) throws IOException {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 为给定的数据块生成校验和，并将数据块和校验和输出到底层输出流。
         */
        if (count == 0 && len >= buf.length) {
            // local buffer is empty and user buffer size >= local buffer size, so
            // simply checksum the user buffer and send it directly to the underlying stream
            final int length = buf.length;
            writeChecksumChunks(b, off, length);
            return length;
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： copy 数据
         */
        // copy user data to local buffer
        int bytesToCopy = buf.length - count;
        bytesToCopy = (len < bytesToCopy) ? len : bytesToCopy;
        System.arraycopy(b, off, buf, count, bytesToCopy);
        count += bytesToCopy;

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 执行 flush
         */
        if (count == buf.length) {
            // local buffer is full
            flushBuffer();
        }
        return bytesToCopy;
    }

    /* Forces any buffered output bytes to be checksumed and written out to
     * the underlying output stream.
     */
    protected synchronized void flushBuffer() throws IOException {
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        flushBuffer(false, true);
    }

    /* Forces buffered output bytes to be checksummed and written out to
     * the underlying output stream. If there is a trailing partial chunk in the
     * buffer,
     * 1) flushPartial tells us whether to flush that chunk
     * 2) if flushPartial is true, keep tells us whether to keep that chunk in the
     * buffer (if flushPartial is false, it is always kept in the buffer)
     *
     * Returns the number of bytes that were flushed but are still left in the
     * buffer (can only be non-zero if keep is true).
     */
    protected synchronized int flushBuffer(boolean keep, boolean flushPartial) throws IOException {
        int bufLen = count;
        int partialLen = bufLen % sum.getBytesPerChecksum();
        int lenToFlush = flushPartial ? bufLen : bufLen - partialLen;

        if (lenToFlush != 0) {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             *  1、Directory 目录
             *  2、File 文件
             *  3、Block 数据块（存储物理单位）128M
             *  4、Packet 数据包 64kb
             *  5、Chunk 数据块（发送物理单位） 512b + 4b
             */
            writeChecksumChunks(buf, 0, lenToFlush);

            if (!flushPartial || keep) {
                count = partialLen;
                System.arraycopy(buf, bufLen - count, buf, 0, count);
            } else {
                count = 0;
            }
        }

        // total bytes left minus unflushed bytes left
        return count - (bufLen - lenToFlush);
    }

    /**
     * Checksums all complete data chunks and flushes them to the underlying
     * stream. If there is a trailing partial chunk, it is not flushed and is
     * maintained in the buffer.
     */
    public void flush() throws IOException {
        flushBuffer(false, false);
    }

    /**
     * Return the number of valid bytes currently in the buffer.
     */
    protected synchronized int getBufferedDataSize() {
        return count;
    }

    /** @return the size for a checksum. */
    protected int getChecksumSize() {
        return sum.getChecksumSize();
    }

    protected DataChecksum getDataChecksum() {
        return sum;
    }

    protected TraceScope createWriteTraceScope() {
        return null;
    }

    /**
     * Generate checksums for the given data chunks and output chunks & checksums
     * to the underlying output stream.
     * block packet  chunk 在逻辑上来说
     */
    private void writeChecksumChunks(byte b[], int off, int len) throws IOException {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 计算出来校验和信息
         */
        sum.calculateChunkedSums(b, off, len, checksum, 0);
        TraceScope scope = createWriteTraceScope();
        try {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 因为这是一个 for 循环: 积少成多
             *  1、不停的写 chunk
             *  2、写到一定程度就可以构建成一个 packet， 丢到 DataStreamer 的 dataQueue 队列
             *  3、pakcet 写到一定程序，就完成了 128M 的数据传输，也就形成了一个 block
             *  -
             *  在进行第一个 packet 发送的时候：会去判断
             *  1、每次遇到一个新的 packet，如果发现刚好是 128M 整数倍之后的第一个新 packet
             *  2、就得去申请一个 block , 其实 namenode 就会返回一个 datanode 列表
             *  3、client + dn1 + dn2 + dn3 四个角色建立链接，形成 数据传输管道 pipline
             *  4、真正执行 packet 发送
             *  5、不管是 客户端，还是 dn,内部都有发送和接收 ack 的线程
             */
            for (int i = 0; i < len; i += sum.getBytesPerChecksum()) {
                int chunkLen = Math.min(sum.getBytesPerChecksum(), len - i);
                int ckOffset = i / sum.getBytesPerChecksum() * getChecksumSize();

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 不停的写 chunk 直到生成一个 packet
                 *  -
                 *  第一组： 数据的 byte[]， offset， 长度
                 *  1、b
                 *  2、偏移位置
                 *  3、chunkLen
                 *  -
                 *  第二组： 校验信息的 byte[]， offset， 长度
                 *  4、checksum
                 *  5、ckOffset
                 *  6、ChecksumSize
                 */
                writeChunk(b, off + i, chunkLen, checksum, ckOffset, getChecksumSize());
            }
        } finally {
            if (scope != null) {
                scope.close();
            }
        }
    }

    /**
     * Converts a checksum integer value to a byte stream
     */
    static public byte[] convertToByteStream(Checksum sum, int checksumSize) {
        return int2byte((int) sum.getValue(), new byte[checksumSize]);
    }

    static byte[] int2byte(int integer, byte[] bytes) {
        if (bytes.length != 0) {
            bytes[0] = (byte) ((integer >>> 24) & 0xFF);
            bytes[1] = (byte) ((integer >>> 16) & 0xFF);
            bytes[2] = (byte) ((integer >>> 8) & 0xFF);
            bytes[3] = (byte) ((integer >>> 0) & 0xFF);
            return bytes;
        }
        return bytes;
    }

    /**
     * Resets existing buffer with a new one of the specified size.
     */
    protected synchronized void setChecksumBufSize(int size) {
        this.buf = new byte[size];
        this.checksum = new byte[sum.getChecksumSize(size)];
        this.count = 0;
    }

    protected synchronized void resetChecksumBufSize() {
        setChecksumBufSize(sum.getBytesPerChecksum() * BUFFER_NUM_CHUNKS);
    }

    @Override
    public boolean hasCapability(String capability) {
        return false;
    }
}
