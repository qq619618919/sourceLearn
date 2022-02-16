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

package org.apache.hadoop.hdfs.server.namenode;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.LayoutFlags;
import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * An implementation of the abstract class {@link EditLogOutputStream}, which stores edits in a local file.
 * // TODO_MA 马中华 注释： 在创建 [edits_inprogress_0000000000000000485] 文件的时候,
 * // TODO_MA 马中华 注释： 首先会用 "-1" 填充 1M 大小的文件空间， 然后将写入的指针归 0。
 * // TODO_MA 马中华 注释： 当有数据的时候，进行写入，写入的时候，会覆盖之前预制填充的数据。
 * // TODO_MA 马中华 注释： 但不管怎么样，如果数据大小不满 1M 的话，那么 edits 文件的大小最小为 1M。
 */
@InterfaceAudience.Private
public class EditLogFileOutputStream extends EditLogOutputStream {
    private static final Logger LOG = LoggerFactory.getLogger(EditLogFileOutputStream.class);
    public static final int MIN_PREALLOCATION_LENGTH = 1024 * 1024;

    // TODO_MA 马中华 注释： 输出流对应的editlog文件。
    private File file;

    // TODO_MA 马中华 注释： editlog文件对应的输出流。
    private FileOutputStream fp; // file stream for storing edit logs

    // TODO_MA 马中华 注释： editlog文件对应的输出流通道。
    private FileChannel fc; // channel of the file stream for sync

    // TODO_MA 马中华 注释： 一个具有两块缓存的缓冲区，数据必须先写入缓存，然后再由缓存同步到磁盘上。
    private EditsDoubleBuffer doubleBuf;

    // TODO_MA 马中华 注释： 用来扩充 editlog 文件大小的数据块。
    // TODO_MA 马中华 注释： 当要进行同步操作时，如果 editlog 文件不够大， 则使用 fill 来扩充 editlog。
    // TODO_MA 马中华 注释： 文件最小 1M
    // TODO_MA 马中华 注释： 一个具有两块缓存的缓冲区， 数据必须先写入缓存， 然后再由缓存同步到磁盘上。
    static final ByteBuffer fill = ByteBuffer.allocateDirect(MIN_PREALLOCATION_LENGTH);

    private boolean shouldSyncWritesAndSkipFsync = false;

    private static boolean shouldSkipFsyncForTests = false;

    // TODO_MA 马中华 注释： 用 OP_INVALID 填满 fill
    static {
        fill.position(0);
        for (int i = 0; i < fill.capacity(); i++) {
            fill.put(FSEditLogOpCodes.OP_INVALID.getOpCode());
        }
    }

    /**
     * Creates output buffers and file object.
     *
     * @param conf
     *          Configuration object
     * @param name
     *          File name to store edit log
     * @param size
     *          Size of flush buffer
     * @throws IOException
     */
    public EditLogFileOutputStream(Configuration conf, File name, int size) throws IOException {
        super();

        // TODO_MA 马中华 注释：
        shouldSyncWritesAndSkipFsync = conf.getBoolean(DFSConfigKeys.DFS_NAMENODE_EDITS_NOEDITLOGCHANNELFLUSH,
                DFSConfigKeys.DFS_NAMENODE_EDITS_NOEDITLOGCHANNELFLUSH_DEFAULT);

        file = name;

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 初始化得到一个双写缓冲管理器
         */
        doubleBuf = new EditsDoubleBuffer(size);

        RandomAccessFile rp;
        if (shouldSyncWritesAndSkipFsync) {
            rp = new RandomAccessFile(name, "rws");
        } else {
            rp = new RandomAccessFile(name, "rw");
        }
        try {
            fp = new FileOutputStream(rp.getFD()); // open for append
        } catch (IOException e) {
            IOUtils.closeStream(rp);
            throw e;
        }
        fc = rp.getChannel();
        fc.position(fc.size());
    }

    @Override
    public void write(FSEditLogOp op) throws IOException {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        doubleBuf.writeOp(op, getCurrentLogVersion());
    }

    /**
     * Write a transaction to the stream. The serialization format is:
     * <ul>
     *   <li>the opcode (byte)</li>
     *   <li>the transaction id (long)</li>
     *   <li>the actual Writables for the transaction</li>
     * </ul>
     * */
    @Override
    public void writeRaw(byte[] bytes, int offset, int length) throws IOException {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        doubleBuf.writeRaw(bytes, offset, length);
    }

    /**
     * Create empty edits logs file.
     */
    @Override
    public void create(int layoutVersion) throws IOException {
        fc.truncate(0);
        fc.position(0);
        writeHeader(layoutVersion, doubleBuf.getCurrentBuf());

        // TODO_MA 马中华 注释： 交换两个缓冲区
        setReadyToFlush();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        flush();

        setCurrentLogVersion(layoutVersion);
    }

    /**
     * Write header information for this EditLogFileOutputStream to the provided
     * DataOutputSream.
     *
     * @param layoutVersion the LayoutVersion of the EditLog
     * @param out the output stream to write the header to.
     * @throws IOException in the event of error writing to the stream.
     */
    @VisibleForTesting
    public static void writeHeader(int layoutVersion, DataOutputStream out) throws IOException {
        out.writeInt(layoutVersion);
        LayoutFlags.write(out);
    }

    @Override
    public void close() throws IOException {
        if (fp == null) {
            throw new IOException("Trying to use aborted output stream");
        }

        try {
            // close should have been called after all pending transactions
            // have been flushed & synced.
            // if already closed, just skip
            if (doubleBuf != null) {
                doubleBuf.close();
                doubleBuf = null;
            }

            // remove any preallocated padding bytes from the transaction log.
            if (fc != null && fc.isOpen()) {
                fc.truncate(fc.position());
                fc.close();
                fc = null;
            }
            fp.close();
            fp = null;
        } finally {
            IOUtils.cleanupWithLogger(LOG, fc, fp);
            doubleBuf = null;
            fc = null;
            fp = null;
        }
        fp = null;
    }

    @Override
    public void abort() throws IOException {
        if (fp == null) {
            return;
        }
        IOUtils.cleanupWithLogger(LOG, fp);
        fp = null;
    }

    /**
     * All data that has been written to the stream so far will be flushed. New
     * data can be still written to the stream while flushing is performed.
     */
    @Override
    public void setReadyToFlush() throws IOException {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        doubleBuf.setReadyToFlush();
    }

    /**
     * // TODO_MA 马中华 注释： 执行 flush，持久化数据到磁盘
     * Flush ready buffer to persistent store. currentBuffer is not flushed as it
     * accumulates new log records while readyBuffer will be flushed and synced.
     */
    @Override
    public void flushAndSync(boolean durable) throws IOException {
        if (fp == null) {
            throw new IOException("Trying to use aborted output stream");
        }
        if (doubleBuf.isFlushed()) {
            LOG.info("Nothing to flush");
            return;
        }
        preallocate(); // preallocate file if necessary

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 将缓存中的，数据同步到 editlog 文件中
         */
        doubleBuf.flushTo(fp);

        if (durable && !shouldSkipFsyncForTests && !shouldSyncWritesAndSkipFsync) {
            fc.force(false); // metadata updates not needed
        }
    }

    /**
     * @return true if the number of buffered data exceeds the intial buffer size
     */
    @Override
    public boolean shouldForceSync() {
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return doubleBuf.shouldForceSync();
    }

    private void preallocate() throws IOException {
        long position = fc.position();
        long size = fc.size();
        int bufSize = doubleBuf.getReadyBuf().getLength();
        long need = bufSize - (size - position);
        if (need <= 0) {
            return;
        }
        long oldSize = size;
        long total = 0;
        long fillCapacity = fill.capacity();
        while (need > 0) {
            fill.position(0);
            IOUtils.writeFully(fc, fill, size);
            need -= fillCapacity;
            size += fillCapacity;
            total += fillCapacity;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Preallocated " + total + " bytes at the end of " + "the edit log (offset " + oldSize + ")");
        }
    }

    /**
     * Returns the file associated with this stream.
     */
    File getFile() {
        return file;
    }

    @Override
    public String toString() {
        return "EditLogFileOutputStream(" + file + ")";
    }

    /**
     * @return true if this stream is currently open.
     */
    public boolean isOpen() {
        return fp != null;
    }

    @VisibleForTesting
    public void setFileChannelForTesting(FileChannel fc) {
        this.fc = fc;
    }

    @VisibleForTesting
    public FileChannel getFileChannelForTesting() {
        return fc;
    }

    /**
     * For the purposes of unit tests, we don't need to actually
     * write durably to disk. So, we can skip the fsync() calls
     * for a speed improvement.
     * @param skip true if fsync should <em>not</em> be called
     */
    @VisibleForTesting
    public static void setShouldSkipFsyncForTesting(boolean skip) {
        shouldSkipFsyncForTests = skip;
    }
}
