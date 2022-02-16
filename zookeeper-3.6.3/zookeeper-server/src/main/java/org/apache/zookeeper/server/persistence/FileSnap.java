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

package org.apache.zookeeper.server.persistence;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the snapshot interface.
 * it is responsible for storing, serializing
 * and deserializing the right snapshot.
 * and provides access to the snapshots.
 */
public class FileSnap implements SnapShot {

    File snapDir;
    SnapshotInfo lastSnapshotInfo = null;
    private volatile boolean close = false;
    private static final int VERSION = 2;
    private static final long dbId = -1;
    private static final Logger LOG = LoggerFactory.getLogger(FileSnap.class);
    public static final int SNAP_MAGIC = ByteBuffer.wrap("ZKSN".getBytes()).getInt();

    public static final String SNAPSHOT_FILE_PREFIX = "snapshot";

    public FileSnap(File snapDir) {
        this.snapDir = snapDir;
    }

    /**
     * get information of the last saved/restored snapshot
     *
     * @return info of last snapshot
     */
    public SnapshotInfo getLastSnapshotInfo() {
        return this.lastSnapshotInfo;
    }

    /**
     * deserialize a data tree from the most recent snapshot
     *
     * @return the zxid of the snapshot
     */
    public long deserialize(DataTree dt, Map<Long, Integer> sessions) throws IOException {
        // we run through 100 snapshots (not all of them)
        // if we cannot get it running within 100 snapshots we should  give up
        List<File> snapList = findNValidSnapshots(100);
        if(snapList.size() == 0) {
            return -1L;
        }
        File snap = null;
        long snapZxid = -1;
        boolean foundValid = false;

        // TODO_MA 注释： 遍历快照
        for(int i = 0, snapListSize = snapList.size(); i < snapListSize; i++) {

            // TODO_MA 注释： 获取最近的一次快照的数据文件
            snap = snapList.get(i);

            // TODO_MA 注释： 从快照文件名中，获取 zxid
            LOG.info("Reading snapshot {}", snap);
            snapZxid = Util.getZxidFromName(snap.getName(), SNAPSHOT_FILE_PREFIX);

            try(CheckedInputStream snapIS = SnapStream.getInputStream(snap)) {
                InputArchive ia = BinaryInputArchive.getArchive(snapIS);

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 执行恢复
                 *  1、session持久化了，要恢复
                 *  2、datatree持久化要恢复
                 */
                deserialize(dt, sessions, ia);

                // TODO_MA 注释： 执行 CRC 校验
                SnapStream.checkSealIntegrity(snapIS, ia);
                // Digest feature was added after the CRC to make it backward
                // compatible, the older code can still read snapshots which includes digest.
                // To check the intact, after adding digest we added another CRC check.
                if(dt.deserializeZxidDigest(ia, snapZxid)) {
                    SnapStream.checkSealIntegrity(snapIS, ia);
                }

                // TODO_MA 注释： 恢复 OK ，退出循环
                foundValid = true;
                break;
            } catch(IOException e) {
                LOG.warn("problem reading snap file {}", snap, e);
            }
        }
        if(!foundValid) {
            throw new IOException("Not able to find valid snapshots in " + snapDir);
        }

        // TODO_MA 注释： 最大的 zxid 保存到 datatree 中
        dt.lastProcessedZxid = snapZxid;

        // TODO_MA 注释： 生成一个快照信息
        lastSnapshotInfo = new SnapshotInfo(dt.lastProcessedZxid, snap.lastModified() / 1000);

        // compare the digest if this is not a fuzzy snapshot, we want to compare
        // and find inconsistent asap.
        if(dt.getDigestFromLoadedSnapshot() != null) {
            dt.compareSnapshotDigests(dt.lastProcessedZxid);
        }

        // TODO_MA 注释： 返回一个最大的 zxid
        return dt.lastProcessedZxid;
    }

    /**
     * deserialize the datatree from an inputarchive
     *
     * @param dt       the datatree to be serialized into
     * @param sessions the sessions to be filled up
     * @param ia       the input archive to restore from
     * @throws IOException
     */
    public void deserialize(DataTree dt, Map<Long, Integer> sessions, InputArchive ia) throws IOException {

        // TODO_MA 注释： 先从 fileheader 中读取 SNAP_MAGIC = ZKSN
        FileHeader header = new FileHeader();
        header.deserialize(ia, "fileheader");

        // TODO_MA 注释： 如果 魔法值 被破坏了，证明 快照文件失效了。
        if(header.getMagic() != SNAP_MAGIC) {
            throw new IOException("mismatching magic headers " + header.getMagic() + " !=  " + FileSnap.SNAP_MAGIC);
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        SerializeUtils.deserializeSnapshot(dt, ia, sessions);
    }

    /**
     * find the most recent snapshot in the database.
     *
     * @return the file containing the most recent snapshot
     */
    public File findMostRecentSnapshot() throws IOException {
        List<File> files = findNValidSnapshots(1);
        if(files.size() == 0) {
            return null;
        }
        return files.get(0);
    }

    /**
     * find the last (maybe) valid n snapshots. this does some
     * minor checks on the validity of the snapshots. It just
     * checks for / at the end of the snapshot. This does
     * not mean that the snapshot is truly valid but is
     * valid with a high probability. also, the most recent
     * will be first on the list.
     *
     * @param n the number of most recent snapshots
     * @return the last n snapshots (the number might be
     * less than n in case enough snapshots are not available).
     * @throws IOException
     */
    protected List<File> findNValidSnapshots(int n) throws IOException {

        // TODO_MA 注释： 快照文件降序排序(最近的快照是第一个)
        List<File> files = Util.sortDataDir(snapDir.listFiles(), SNAPSHOT_FILE_PREFIX, false);

        int count = 0;
        List<File> list = new ArrayList<File>();
        for(File f : files) {
            // we should catch the exceptions
            // from the valid snapshot and continue
            // until we find a valid one
            try {

                // TODO_MA 注释：
                if(SnapStream.isValidSnapshot(f)) {
                    list.add(f);
                    count++;

                    // TODO_MA 注释： 找到 100个就退出了。
                    if(count == n) {
                        break;
                    }
                }
            } catch(IOException e) {
                LOG.warn("invalid snapshot {}", f, e);
            }
        }
        return list;
    }

    /**
     * find the last n snapshots. this does not have
     * any checks if the snapshot might be valid or not
     *
     * @param n the number of most recent snapshots
     * @return the last n snapshots
     * @throws IOException
     */
    public List<File> findNRecentSnapshots(int n) throws IOException {
        List<File> files = Util.sortDataDir(snapDir.listFiles(), SNAPSHOT_FILE_PREFIX, false);
        int count = 0;
        List<File> list = new ArrayList<File>();
        for(File f : files) {
            if(count == n) {
                break;
            }
            if(Util.getZxidFromName(f.getName(), SNAPSHOT_FILE_PREFIX) != -1) {
                count++;
                list.add(f);
            }
        }
        return list;
    }

    /**
     * serialize the datatree and sessions
     *
     * @param dt       the datatree to be serialized
     * @param sessions the sessions to be serialized
     * @param oa       the output archive to serialize into
     * @param header   the header of this snapshot
     * @throws IOException
     */
    protected void serialize(DataTree dt, Map<Long, Integer> sessions, OutputArchive oa,
            FileHeader header) throws IOException {
        // this is really a programmatic error and not something that can
        // happen at runtime
        if(header == null) {
            throw new IllegalStateException("Snapshot's not open for writing: uninitialized header");
        }
        header.serialize(oa, "fileheader");
        SerializeUtils.serializeSnapshot(dt, oa, sessions);
    }

    /**
     * serialize the datatree and session into the file snapshot
     *
     * @param dt       the datatree to be serialized
     * @param sessions the sessions to be serialized
     * @param snapShot the file to store snapshot into
     * @param fsync    sync the file immediately after write
     */
    public synchronized void serialize(DataTree dt, Map<Long, Integer> sessions, File snapShot,
            boolean fsync) throws IOException {
        if(!close) {

            // TODO_MA 注释：
            try(CheckedOutputStream snapOS = SnapStream.getOutputStream(snapShot, fsync)) {
                OutputArchive oa = BinaryOutputArchive.getArchive(snapOS);

                // TODO_MA 注释： 构建快照文件头信息
                FileHeader header = new FileHeader(SNAP_MAGIC, VERSION, dbId);

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 拍摄快照
                 */
                serialize(dt, sessions, oa, header);
                SnapStream.sealStream(snapOS, oa);

                // Digest feature was added after the CRC to make it backward
                // compatible, the older code cal still read snapshots which includes digest.
                //
                // To check the intact, after adding digest we added another CRC check.
                if(dt.serializeZxidDigest(oa)) {
                    SnapStream.sealStream(snapOS, oa);
                }

                // TODO_MA 注释： 快照信息
                lastSnapshotInfo = new SnapshotInfo(Util.getZxidFromName(snapShot.getName(), SNAPSHOT_FILE_PREFIX),
                        snapShot.lastModified() / 1000);
            }
        } else {
            throw new IOException("FileSnap has already been closed");
        }
    }

    private void writeChecksum(CheckedOutputStream crcOut, OutputArchive oa) throws IOException {
        long val = crcOut.getChecksum().getValue();
        oa.writeLong(val, "val");
        oa.writeString("/", "path");
    }

    private void checkChecksum(CheckedInputStream crcIn, InputArchive ia) throws IOException {
        long checkSum = crcIn.getChecksum().getValue();
        long val = ia.readLong("val");
        // read and ignore "/" written by writeChecksum
        ia.readString("path");
        if(val != checkSum) {
            throw new IOException("CRC corruption");
        }
    }

    /**
     * synchronized close just so that if serialize is in place
     * the close operation will block and will wait till serialize
     * is done and will set the close flag
     */
    @Override
    public synchronized void close() throws IOException {
        close = true;
    }

}
