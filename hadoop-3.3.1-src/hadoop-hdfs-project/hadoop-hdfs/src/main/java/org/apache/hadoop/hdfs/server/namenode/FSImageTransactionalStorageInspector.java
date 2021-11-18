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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;

class FSImageTransactionalStorageInspector extends FSImageStorageInspector {
    public static final Logger LOG = LoggerFactory.getLogger(FSImageTransactionalStorageInspector.class);

    private boolean needToSave = false;
    private boolean isUpgradeFinalized = true;

    final List<FSImageFile> foundImages = new ArrayList<FSImageFile>();
    private long maxSeenTxId = 0;

    // TODO_MA 马中华 注释： 正则集合
    private final List<Pattern> namePatterns = Lists.newArrayList();

    FSImageTransactionalStorageInspector() {
        this(EnumSet.of(NameNodeFile.IMAGE));
    }

    FSImageTransactionalStorageInspector(EnumSet<NameNodeFile> nnfs) {
        for (NameNodeFile nnf : nnfs) {
            // TODO_MA 马中华 注释： 正则表达式  fsimage_*
            Pattern pattern = Pattern.compile(nnf.getName() + "_(\\d+)");
            namePatterns.add(pattern);
        }
    }

    private Matcher matchPattern(String name) {
        for (Pattern p : namePatterns) {
            Matcher m = p.matcher(name);
            if (m.matches()) {
                return m;
            }
        }
        return null;
    }

    @Override
    public void inspectDirectory(StorageDirectory sd) throws IOException {

        // TODO_MA 马中华 注释： 如果不存在 VERSION 文件，直接返回
        // Was the directory just formatted?
        if (!sd.getVersionFile().exists()) {
            LOG.info("No version file in " + sd.getRoot());
            needToSave |= true;
            return;
        }

        // Check for a seen_txid file, which marks a minimum transaction ID that
        // must be included in our load plan.
        try {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 读取到 max seen_txid
             *  当然是从 seen_txid 文件中进行读取
             */
            maxSeenTxId = Math.max(maxSeenTxId, NNStorage.readTransactionIdFile(sd));
        } catch (IOException ioe) {
            LOG.warn("Unable to determine the max transaction ID seen by " + sd, ioe);
            return;
        }

        // TODO_MA 马中华 注释： 扫描 current 目录下的所有文件
        File currentDir = sd.getCurrentDir();
        File filesInStorage[];
        try {
            filesInStorage = FileUtil.listFiles(currentDir);
        } catch (IOException ioe) {
            LOG.warn("Unable to inspect storage directory " + currentDir, ioe);
            return;
        }

        // TODO_MA 马中华 注释： 遍历每个文件，获取到 fsimage 文件
        for (File f : filesInStorage) {
            LOG.debug("Checking file " + f);
            String name = f.getName();

            // Check for fsimage_*
            // TODO_MA 马中华 注释： 正则匹配：fsimage_*
            Matcher imageMatch = this.matchPattern(name);
            if (imageMatch != null) {

                // TODO_MA 马中华 注释： 寻找 fsimage 文件
                if (sd.getStorageDirType().isOfType(NameNodeDirType.IMAGE)) {
                    try {

                        // TODO_MA 马中华 注释： 解析得到 文件名中的 txid
                        long txid = Long.parseLong(imageMatch.group(1));

                        /*************************************************
                         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                         *  注释： 将 fsimage 文件加入到 foundImages 集合中
                         */
                        foundImages.add(new FSImageFile(sd, f, txid));
                    } catch (NumberFormatException nfe) {
                        LOG.error("Image file " + f + " has improperly formatted " + "transaction ID");
                        // skip
                    }
                } else {
                    LOG.warn(
                            "Found image file at " + f + " but storage directory is " + "not configured to contain images.");
                }
            }
        }

        // set finalized flag
        isUpgradeFinalized = isUpgradeFinalized && !sd.getPreviousDir().exists();
    }

    @Override
    public boolean isUpgradeFinalized() {
        return isUpgradeFinalized;
    }

    /**
     * @return the image files that have the most recent associated
     * transaction IDs.  If there are multiple storage directories which
     * contain equal images, we'll return them all.
     *
     * @throws FileNotFoundException if not images are found.
     */
    @Override
    List<FSImageFile> getLatestImages() throws IOException {
        // TODO_MA 马中华 注释： 结果容器
        LinkedList<FSImageFile> ret = new LinkedList<FSImageFile>();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 遍历每个 fsimage 文件， 把具有最大 txid 的 fsimage 文件加入到 ret 中
         */
        for (FSImageFile img : foundImages) {
            if (ret.isEmpty()) {
                ret.add(img);
            } else {
                FSImageFile cur = ret.getFirst();
                if (cur.txId == img.txId) {
                    ret.add(img);
                } else if (cur.txId < img.txId) {
                    // TODO_MA 马中华 注释： 每次都清空再加，所以，其实里面只有一个
                    ret.clear();
                    ret.add(img);
                }
            }
        }

        if (ret.isEmpty()) {
            throw new FileNotFoundException("No valid image files found");
        }

        // TODO_MA 马中华 注释： 返回 ret
        return ret;
    }

    public List<FSImageFile> getFoundImages() {
        return ImmutableList.copyOf(foundImages);
    }

    @Override
    public boolean needToSave() {
        return needToSave;
    }

    @Override
    long getMaxSeenTxId() {
        return maxSeenTxId;
    }
}
