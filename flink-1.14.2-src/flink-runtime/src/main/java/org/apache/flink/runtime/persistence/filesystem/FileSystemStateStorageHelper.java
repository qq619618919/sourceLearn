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

package org.apache.flink.runtime.persistence.filesystem;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.persistence.RetrievableStateStorageHelper;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.state.RetrievableStreamStateHandle;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;

/**
 * {@link RetrievableStateStorageHelper} implementation which stores the state in the given
 * filesystem path.
 *
 * @param <T> The type of the data that can be stored by this storage helper.
 */
public class FileSystemStateStorageHelper<T extends Serializable> implements RetrievableStateStorageHelper<T> {

    private final Path rootPath;

    private final String prefix;

    private final FileSystem fs;

    public FileSystemStateStorageHelper(Path rootPath,
                                        String prefix) throws IOException {
        this.rootPath = Preconditions.checkNotNull(rootPath, "Root path");
        this.prefix = Preconditions.checkNotNull(prefix, "Prefix");

        fs = FileSystem.get(rootPath.toUri());
    }

    @Override
    public RetrievableStateHandle<T> store(T state) throws Exception {
        Exception latestException = null;

        // TODO_MA 马中华 注释： 重试机制
        for (int attempt = 0; attempt < 10; attempt++) {
            Path filePath = getNewFilePath();

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 完成 JobGraph 到 FileSystem 的写入
             */
            try (FSDataOutputStream outStream = fs.create(filePath, FileSystem.WriteMode.NO_OVERWRITE)) {

                // TODO_MA 马中华 注释： 完成写入
                InstantiationUtil.serializeObject(outStream, state);

                // TODO_MA 马中华 注释： 生成一个 StateHandle
                return new RetrievableStreamStateHandle<T>(filePath, outStream.getPos());

            } catch (Exception e) {
                latestException = e;
            }
        }

        // TODO_MA 马中华 注释： 如果 10次 重试机会都用完了，则抛错
        throw new Exception("Could not open output stream for state backend", latestException);
    }

    private Path getNewFilePath() {
        return new Path(rootPath, FileUtils.getRandomFilename(prefix));
    }
}
