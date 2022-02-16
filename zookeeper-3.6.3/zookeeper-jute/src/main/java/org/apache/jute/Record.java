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

package org.apache.jute;

import java.io.IOException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Interface that is implemented by generated classes.
 */
@InterfaceAudience.Public
public interface Record {
    void serialize(OutputArchive archive, String tag) throws IOException;
    void deserialize(InputArchive archive, String tag) throws IOException;
}


/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释：  tag 是一个标记
 *  持久化对象/实例到磁盘的时候： 一个 实例 会变成一个抽象的 map
 *  "id" => value
 *  "name" => value
 *  -
 *  将来在阅读 ZKDatabase 冷启动数据恢复的时候，会看到这个工作机制
 */
class Student implements  Record{

    private int id;
    private String name;
    private Student student;

    @Override
    public void serialize(OutputArchive archive, String tag) throws IOException {
        archive.writeInt(id, "");
        archive.writeString(name, "");
    }

    @Override
    public void deserialize(InputArchive archive, String tag) throws IOException {
        this.id = archive.readInt("");
        this.name = archive.readString("");
    }
}


// TODO_MA 马中华 注释： ZK 内部有一个数据库：ZKDataBase
// TODO_MA 马中华 注释： 冷启动的时候，ZKDataBase 需要从 磁盘读取 DataNode 对象恢复到内存中  涉及到反序列化
// TODO_MA 马中华 注释： ZKDataBase  DataTree， 保存一些其他的信息，通过这个标记！
// TODO_MA 马中华 注释： Hadoop SequenceFile (序列化格式的 ： key=value)