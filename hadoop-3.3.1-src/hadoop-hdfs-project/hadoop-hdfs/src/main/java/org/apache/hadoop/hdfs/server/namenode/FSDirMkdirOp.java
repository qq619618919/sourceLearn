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

import org.apache.hadoop.fs.permission.FsCreateModes;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.security.AccessControlException;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.util.Time.now;

class FSDirMkdirOp {

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 把要创建的那个 文件夹对应的 INode 节点 加入到 FSDirectory 这棵树上
     */
    static FileStatus mkdirs(FSNamesystem fsn, FSPermissionChecker pc, String src, PermissionStatus permissions,
                             boolean createParent) throws IOException {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： HDFS 文件系统的内存数据结构
         */
        FSDirectory fsd = fsn.getFSDirectory();
        if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* NameSystem.mkdirs: " + src);
        }
        fsd.writeLock();
        try {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 根据指定的 src 路径，解析得到 INodesInPath 抽象对象
             *  INodesInPath 内部包含了这个 路径上的所有的 INode 对象
             */
            INodesInPath iip = fsd.resolvePath(pc, src, DirOp.CREATE);
            // TODO_MA 马中华 注释： 生成了一个 节点链
            // TODO_MA 马中华 注释： src = /a/b/c/d

            // TODO_MA 马中华 注释： 获取最后一个 INode 对象
            final INode lastINode = iip.getLastINode();

            // TODO_MA 马中华 注释： 明明是创建文件夹，但是又是有一个同名的文件存在
            if (lastINode != null && lastINode.isFile()) {
                throw new FileAlreadyExistsException("Path is not a directory: " + src);
            }

            // TODO_MA 马中华 注释： 最后一个 INode 为空，我们才去创建，当然是创建文件夹
            if (lastINode == null) {
                if (fsd.isPermissionEnabled()) {
                    fsd.checkAncestorAccess(pc, iip, FsAction.WRITE);
                }

                // TODO_MA 马中华 注释： 是否级联创建父目录
                // TODO_MA 马中华 注释： 如果 createParent 为 false，只判断节点是否存在即可
                if (!createParent) {
                    fsd.verifyParentDir(iip);
                }

                // validate that we have enough inodes. This is, at best, a
                // heuristic because the mkdirs() operation might need to
                // create multiple inodes.
                fsn.checkFsObjectLimit();

                // Ensure that the user can traversal the path by adding implicit
                // u+wx permission to all ancestor directories.
                // TODO_MA 马中华 注释： 通过向所有祖先目录添加隐式 u+wx 权限，确保用户可以遍历路径。
                INodesInPath existing = createParentDirectories(fsd, iip, permissions, false);

                if (existing != null) {
                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 创建文件夹
                     */
                    existing = createSingleDirectory(fsd, existing, iip.getLastLocalName(), permissions);
                }
                if (existing == null) {
                    throw new IOException("Failed to create directory: " + src);
                }
                iip = existing;
            }

            // TODO_MA 马中华 注释： 返回值
            return fsd.getAuditFileInfo(iip);
        } finally {
            fsd.writeUnlock();
        }
    }

    /**
     * For a given absolute path, create all ancestors as directories along the
     * path. All ancestors inherit their parent's permission plus an implicit
     * u+wx permission. This is used by create() and addSymlink() for
     * implicitly creating all directories along the path.
     *
     * For example, path="/foo/bar/spam", "/foo" is an existing directory,
     * "/foo/bar" is not existing yet, the function will create directory bar.
     *
     * @return a INodesInPath with all the existing and newly created
     *         ancestor directories created.
     *         Or return null if there are errors.
     */
    static INodesInPath createAncestorDirectories(FSDirectory fsd, INodesInPath iip,
                                                  PermissionStatus permission) throws IOException {
        return createParentDirectories(fsd, iip, permission, true);
    }

    /**
     * Create all ancestor directories and return the parent inodes.
     *
     * @param fsd FSDirectory
     * @param iip inodes in path to the fs directory
     * @param perm the permission of the directory. Note that all ancestors
     *             created along the path has implicit {@code u+wx} permissions.
     * @param inheritPerms if the ancestor directories should inherit permissions
     *                 or use the specified permissions.
     *
     * @return {@link INodesInPath} which contains all inodes to the
     * target directory, After the execution parentPath points to the path of
     * the returned INodesInPath. The function return null if the operation has
     * failed.
     */
    private static INodesInPath createParentDirectories(FSDirectory fsd, INodesInPath iip, PermissionStatus perm,
                                                        boolean inheritPerms) throws IOException {
        assert fsd.hasWriteLock();
        // this is the desired parent iip if the subsequent delta is 1.
        INodesInPath existing = iip.getExistingINodes();
        int missing = iip.length() - existing.length();
        if (missing == 0) {  // full path exists, return parents.
            existing = iip.getParentINodesInPath();
        } else if (missing > 1) { // need to create at least one ancestor dir.
            // Ensure that the user can traversal the path by adding implicit
            // u+wx permission to all ancestor directories.
            PermissionStatus basePerm = inheritPerms ? existing.getLastINode().getPermissionStatus() : perm;
            perm = addImplicitUwx(basePerm, perm);
            // create all the missing directories.
            final int last = iip.length() - 2;
            for (int i = existing.length(); existing != null && i <= last; i++) {
                byte[] component = iip.getPathComponent(i);
                existing = createSingleDirectory(fsd, existing, component, perm);
            }
        }
        return existing;
    }

    static void mkdirForEditLog(FSDirectory fsd, long inodeId, String src, PermissionStatus permissions,
                                List<AclEntry> aclEntries,
                                long timestamp) throws QuotaExceededException, UnresolvedLinkException, AclException, FileAlreadyExistsException, ParentNotDirectoryException, AccessControlException {
        assert fsd.hasWriteLock();
        INodesInPath iip = fsd.getINodesInPath(src, DirOp.WRITE_LINK);
        final byte[] localName = iip.getLastLocalName();
        final INodesInPath existing = iip.getParentINodesInPath();
        Preconditions.checkState(existing.getLastINode() != null);
        unprotectedMkdir(fsd, inodeId, existing, localName, permissions, aclEntries, timestamp);
    }

    private static INodesInPath createSingleDirectory(FSDirectory fsd, INodesInPath existing, byte[] localName,
                                                      PermissionStatus perm) throws IOException {
        assert fsd.hasWriteLock();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 创建文件夹， 操作内存
         */
        existing = unprotectedMkdir(fsd, fsd.allocateNewInodeId(), existing, localName, perm, null, now());
        if (existing == null) {
            return null;
        }

        // TODO_MA 马中华 注释： 创建好的文件夹
        final INode newNode = existing.getLastINode();

        // Directory creation also count towards FilesCreated
        // to match count of FilesDeleted metric.
        NameNode.getNameNodeMetrics().incrFilesCreated();

        String cur = existing.getPath();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 记录日志，磁盘操作，先记录，把日志写到 输出缓冲上，待会儿还要做 flush
         */
        fsd.getEditLog().logMkDir(cur, newNode);
        // TODO_MA 马中华 注释： FSNameSystem FSImage FSEditLog logMkDir
        // TODO_MA 马中华 注释： FSNameSystem FSImage FSEditLog logRename
        // TODO_MA 马中华 注释： 关于这些  LogXXX 方法的底层实现，一定是同一个 logEdit()

        if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("mkdirs: created directory " + cur);
        }
        return existing;
    }

    private static PermissionStatus addImplicitUwx(PermissionStatus parentPerm, PermissionStatus perm) {
        FsPermission p = parentPerm.getPermission();
        FsPermission ancestorPerm;
        if (p.getUnmasked() == null) {
            ancestorPerm = new FsPermission(p.getUserAction().or(FsAction.WRITE_EXECUTE), p.getGroupAction(),
                    p.getOtherAction()
            );
        } else {
            ancestorPerm = FsCreateModes.create(
                    new FsPermission(p.getUserAction().or(FsAction.WRITE_EXECUTE), p.getGroupAction(), p.getOtherAction()),
                    p.getUnmasked()
            );
        }
        return new PermissionStatus(perm.getUserName(), perm.getGroupName(), ancestorPerm);
    }

    /**
     * create a directory at path specified by parent
     */
    private static INodesInPath unprotectedMkdir(FSDirectory fsd, long inodeId, INodesInPath parent, byte[] name,
                                                 PermissionStatus permission, List<AclEntry> aclEntries,
                                                 long timestamp) throws QuotaExceededException, AclException, FileAlreadyExistsException {
        assert fsd.hasWriteLock();
        assert parent.getLastINode() != null;
        if (!parent.getLastINode().isDirectory()) {
            throw new FileAlreadyExistsException(
                    "Parent path is not a directory: " + parent.getPath() + " " + DFSUtil.bytes2String(name));
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 封装一个文件夹
         */
        final INodeDirectory dir = new INodeDirectory(inodeId, name, permission, timestamp);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        INodesInPath iip = fsd.addLastINode(parent, dir, permission.getPermission(), true);

        if (iip != null && aclEntries != null) {
            AclStorage.updateINodeAcl(dir, aclEntries, Snapshot.CURRENT_STATE_ID);
        }
        return iip;
    }
}

