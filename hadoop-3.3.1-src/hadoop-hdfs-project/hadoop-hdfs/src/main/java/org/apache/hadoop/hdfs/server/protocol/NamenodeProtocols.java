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

package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.ReconfigurationProtocol;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.ipc.RefreshCallQueueProtocol;
import org.apache.hadoop.ipc.GenericRefreshProtocol;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.apache.hadoop.tracing.TraceAdminProtocol;

/** The full set of RPC methods implemented by the Namenode.  */
@InterfaceAudience.Private
public interface NamenodeProtocols
      extends ClientProtocol,           // TODO_MA 马中华 注释： Client
              DatanodeProtocol,         // TODO_MA 马中华 注释： DataNode
              DatanodeLifelineProtocol,
              NamenodeProtocol,         // TODO_MA 马中华 注释： SecondaryNameNode
              RefreshAuthorizationPolicyProtocol,
              ReconfigurationProtocol,
              RefreshUserMappingsProtocol,
              RefreshCallQueueProtocol,
              GenericRefreshProtocol,
              GetUserMappingsProtocol,
              HAServiceProtocol,        // TODO_MA 马中华 注释： ZKFC
              TraceAdminProtocol {
}
