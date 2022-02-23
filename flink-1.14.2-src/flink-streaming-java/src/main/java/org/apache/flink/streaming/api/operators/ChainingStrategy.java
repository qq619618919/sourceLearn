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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Defines the chaining scheme for the operator. When an operator is chained to the predecessor, it
 * means that they run in the same thread. They become one operator consisting of multiple steps.
 *
 * <p>The default value used by the StreamOperator is {@link #HEAD}, which means that the operator
 * is not chained to its predecessor. Most operators override this with {@link #ALWAYS}, meaning
 * they will be chained to predecessors whenever possible.
 */
@PublicEvolving
public enum ChainingStrategy {

    /**
     * // TODO_MA 马中华 注释： 只要有可能，操作员就会被急切地锁住。
     * Operators will be eagerly chained whenever possible.
     *
     * <p>To optimize performance, it is generally a good practice to allow maximal chaining and
     * increase operator parallelism.
     */
    ALWAYS,

    /**
     * // TODO_MA 马中华 注释： 运算符不会链接到前面或后面的运算符。
     * The operator will not be chained to the preceding or succeeding operators.
     */
    NEVER,

    /**
     * // TODO_MA 马中华 注释： 运算符不会链接到前任，但后继者可能会链接到此运算符。
     * The operator will not be chained to the predecessor, but successors may chain to this operator.
     */
    HEAD,

    /**
     * // TODO_MA 马中华 注释： 该运算符将在链的头部运行（类似于 {@link HEAD}，但如果可能，它还会尝试链接源输入。
     * This operator will run at the head of a chain (similar as in {@link #HEAD}, but it will
     * additionally try to chain source inputs if possible.
     * // TODO_MA 马中华 注释： 这允许多输入运算符与多个源链接到一个任务中。
     * This allows multi-input operators to be chained with multiple sources into one task.
     */
    HEAD_WITH_SOURCES;

    public static final ChainingStrategy DEFAULT_CHAINING_STRATEGY = ALWAYS;
}
