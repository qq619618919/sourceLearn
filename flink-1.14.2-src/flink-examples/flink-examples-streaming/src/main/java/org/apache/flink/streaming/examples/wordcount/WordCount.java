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

package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.examples.wordcount.util.WordCountData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram over text
 * files in a streaming fashion.
 *
 * <p>The input is a plain text file with lines separated by newline characters.
 *
 * <p>Usage: <code>WordCount --input &lt;path&gt; --output &lt;path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from {@link WordCountData}.
 *
 * <p>This example shows how to:
 *
 * <ul>
 *   <li>write a simple Flink Streaming program,
 *   <li>use tuple data types,
 *   <li>write and use user-defined functions.
 * </ul>
 */
public class WordCount {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    // TODO_MA 马中华 注释： 5 个大步骤
    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

        // set up the execution environment
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 1、spark:  SparkSession (SparkContext)
         *  1、 其实需要关注内部的一个成员变量： List transformatioins
         *  2、 关于配置的解析，checkpoint 有用
         */
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataStream<String> text = null;
        if (params.has("input")) {
            // union all the inputs from text files
            for (String input : params.getMultiParameterRequired("input")) {
                if (text == null) {
                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释：  2、 对接数据源，得到数据抽象对象
                     *  此时此刻并没有读取
                     */
                    text = env.readTextFile(input);
                } else {
                    text = text.union(env.readTextFile(input));
                }
            }
            Preconditions.checkNotNull(text, "Input DataStream should not be null.");
        } else {
            System.out.println("Executing WordCount example with default input data set.");
            System.out.println("Use --input to specify file input.");
            // get default test text data
            text = env.fromElements(WordCountData.WORDS);
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 3、transform 计算 = 调用各种算子，进行链式操作，执行逻辑计算，得到结果数据抽象对象
         */
        DataStream<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new Tokenizer())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .keyBy(value -> value.f0).sum(1);

        // emit result
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 4、处理结果
         */
        if (params.has("output")) {
            counts.writeAsText(params.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            counts.print();
        }
        // execute program
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 5、提交
         */
        env.execute("Streaming WordCount");
    }

    // TODO_MA 马中华 注释： spark 的编程套路也是一样的！

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into multiple pairs in the
     * form of "(word,1)" ({@code Tuple2<String, Integer>}).
     */
    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value,
                            Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
