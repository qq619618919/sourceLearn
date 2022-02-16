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

package org.apache.hadoop.yarn.server.resourcemanager.amlauncher;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释：
 *  ApplicationMasterLauncher 的工作职责，是负责启动 ApplicationMaster
 *  ApplicationMasterLauncher 的工作机制，其实和 AsyncDispatcher 是一样的
 */
public class ApplicationMasterLauncher extends AbstractService implements EventHandler<AMLauncherEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationMasterLauncher.class);

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 线程池 是执行任务的。 队列里面的元素，就是任务 Runnable
     */
    private ThreadPoolExecutor launcherPool;

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 消费队列的线程
     */
    private LauncherThread launcherHandlingThread;

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 队列
     *  Runnable = AMLauncher  AM 的启动器
     */
    private final BlockingQueue<Runnable> masterEvents = new LinkedBlockingQueue<Runnable>();

    protected final RMContext context;

    public ApplicationMasterLauncher(RMContext context) {
        super(ApplicationMasterLauncher.class.getName());
        this.context = context;

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 消费线程初始化
         */
        this.launcherHandlingThread = new LauncherThread();
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        int threadCount = conf.getInt(YarnConfiguration.RM_AMLAUNCHER_THREAD_COUNT,
                YarnConfiguration.DEFAULT_RM_AMLAUNCHER_THREAD_COUNT
        );
        ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat("ApplicationMasterLauncher #%d").build();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 初始化 工作线程池
         */
        launcherPool = new ThreadPoolExecutor(threadCount, threadCount, 1, TimeUnit.HOURS, new LinkedBlockingQueue<Runnable>());
        launcherPool.setThreadFactory(tf);

        Configuration newConf = new YarnConfiguration(conf);
        newConf.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
                conf.getInt(YarnConfiguration.RM_NODEMANAGER_CONNECT_RETRIES,
                        YarnConfiguration.DEFAULT_RM_NODEMANAGER_CONNECT_RETRIES
                )
        );
        setConfig(newConf);
        super.serviceInit(newConf);
    }

    @Override
    protected void serviceStart() throws Exception {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 启动消费线程
         */
        launcherHandlingThread.start();
        super.serviceStart();
    }

    protected Runnable createRunnableLauncher(RMAppAttempt application, AMLauncherEventType event) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        Runnable launcher = new AMLauncher(context, application, event, getConfig());
        return launcher;
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： AML 其实是每次接收到一个事件，AMLauncherEventType.LAUNCH 就封装一个 AMLauncher 的任务
     */
    private void launch(RMAppAttempt application) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 创建一个任务： AMLauncher
         */
        Runnable launcher = createRunnableLauncher(application, AMLauncherEventType.LAUNCH);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 将 AMLauncher 任务加入队列
         */
        masterEvents.add(launcher);
    }


    @Override
    protected void serviceStop() throws Exception {
        launcherHandlingThread.interrupt();
        try {
            launcherHandlingThread.join();
        } catch (InterruptedException ie) {
            LOG.info(launcherHandlingThread.getName() + " interrupted during join ", ie);
        }
        launcherPool.shutdown();
    }

    private class LauncherThread extends Thread {

        public LauncherThread() {
            super("ApplicationMaster Launcher");
        }

        @Override
        public void run() {
            while (!this.isInterrupted()) {
                Runnable toLaunch;
                try {
                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 从队列中取出任务 toLaunch = AMLauncher
                     */
                    toLaunch = masterEvents.take();

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 提交到线程池执行
                     *  AMLauncher 的 run() 就是负责启动 ApplicationMaster 的
                     */
                    launcherPool.execute(toLaunch);

                    // TODO_MA 马中华 注释： 接着去看 AMLauncher 的 run()
                } catch (InterruptedException e) {
                    LOG.warn(this.getClass().getName() + " interrupted. Returning.");
                    return;
                }
            }
        }
    }

    private void cleanup(RMAppAttempt application) {
        Runnable launcher = createRunnableLauncher(application, AMLauncherEventType.CLEANUP);
        masterEvents.add(launcher);
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 如果 RM 接收到了一个 Application
     *  1、构建一个 RMApp 状态机来管理这个 Application 的执行的状态
     *  2、提交一个 START 事件给 AsnycDispatcher ，最终由 ApplicationMasterLancuner 组件来执行处理
     */
    @Override
    public synchronized void handle(AMLauncherEvent appEvent) {
        AMLauncherEventType event = appEvent.getType();
        RMAppAttempt application = appEvent.getAppAttempt();
        switch (event) {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 启动 ApplicationMaster 的入口
             */
            case LAUNCH:
                launch(application);
                break;

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            case CLEANUP:
                cleanup(application);
                break;
            default:
                break;
        }
    }
}
