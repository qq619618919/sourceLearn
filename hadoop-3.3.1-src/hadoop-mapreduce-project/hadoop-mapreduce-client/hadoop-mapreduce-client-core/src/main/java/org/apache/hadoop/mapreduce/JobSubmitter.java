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
package org.apache.hadoop.mapreduce;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.QueueACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.mapred.QueueManager.toFullPropertyName;

import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.split.JobSplitWriter;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.JsonSerialization;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.ReservationId;

import org.apache.hadoop.thirdparty.com.google.common.base.Charsets;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class JobSubmitter {
    protected static final Logger LOG = LoggerFactory.getLogger(JobSubmitter.class);
    private static final String SHUFFLE_KEYGEN_ALGORITHM = "HmacSHA1";
    private static final int SHUFFLE_KEY_LENGTH = 64;
    private FileSystem jtFs;
    private ClientProtocol submitClient;
    private String submitHostName;
    private String submitHostAddress;

    JobSubmitter(FileSystem submitFs, ClientProtocol submitClient) throws IOException {
        this.submitClient = submitClient;
        this.jtFs = submitFs;
    }

    /**
     * configure the jobconf of the user with the command line options of
     * -libjars, -files, -archives.
     * @param job
     * @throws IOException
     */
    private void copyAndConfigureFiles(Job job, Path jobSubmitDir) throws IOException {
        Configuration conf = job.getConfiguration();
        boolean useWildcards = conf.getBoolean(Job.USE_WILDCARD_FOR_LIBJARS, Job.DEFAULT_USE_WILDCARD_FOR_LIBJARS);
        JobResourceUploader rUploader = new JobResourceUploader(jtFs, useWildcards);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        rUploader.uploadResources(job, jobSubmitDir);

        // Get the working directory. If not set, sets it to filesystem working dir
        // This code has been added so that working directory reset before running
        // the job. This is necessary for backward compatibility as other systems
        // might use the public API JobConf#setWorkingDirectory to reset the working
        // directory.
        job.getWorkingDirectory();
    }

    /**
     * Internal method for submitting jobs to the system.
     *
     * <p>The job submission process involves:
     * <ol>
     *   <li>
     *   Checking the input and output specifications of the job.
     *   </li>
     *   <li>
     *   Computing the {@link InputSplit}s for the job.
     *   </li>
     *   <li>
     *   Setup the requisite accounting information for the
     *   {@link DistributedCache} of the job, if necessary.
     *   </li>
     *   <li>
     *   Copying the job's jar and configuration to the map-reduce system
     *   directory on the distributed file-system.
     *   </li>
     *   <li>
     *   Submitting the job to the <code>JobTracker</code> and optionally
     *   monitoring it's status.
     *   </li>
     * </ol></p>
     * @param job the configuration to submit
     * @param cluster the handle to the Cluster
     * @throws ClassNotFoundException
     * @throws InterruptedException
     * @throws IOException
     */
    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 准备这个应用程序在 YARN 中运行的时候所需要的一切相关资料
     *  job，相关的配置，依赖jar包，...  最终统一上传到 HDFS 的一个临时工作目录
     *  如果将来一个 NodeManager 里面需要运行一个 Task，则从 HDFS 中下载这些东西！
     */
    JobStatus submitJobInternal(Job job, Cluster cluster) throws ClassNotFoundException, InterruptedException, IOException {

        //validate the jobs output specs
        checkSpecs(job);

        Configuration conf = job.getConfiguration();
        addMRFrameworkToDistributedCache(conf);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： HDFS 上的临时存储目录
         */
        Path jobStagingArea = JobSubmissionFiles.getStagingDir(cluster, conf);

        //configure the command line options correctly on the submitting dfs
        InetAddress ip = InetAddress.getLocalHost();
        if (ip != null) {
            submitHostAddress = ip.getHostAddress();
            submitHostName = ip.getHostName();
            conf.set(MRJobConfig.JOB_SUBMITHOST, submitHostName);
            conf.set(MRJobConfig.JOB_SUBMITHOSTADDR, submitHostAddress);
        }
        JobID jobId = submitClient.getNewJobID();
        job.setJobID(jobId);

        // TODO_MA 马中华 注释： 提交路径，完整的 Job 的资料存储路径
        Path submitJobDir = new Path(jobStagingArea, jobId.toString());
        JobStatus status = null;
        try {
            conf.set(MRJobConfig.USER_NAME, UserGroupInformation.getCurrentUser().getShortUserName());
            conf.set("hadoop.http.filter.initializers", "org.apache.hadoop.yarn.server.webproxy.amfilter.AmFilterInitializer");
            conf.set(MRJobConfig.MAPREDUCE_JOB_DIR, submitJobDir.toString());
            LOG.debug("Configuring job " + jobId + " with " + submitJobDir + " as the submit dir");
            // get delegation token for the dir
            TokenCache.obtainTokensForNamenodes(job.getCredentials(), new Path[]{submitJobDir}, conf);

            populateTokenCache(conf, job.getCredentials());

            // generate a secret to authenticate shuffle transfers
            if (TokenCache.getShuffleSecretKey(job.getCredentials()) == null) {
                KeyGenerator keyGen;
                try {
                    keyGen = KeyGenerator.getInstance(SHUFFLE_KEYGEN_ALGORITHM);
                    keyGen.init(SHUFFLE_KEY_LENGTH);
                } catch (NoSuchAlgorithmException e) {
                    throw new IOException("Error generating shuffle secret key", e);
                }
                SecretKey shuffleKey = keyGen.generateKey();
                TokenCache.setShuffleSecretKey(shuffleKey.getEncoded(), job.getCredentials());
            }
            if (CryptoUtils.isEncryptedSpillEnabled(conf)) {
                conf.setInt(MRJobConfig.MR_AM_MAX_ATTEMPTS, 1);
                LOG.warn("Max job attempts set to 1 since encrypted intermediate" + "data spill is enabled");
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 准备相关的依赖文件
             */
            copyAndConfigureFiles(job, submitJobDir);

            Path submitJobFile = JobSubmissionFiles.getJobConfPath(submitJobDir);

            // Create the splits for the job
            LOG.debug("Creating splits at " + jtFs.makeQualified(submitJobDir));
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： MR 程序中的 ReduceTask 是由用户计算逻辑决定的。
             *  但是 MapTask 到底多少用户是不指定的，需要MR自己来计算
             *  MR切片机制:
             *  List<InputSplit> splits = FileInputFormat.getSplits(JobConf job);
             *  maps = splits.size()
             *  一个 InputSplit 就是一个逻辑切片，一个逻辑切片，就是要启动一个 MapTask
             */
            int maps = writeSplits(job, submitJobDir);
            conf.setInt(MRJobConfig.NUM_MAPS, maps);

            LOG.info("number of splits:" + maps);

            int maxMaps = conf.getInt(MRJobConfig.JOB_MAX_MAP, MRJobConfig.DEFAULT_JOB_MAX_MAP);
            if (maxMaps >= 0 && maxMaps < maps) {
                throw new IllegalArgumentException("The number of map tasks " + maps + " exceeded limit " + maxMaps);
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 解析队列
             */
            // write "queue admins of the queue to which job is being submitted" to job file.
            String queue = conf.get(MRJobConfig.QUEUE_NAME, JobConf.DEFAULT_QUEUE_NAME);
            AccessControlList acl = submitClient.getQueueAdmins(queue);
            conf.set(toFullPropertyName(queue, QueueACL.ADMINISTER_JOBS.getAclName()), acl.getAclString());

            // removing jobtoken referrals before copying the jobconf to HDFS
            // as the tasks don't need this setting, actually they may break
            // because of it if present as the referral will point to a
            // different job.
            TokenCache.cleanUpTokenReferral(conf);

            if (conf.getBoolean(MRJobConfig.JOB_TOKEN_TRACKING_IDS_ENABLED, MRJobConfig.DEFAULT_JOB_TOKEN_TRACKING_IDS_ENABLED)) {
                // Add HDFS tracking ids
                ArrayList<String> trackingIds = new ArrayList<String>();
                for (Token<? extends TokenIdentifier> t : job.getCredentials().getAllTokens()) {
                    trackingIds.add(t.decodeIdentifier().getTrackingId());
                }
                conf.setStrings(MRJobConfig.JOB_TOKEN_TRACKING_IDS, trackingIds.toArray(new String[trackingIds.size()]));
            }

            // Set reservation info if it exists
            ReservationId reservationId = job.getReservationId();
            if (reservationId != null) {
                conf.set(MRJobConfig.RESERVATION_ID, reservationId.toString());
            }

            // Write job file to submit dir
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 把 conf 对象中的相关配置，生辰给一个 job.xml 的文件
             *  最终，提交了一个 job.xml 配置文件过去 HDFS 临时工作目录
             */
            writeConf(conf, submitJobFile);

            //
            // Now, actually submit the job (using the submit name)
            //
            printTokens(jobId, job.getCredentials());

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 真实提交
             */
            status = submitClient.submitJob(jobId, submitJobDir.toString(), job.getCredentials());
            if (status != null) {
                return status;
            } else {
                throw new IOException("Could not launch job");
            }
        } finally {
            if (status == null) {
                LOG.info("Cleaning up the staging area " + submitJobDir);
                if (jtFs != null && submitJobDir != null) jtFs.delete(submitJobDir, true);

            }
        }
    }

    private void checkSpecs(Job job) throws ClassNotFoundException, InterruptedException, IOException {
        JobConf jConf = (JobConf) job.getConfiguration();
        // Check the output specification
        if (jConf.getNumReduceTasks() == 0 ? jConf.getUseNewMapper() : jConf.getUseNewReducer()) {
            org.apache.hadoop.mapreduce.OutputFormat<?, ?> output = ReflectionUtils.newInstance(job.getOutputFormatClass(),
                    job.getConfiguration()
            );
            output.checkOutputSpecs(job);
        } else {
            jConf.getOutputFormat().checkOutputSpecs(jtFs, jConf);
        }
    }

    private void writeConf(Configuration conf, Path jobFile) throws IOException {
        // Write job file to JobTracker's fs
        FSDataOutputStream out = FileSystem.create(jtFs, jobFile, new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION));
        try {
            conf.writeXml(out);
        } finally {
            out.close();
        }
    }

    private void printTokens(JobID jobId, Credentials credentials) throws IOException {
        LOG.info("Submitting tokens for job: " + jobId);
        LOG.info("Executing with tokens: {}", credentials.getAllTokens());
    }

    @SuppressWarnings("unchecked")
    private <T extends InputSplit> int writeNewSplits(JobContext job,
                                                      Path jobSubmitDir) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = job.getConfiguration();

        // TODO_MA 马中华 注释： 默认的 InputFormat = TextInputFormat
        // TODO_MA 马中华 注释： TextInputFormat 是 FIleInputFormat 的子类
        InputFormat<?, ?> input = ReflectionUtils.newInstance(job.getInputFormatClass(), conf);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： InputSplit 其实就是一个逻辑切片，对应到一个 MapTask
         */
        List<InputSplit> splits = input.getSplits(job);

        // TODO_MA 马中华 注释： splits 站换乘 array
        T[] array = (T[]) splits.toArray(new InputSplit[splits.size()]);

        // sort the splits into order based on size, so that the biggest
        // go first
        Arrays.sort(array, new SplitComparator());
        JobSplitWriter.createSplitFiles(jobSubmitDir, conf, jobSubmitDir.getFileSystem(conf), array);

        // TODO_MA 马中华 注释： array.length = splits.size()
        return array.length;
    }

    private int writeSplits(org.apache.hadoop.mapreduce.JobContext job,
                            Path jobSubmitDir) throws IOException, InterruptedException, ClassNotFoundException {
        JobConf jConf = (JobConf) job.getConfiguration();
        int maps;
        if (jConf.getUseNewMapper()) {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            maps = writeNewSplits(job, jobSubmitDir);
        } else {
            maps = writeOldSplits(jConf, jobSubmitDir);
        }
        return maps;
    }

    //method to write splits for old api mapper.
    private int writeOldSplits(JobConf job, Path jobSubmitDir) throws IOException {
        org.apache.hadoop.mapred.InputSplit[] splits = job.getInputFormat().getSplits(job, job.getNumMapTasks());
        // sort the splits into order based on size, so that the biggest
        // go first
        Arrays.sort(splits, new Comparator<org.apache.hadoop.mapred.InputSplit>() {
            public int compare(org.apache.hadoop.mapred.InputSplit a, org.apache.hadoop.mapred.InputSplit b) {
                try {
                    long left = a.getLength();
                    long right = b.getLength();
                    if (left == right) {
                        return 0;
                    } else if (left < right) {
                        return 1;
                    } else {
                        return -1;
                    }
                } catch (IOException ie) {
                    throw new RuntimeException("Problem getting input split size", ie);
                }
            }
        });
        JobSplitWriter.createSplitFiles(jobSubmitDir, job, jobSubmitDir.getFileSystem(job), splits);
        return splits.length;
    }

    private static class SplitComparator implements Comparator<InputSplit> {
        @Override
        public int compare(InputSplit o1, InputSplit o2) {
            try {
                long len1 = o1.getLength();
                long len2 = o2.getLength();
                if (len1 < len2) {
                    return 1;
                } else if (len1 == len2) {
                    return 0;
                } else {
                    return -1;
                }
            } catch (IOException ie) {
                throw new RuntimeException("exception in compare", ie);
            } catch (InterruptedException ie) {
                throw new RuntimeException("exception in compare", ie);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void readTokensFromFiles(Configuration conf, Credentials credentials) throws IOException {
        // add tokens and secrets coming from a token storage file
        String binaryTokenFilename = conf.get(MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY);
        if (binaryTokenFilename != null) {
            Credentials binary = Credentials.readTokenStorageFile(
                    FileSystem.getLocal(conf).makeQualified(new Path(binaryTokenFilename)), conf);
            credentials.addAll(binary);
        }
        // add secret keys coming from a json file
        String tokensFileName = conf.get("mapreduce.job.credentials.json");
        if (tokensFileName != null) {
            LOG.info("loading user's secret keys from " + tokensFileName);
            String localFileName = new Path(tokensFileName).toUri().getPath();

            try {
                // read JSON
                Map<String, String> nm = JsonSerialization.mapReader().readValue(new File(localFileName));

                for (Map.Entry<String, String> ent : nm.entrySet()) {
                    credentials.addSecretKey(new Text(ent.getKey()), ent.getValue().getBytes(Charsets.UTF_8));
                }
            } catch (JsonMappingException | JsonParseException e) {
                LOG.warn("couldn't parse Token Cache JSON file with user secret keys");
            }
        }
    }

    //get secret keys and tokens and store them into TokenCache
    private void populateTokenCache(Configuration conf, Credentials credentials) throws IOException {
        readTokensFromFiles(conf, credentials);
        // add the delegation tokens from configuration
        String[] nameNodes = conf.getStrings(MRJobConfig.JOB_NAMENODES);
        LOG.debug("adding the following namenodes' delegation tokens:" + Arrays.toString(nameNodes));
        if (nameNodes != null) {
            Path[] ps = new Path[nameNodes.length];
            for (int i = 0; i < nameNodes.length; i++) {
                ps[i] = new Path(nameNodes[i]);
            }
            TokenCache.obtainTokensForNamenodes(credentials, ps, conf);
        }
    }

    @SuppressWarnings("deprecation")
    private static void addMRFrameworkToDistributedCache(Configuration conf) throws IOException {
        String framework = conf.get(MRJobConfig.MAPREDUCE_APPLICATION_FRAMEWORK_PATH, "");
        if (!framework.isEmpty()) {
            URI uri;
            try {
                uri = new URI(framework);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(
                        "Unable to parse '" + framework + "' as a URI, check the setting for " + MRJobConfig.MAPREDUCE_APPLICATION_FRAMEWORK_PATH,
                        e
                );
            }

            String linkedName = uri.getFragment();

            // resolve any symlinks in the URI path so using a "current" symlink
            // to point to a specific version shows the specific version
            // in the distributed cache configuration
            FileSystem fs = FileSystem.get(uri, conf);
            Path frameworkPath = fs.makeQualified(new Path(uri.getScheme(), uri.getAuthority(), uri.getPath()));
            FileContext fc = FileContext.getFileContext(frameworkPath.toUri(), conf);
            frameworkPath = fc.resolvePath(frameworkPath);
            uri = frameworkPath.toUri();
            try {
                uri = new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(), null, linkedName);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }

            DistributedCache.addCacheArchive(uri, conf);
        }
    }
}
