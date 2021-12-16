#### 数据切片与MapTask并行度决定机制

1.一个job的map阶段并行度由客户端在提交job时的切片数决定

2.每一个split切片分配一个maptask并行实例处理

3.默认情况下，切片大小=blocksize

4.切片时不考虑数据集整体，而是逐个针对每一个文件单独切片

通过源码可以发现切片个数决定了未来开启的maptask个数

#### mapreduce_job提交流程

~~~java
public boolean waitForCompletion(boolean verbose
                                   ) throws IOException, InterruptedException,
                                            ClassNotFoundException {
    // job提交流程的入口
    if (state == JobState.DEFINE) {
      submit();
    }
    // 这时候state已经变为了running状态，接下来就是job的监控代码
    if (verbose) {
      monitorAndPrintJob();
    } else {
      // get the completion poll interval from the client.
      int completionPollIntervalMillis = 
        Job.getCompletionPollInterval(cluster.getConf());
      while (!isComplete()) {
        try {
          Thread.sleep(completionPollIntervalMillis);
        } catch (InterruptedException ie) {
        }
      }
    }
    return isSuccessful();
  }
~~~

~~~java
public void submit() 
         throws IOException, InterruptedException, ClassNotFoundException {
    // 再次确定状态是否为define
    ensureState(JobState.DEFINE);
    // 处理新旧API的兼容性问题
    setUseNewAPI();
    // 里面有两个客户端，一个是yarn，一个是local client
    connect();
    final JobSubmitter submitter = 
        getJobSubmitter(cluster.getFileSystem(), cluster.getClient());
    status = ugi.doAs(new PrivilegedExceptionAction<JobStatus>() {
      public JobStatus run() throws IOException, InterruptedException, 
      ClassNotFoundException {
        // 上面拿到客户端以后，接下来就是往集群中提交job任务
        return submitter.submitJobInternal(Job.this, cluster);
      }
    });
    state = JobState.RUNNING;
    LOG.info("The url to track the job: " + getTrackingURL());
   }
~~~

~~~java
private synchronized void connect()
          throws IOException, InterruptedException, ClassNotFoundException {
    if (cluster == null) {
      cluster = 
        ugi.doAs(new PrivilegedExceptionAction<Cluster>() {
                   public Cluster run()
                          throws IOException, InterruptedException, 
                                 ClassNotFoundException {
                     // 创建客户端对象                
                     return new Cluster(getConfiguration());
                   }
                 });
    }
  }
~~~

job提交核心：

~~~java
 JobStatus submitJobInternal(Job job, Cluster cluster) 
  throws ClassNotFoundException, InterruptedException, IOException {

    //validate the jobs output specs 
    checkSpecs(job);

    Configuration conf = job.getConfiguration();
    addMRFrameworkToDistributedCache(conf);

    Path jobStagingArea = JobSubmissionFiles.getStagingDir(cluster, conf);
    //configure the command line options correctly on the submitting dfs
    InetAddress ip = InetAddress.getLocalHost();
    if (ip != null) {
      submitHostAddress = ip.getHostAddress();
      submitHostName = ip.getHostName();
      conf.set(MRJobConfig.JOB_SUBMITHOST,submitHostName);
      conf.set(MRJobConfig.JOB_SUBMITHOSTADDR,submitHostAddress);
    }
    JobID jobId = submitClient.getNewJobID();
    job.setJobID(jobId);
    Path submitJobDir = new Path(jobStagingArea, jobId.toString());
    JobStatus status = null;
    try {
      conf.set(MRJobConfig.USER_NAME,
          UserGroupInformation.getCurrentUser().getShortUserName());
      conf.set("hadoop.http.filter.initializers", 
          "org.apache.hadoop.yarn.server.webproxy.amfilter.AmFilterInitializer");
      conf.set(MRJobConfig.MAPREDUCE_JOB_DIR, submitJobDir.toString());
      LOG.debug("Configuring job " + jobId + " with " + submitJobDir 
          + " as the submit dir");
      // get delegation token for the dir
      TokenCache.obtainTokensForNamenodes(job.getCredentials(),
          new Path[] { submitJobDir }, conf);
      
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
        TokenCache.setShuffleSecretKey(shuffleKey.getEncoded(),
            job.getCredentials());
      }
      if (CryptoUtils.isEncryptedSpillEnabled(conf)) {
        conf.setInt(MRJobConfig.MR_AM_MAX_ATTEMPTS, 1);
        LOG.warn("Max job attempts set to 1 since encrypted intermediate" +
                "data spill is enabled");
      }

      copyAndConfigureFiles(job, submitJobDir);

      Path submitJobFile = JobSubmissionFiles.getJobConfPath(submitJobDir);
      
      // Create the splits for the job
      LOG.debug("Creating splits at " + jtFs.makeQualified(submitJobDir));
      // 获取切片数，决定未来开几个MapTask 
      int maps = writeSplits(job, submitJobDir);
      conf.setInt(MRJobConfig.NUM_MAPS, maps);
      LOG.info("number of splits:" + maps);

      int maxMaps = conf.getInt(MRJobConfig.JOB_MAX_MAP,
          MRJobConfig.DEFAULT_JOB_MAX_MAP);
      if (maxMaps >= 0 && maxMaps < maps) {
        throw new IllegalArgumentException("The number of map tasks " + maps +
            " exceeded limit " + maxMaps);
      }

      // write "queue admins of the queue to which job is being submitted"
      // to job file.
      String queue = conf.get(MRJobConfig.QUEUE_NAME,
          JobConf.DEFAULT_QUEUE_NAME);
      AccessControlList acl = submitClient.getQueueAdmins(queue);
      conf.set(toFullPropertyName(queue,
          QueueACL.ADMINISTER_JOBS.getAclName()), acl.getAclString());

      // removing jobtoken referrals before copying the jobconf to HDFS
      // as the tasks don't need this setting, actually they may break
      // because of it if present as the referral will point to a
      // different job.
      TokenCache.cleanUpTokenReferral(conf);

      if (conf.getBoolean(
          MRJobConfig.JOB_TOKEN_TRACKING_IDS_ENABLED,
          MRJobConfig.DEFAULT_JOB_TOKEN_TRACKING_IDS_ENABLED)) {
        // Add HDFS tracking ids
        ArrayList<String> trackingIds = new ArrayList<String>();
        for (Token<? extends TokenIdentifier> t :
            job.getCredentials().getAllTokens()) {
          trackingIds.add(t.decodeIdentifier().getTrackingId());
        }
        conf.setStrings(MRJobConfig.JOB_TOKEN_TRACKING_IDS,
            trackingIds.toArray(new String[trackingIds.size()]));
      }

      // Set reservation info if it exists
      ReservationId reservationId = job.getReservationId();
      if (reservationId != null) {
        conf.set(MRJobConfig.RESERVATION_ID, reservationId.toString());
      }

      // Write job file to submit dir
      // 会在磁盘中生成job.xml文件，决定着job的所有配置信息 
      writeConf(conf, submitJobFile);
      
      //
      // Now, actually submit the job (using the submit name)
      //
      printTokens(jobId, job.getCredentials());
      status = submitClient.submitJob(
          jobId, submitJobDir.toString(), job.getCredentials());
      if (status != null) {
        return status;
      } else {
        throw new IOException("Could not launch job");
      }
    } finally {
      if (status == null) {
        LOG.info("Cleaning up the staging area " + submitJobDir);
        if (jtFs != null && submitJobDir != null)
          jtFs.delete(submitJobDir, true);

      }
    }
  }
~~~

#### job提交流程源码总结

~~~java
waitForCompletion()

submit();

// 1建立连接
connect();  
// 1）创建提交Job的代理
new Cluster(getConfiguration());
// （1）判断是本地运行环境还是yarn集群运行环境
initialize(jobTrackAddr, conf); 

// 2 提交job
submitter.submitJobInternal(Job.this, cluster)

    // 1）创建给集群提交数据的Stag路径
    Path jobStagingArea = JobSubmissionFiles.getStagingDir(cluster, conf);

// 2）获取jobid ，并创建Job路径
JobID jobId = submitClient.getNewJobID();

    // 3）拷贝jar包到集群
copyAndConfigureFiles(job, submitJobDir);   
    rUploader.uploadFiles(job, jobSubmitDir);

    // 4）计算切片，生成切片规划文件
writeSplits(job, submitJobDir);
        maps = writeNewSplits(job, jobSubmitDir);
        input.getSplits(job);

    // 5）向Stag路径写XML配置文件
writeConf(conf, submitJobFile);
    conf.writeXml(out);

    // 6）提交Job,返回提交状态
status = submitClient.submitJob(jobId, submitJobDir.toString(), job.getCredentials());

~~~

![image](https://cdn.jsdelivr.net/gh/chen-xing/figure_bed_02/cdn/20211216132227465.png)
