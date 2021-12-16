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

#### 切片源码

~~~java
public InputSplit[] getSplits(JobConf job, int numSplits)
    throws IOException {
    StopWatch sw = new StopWatch().start();
    FileStatus[] stats = listStatus(job);

    // Save the number of input files for metrics/loadgen
    job.setLong(NUM_INPUT_FILES, stats.length);
    long totalSize = 0;                           // compute total size
    boolean ignoreDirs = !job.getBoolean(INPUT_DIR_RECURSIVE, false)
      && job.getBoolean(INPUT_DIR_NONRECURSIVE_IGNORE_SUBDIRS, false);

    List<FileStatus> files = new ArrayList<>(stats.length);
    for (FileStatus file: stats) {                // check we have valid files
      if (file.isDirectory()) {
        if (!ignoreDirs) {
          throw new IOException("Not a file: "+ file.getPath());
        }
      } else {
        files.add(file);
        totalSize += file.getLen();
      }
    }

    long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
    long minSize = Math.max(job.getLong(org.apache.hadoop.mapreduce.lib.input.
      FileInputFormat.SPLIT_MINSIZE, 1), minSplitSize);

    // generate splits
    ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
    NetworkTopology clusterMap = new NetworkTopology();
    for (FileStatus file: files) {
      Path path = file.getPath();
      long length = file.getLen();
      if (length != 0) {
        FileSystem fs = path.getFileSystem(job);
        BlockLocation[] blkLocations;
        if (file instanceof LocatedFileStatus) {
          blkLocations = ((LocatedFileStatus) file).getBlockLocations();
        } else {
          blkLocations = fs.getFileBlockLocations(file, 0, length);
        }
        if (isSplitable(fs, path)) {
          long blockSize = file.getBlockSize();
          long splitSize = computeSplitSize(goalSize, minSize, blockSize);

          long bytesRemaining = length;
          while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
            String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations,
                length-bytesRemaining, splitSize, clusterMap);
            splits.add(makeSplit(path, length-bytesRemaining, splitSize,
                splitHosts[0], splitHosts[1]));
            bytesRemaining -= splitSize;
          }

          if (bytesRemaining != 0) {
            String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations, length
                - bytesRemaining, bytesRemaining, clusterMap);
            splits.add(makeSplit(path, length - bytesRemaining, bytesRemaining,
                splitHosts[0], splitHosts[1]));
          }
        } else {
          if (LOG.isDebugEnabled()) {
            // Log only if the file is big enough to be splitted
            if (length > Math.min(file.getBlockSize(), minSize)) {
              LOG.debug("File is not splittable so no parallelization "
                  + "is possible: " + file.getPath());
            }
          }
          String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations,0,length,clusterMap);
          splits.add(makeSplit(path, 0, length, splitHosts[0], splitHosts[1]));
        }
      } else { 
        //Create empty hosts array for zero length files
        splits.add(makeSplit(path, 0, length, new String[0]));
      }
    }
    sw.stop();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Total # of splits generated by getSplits: " + splits.size()
          + ", TimeTaken: " + sw.now(TimeUnit.MILLISECONDS));
    }
    return splits.toArray(new FileSplit[splits.size()]);
  }
~~~

#### 切片源码总结

1.程序先找到数据存储的目录

2.开始遍历处理（规划切片）目录下的每一个文件

3.遍历第一个文件

a.获取文件大小，fs.sizeof(xx.txt)

b.计算切片大小

本地模式下blocksize默认为32M，集群模式下blocksize默认为128M，这个大小可以根据磁盘的传输速度来调整，一般取128M或者256M

注意：每次切片时，都要判断切完剩下的部分是否大于块的1.1倍，不大于1.1倍就划分一块切片

c.将切片信息写入到一个切片规划文件中

d.整个切片的核心过程在getSplit()方法中完成

inputsplit只记录了切片的元数据信息，比如起始位置，长度以及所在的节点列表等

4.提交切片规划文件到yarn上，yarn上的mrappmaster就可以根据切片规划文件计算开启maptask个数

#### 切片机制

框架默认的TextInputFormat切片机制是对任务按文件规划切片，不管文件多小，都会是一个单独的切片，都是交给一个maptask，这样如果有大量的小文件，就会产生大量的maptask，处理效率极其低下

combineTextInputFormat用于小文件过多的场景，他可以将小文件从逻辑上规划到一个切片中，这样，多个小文件就可以交给一个maptask处理

### MapReduce工作流程

![image](https://cdn.jsdelivr.net/gh/chen-xing/figure_bed_02/cdn/20211216142041630.png)

![image](https://cdn.jsdelivr.net/gh/chen-xing/figure_bed_02/cdn/20211216142112513.png)

shuffle过程只是从第七步开始到第16步结束。

### shuffle机制

1.maptask收集我们的map()方法输出的kv对，放到内存缓冲区中

2.从内存缓冲区不断溢出本地磁盘文件，可能会溢出多个文件

3.多个溢出文件会被合并成大的溢出文件

4.在溢出过程以及合并的过程中，都要调用Partitioner进行分区和针对key进行排序

5.reduceTask根据自己的分区号，去各个maptask机器上取相应的结果分区数据

6.reducetask会抓取到同一个分区的来自不同的maptask的结果文件，reducetask会将这些文件再进行合并(归并排序)

7.合并成大文件后，shuffle的过程也就结束了，后面进入reducetask的逻辑运算过程(从文件中取出一个一个的键值对group，调用用户自己定义的reduce方法

shuffle中的缓冲区大小会影响到mapreduce的执行效率， 原则上说，缓冲区越大，磁盘io的次数越少，执行速度就越少

缓冲区的大小可以通过参数调整，参数：mapreduce.task.io.sort.mb默认100M。

### partition分区

场景引出：要求将统计结果按照条件输出到不同文件中(分区)，比如：将统计结果按照手机归属地不同输出到不同的文件

默认分区是hash分区

~~~java
public class HashPartitioner<K2, V2> implements Partitioner<K2, V2> {

  public void configure(JobConf job) {}

  /** Use {@link Object#hashCode()} to partition. */
  public int getPartition(K2 key, V2 value,
                          int numReduceTasks) {
    return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
  }

}
~~~

默认分区是根据key的hashcode对reducetasks个数取模得到的，用户没法控制哪个key存储到哪个分区

#### 自定义partition的步骤

1.自定义类继承Partitioner，重写getPartition方法，来写控制分区的逻辑代码

2.在job驱动中，设置自定义的partitioner

3.自定义partition后，要根据自定义的partitioner的逻辑来设置相应数量的reducetask

```java
job.setNumReduceTasks(2);
```

