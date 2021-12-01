#### 分布式文件系统的设计需求

1.透明性：访问透明性，位置透明性，性能和伸缩透明性。

2.并发控制：客户端对于文件的读写不应该影响其他客户端对同一个文件的读写（实现机制：任何时间都只允许有一个程序写入某文件）

3.文件复制：一个文件可以拥有在不同位置的多个副本，HDFS采用了多副本机制

4.硬件和操作系统的异构性：采用java语言开发， 具有很好的跨平台特性

5.可伸缩性：支持节点的动态加入和退出

6.容错：具有多副本机制和故障自动检测，恢复机制

7.安全：保障系统的安全性

#### HDFS局限性

1.不适合低延迟数据的访问，HDFS主要面向大规模数据的批量处理，采用流式数据读取，具有很高的数据吞吐率，但是，这也意味着较高的延迟。对于低延迟要求的应用程序来说，Hbase是一个更好的选择

2.无法高效存储大量小文件，小文件是指文件大小小于一个块的文件，因为namenode会存储文件系统的元数据，这些数据存储在内存中，所以很快的获取文件存储的实际位置，通常，每个文件，目录和块的大小为150字节，如果有大量的小文件，每个文件对应一个块，那么namenode就需要消耗大量的内存来存储，这时元数据检索效率就会下降很多，需要花费很多的时间来找到一个文件的实际存储位置，其次，用MapReduce来处理大量小文件时，会产生过多的map任务，线程管理开销会大大增加

3.不支持多用户写入以及任意修改文件，HDFS只允许一个文件有一个写入者，不允许多个用户对同一个文件执行写操作，而且只允许对文件执行追加操作，不能执行随机写操作

#### HDFS组成架构

##### 1.namenode，就是master

1）管理hdfs的名称空间

2）配置副本策略

3）管理数据块（block）映射信息

4）处理客户端读写请求

##### 2.datanode，就是slave

1）存储实际的数据块

2）执行数据块的读写操作

##### 3.2nn，并不是namenode的热备

1）辅助namenode，分担其工作量，比如定期合并Fsimage和Edits，并推送给namenode

2）在紧急情况下，可辅助恢复namenode

Fsimage用于维护文件系统树以及文件树中所有的文件和文件夹的元数据

Edits：操作日志文件中记录了所有针对文件的创建，删除，重命名等操作

### HDFS写入数据的流程

1.客户端通过Distributed FileSystem模块向namenode请求上传文件到HDFS中的某个目录下

2.namenode向客户端响应可以上传文件

3.请求上传第一个block（0-128M），请返回datanode

4.namenode的内存里面保存着所有的元数据，然后根据副本数选择相应数量的节点来存储数据（假设有三个副本，首先会选择本地节点，其次选择其他机架的一个节点，最后再选择其他机架的另一个节点）

5.客户端跟第一个节点请求建立block传输通道，然后datanode1再与datanode2建立通道，以此类推（不是一个datanode写完才发送给下一个datanode，而是当一个datanode接收到数据后，直接发送给下一个，然后自己这边也会开始写数据）

~~~wiki
节点选择问题：
默认实现类BlockPlacementPolicyDefault采用策略：

1、第一个副本:放置在CPU不忙，磁盘不满的节点。会通过isGoodTarget过滤无法存放block的节点。

2、第二个副本:选择一个远程机架(和当前节点不同机架)上的随机节点存放。

3、第三个副本:如果前两个节点不再同一个机架，选择和第二个副本相同机架的随机不同节点。如果前两个节点在同一个机架，再选择一个RemoteRack远程机架的节点。(越近越快)

4、超出三个的其余副本，随机选择节点，chooseRandom()。
~~~

~~~java
多副本节点选择原理：
    
protected Node chooseTargetInOrder(int numOfReplicas, 
                                 Node writer,
                                 final Set<Node> excludedNodes,
                                 final long blocksize,
                                 final int maxNodesPerRack,
                                 final List<DatanodeStorageInfo> results,
                                 final boolean avoidStaleNodes,
                                 final boolean newBlock,
                                 EnumMap<StorageType, Integer> storageTypes)
                                 throws NotEnoughReplicasException {
    // 拿到所有的datanode存储信息的个数，根据这个个数判断有几个datanode（其实是n+1）比如numOfResults为0，表示只有本地模式
    final int numOfResults = results.size();
    if (numOfResults == 0) {
        // 选择本地节点来存储
      DatanodeStorageInfo storageInfo = chooseLocalStorage(writer,
          excludedNodes, blocksize, maxNodesPerRack, results, avoidStaleNodes,
          storageTypes, true);

      writer = (storageInfo != null) ? storageInfo.getDatanodeDescriptor()
                                     : null;

      // 如果设置的副本数为0了，那么方法退出
        if (--numOfReplicas == 0) {
        return writer;
      }
    }
    final DatanodeDescriptor dn0 = results.get(0).getDatanodeDescriptor();
    // 如果numOfResults为1了，表示除了本地外还有一个节点
    if (numOfResults <= 1) {
        // 将这个远程节点作为副本保存block
      chooseRemoteRack(1, dn0, excludedNodes, blocksize, maxNodesPerRack,
          results, avoidStaleNodes, storageTypes);
      if (--numOfReplicas == 0) {
        return writer;
      }
    }
    //三个节点的集群
    if (numOfResults <= 2) {
      final DatanodeDescriptor dn1 = results.get(1).getDatanodeDescriptor();
      if (clusterMap.isOnSameRack(dn0, dn1)) {
        chooseRemoteRack(1, dn0, excludedNodes, blocksize, maxNodesPerRack,
            results, avoidStaleNodes, storageTypes);
      } else if (newBlock){
        chooseLocalRack(dn1, excludedNodes, blocksize, maxNodesPerRack,
            results, avoidStaleNodes, storageTypes);
      } else {
        chooseLocalRack(writer, excludedNodes, blocksize, maxNodesPerRack,
            results, avoidStaleNodes, storageTypes);
      }
      if (--numOfReplicas == 0) {
        return writer;
      }
    }
    chooseRandom(numOfReplicas, NodeBase.ROOT, excludedNodes, blocksize,
        maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    return writer;
  }
~~~

### HDFS读取数据流程

1.客户端通过Distributed FileSystem向NameNode请求下载文件。

2.namenode查询元数据，找到文件块block所在的datanode地址，返回目标文件的元数据

3.挑选一台datanode，（就近选择，其次随机），请求读取数据

4.datanode开始传输数据给客户端，（从磁盘里面读取数据流，以packet为单位做校验）

5.客户端以packet为单位接收，先在本地缓存，然后写入目标文件

#### FSimage和edits

1.FSimage文件：HDFS文件系统元数据的一个永久性检查点，其中包含HDFS文件系统的所有目录和文件inode的序列化信息

2.edits文件：存放HDFS文件系统的所有更新操作的路径，文件系统客户端执行的所有写操作首先会被记录到edits文件中

~~~shell
查看fsimage文件
hdfs oiv -p XML -i fsimage_0000000000000000374 -o /opt/software/fsimage.xml

查看edits文件
hdfs oev -p XML -i edits_0000000000000000373-0000000000000000374 -o /opt/software/edits.xml
~~~

#### datanode工作机制

1.一个数据块在datanode上以文件形式存储在磁盘上，包括两个文件，一个是数据本身，一个是元数据包括数据块的长度，块数据的校验和 ，以及时间戳

2.datanode启动后向namanode注册，通过后，周期性（6小时）的向namenode上报所有块信息

3.心跳每三秒一次，心跳返回带有namenode给datanode的命令

4.超过十分钟+30秒没有收到该datanode的心跳，即为不可用

