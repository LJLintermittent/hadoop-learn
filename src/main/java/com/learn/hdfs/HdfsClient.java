package com.learn.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * Description:
 * date: 2021/11/30 16:56
 * Package: com.learn.hdfs
 *
 * @author 李佳乐
 * @email 18066550996@163.com
 */
@SuppressWarnings("all")
public class HdfsClient {

    private FileSystem fileSystem;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        URI uri = new URI("hdfs://hadoop01:8020");
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "3");
        String user = "lijiale";
        fileSystem = FileSystem.get(uri, configuration, user);
    }

    @After
    public void close() throws IOException {
        fileSystem.close();
    }

    @Test
    public void testMkdir() throws URISyntaxException, IOException, InterruptedException {
        fileSystem.mkdirs(new Path("/java/test6"));
    }

    /**
     * 配置资源优先级：
     * hdfs-default.xml - hdfs-site.xml - 在项目资源路径下（resource）可以建立一个hdfs-site.xml配置文件 - 在代码里面配置
     */
    @Test
    public void testPut() throws IOException {
        fileSystem.copyFromLocalFile(true, true
                , new Path("D:\\hadoop-learn/test.txt")
                , new Path("/"));
    }

    @Test
    public void testGet() throws IOException {
        fileSystem.copyToLocalFile(false, new Path("/java/test2/testPut.txt")
                , new Path("D:\\"), true);
    }

    @Test
    public void testDelete() throws IOException {
        fileSystem.delete(new Path("/sanguo"), true);
    }

    @Test
    public void testMove() throws IOException {
        // rename可以修改文件名称，目录名称，以及移动文件位置并且修改文件名称
        fileSystem.rename(new Path("/sanguo"), new Path("/zhanguo"));
    }

    @Test
    public void testFileDetail() throws IOException {
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/"), true);
        while (iterator.hasNext()) {
            LocatedFileStatus fileStatus = iterator.next();
            System.out.println("当前文件的文件路径：" + fileStatus.getPath());
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));

        }
    }

    @Test
    public void testFileOrCatalog() throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                System.out.println("文件：" + fileStatus.getPath().getName());
            } else {
                System.out.println("目录：" + fileStatus.getPath().getName());
            }
        }

    }

}
