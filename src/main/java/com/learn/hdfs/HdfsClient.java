package com.learn.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

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
                , new Path("D:\\hadoop/testPut.txt")
                , new Path("/java/test3"));
    }

    @Test
    public void testGet() throws IOException {
        fileSystem.copyToLocalFile(false, new Path("/java/test2/testPut.txt")
                , new Path("D:\\"), true);
    }

}
