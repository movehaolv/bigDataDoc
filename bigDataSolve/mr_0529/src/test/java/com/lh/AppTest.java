package com.lh;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException, IOException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://node01:9000"), configuration, "lh");

        // 2 上传文件
        fs.copyFromLocalFile(new Path("d:/jn.py"), new Path("/jn.py"));

        // 3 关闭资源
        fs.close();

        System.out.println("over");
    }
}
