package com.lh;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 17:16 2020/11/29
 */


public class HdfsClient {

    public static void main(String[] args) throws IOException, InterruptedIOException {
        put();
    }

    public static void put() throws IOException, InterruptedIOException {
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://had01:9000"), new Configuration());
        fileSystem.copyFromLocalFile(new Path("D:\\SpringBoot.jar"), new Path("/"));
        fileSystem.close();
        System.out.println(11111);
    }
}
