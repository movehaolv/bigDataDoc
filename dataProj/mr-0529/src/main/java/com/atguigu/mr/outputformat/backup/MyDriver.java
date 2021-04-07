package com.atguigu.mr.outputformat.backup;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 15:22 2021/1/3
 */


public class MyDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job =Job.getInstance(new Configuration());

        job.setJarByClass(MyDriver.class);

        job.setOutputFormatClass(MyOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("other/log.txt"));
        FileOutputFormat.setOutputPath(job, new Path("output1"));

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }

}
