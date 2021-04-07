package com.atguigu.mr.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 14:44 2021/1/9
 */


public class JoinDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(JoinDriver.class);
        job.setMapperClass(JoinMapper.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);

        job.addCacheFile(URI.create("file:///D:/workLv/learn/proj/mr-0529/other/Join/pd.txt"));

        FileInputFormat.setInputPaths(job, new Path("other/join/order.txt"));
        FileOutputFormat.setOutputPath(job,new Path("out22"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
