package com.atguigu.mr.test;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.v2.app.MRAppMaster;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;

import java.io.IOException;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 14:26 2020/12/26
 */


public class MyDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);

        job.setJarByClass(MyDriver.class);
        job.setMapperClass(MyMap.class);
//        job.setReducerClass(MyReduce.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(LongWritable.class);
        // 设置输入格式
        job.setInputFormatClass(CombineTextInputFormat.class);
        //如果小文件的总和为224M，将setMaxInputSplitSize中的第二个参数设置成300M的时候，在
        //E:\wordcount\output下只会生成一个part-m-00000这种文件
        //如果将setMaxInputSplitSize中的第二个参数设置成150M的时候，在
        //E:\wordcount\output下会生成part-m-00000 和 part-m-00001 两个文件
        CombineTextInputFormat.setMaxInputSplitSize(job, 1024*1024*150);

        CombineTextInputFormat.setInputPaths(job, new Path("input"));

        FileOutputFormat.setOutputPath(job, new Path("output11"));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);

    }
}
