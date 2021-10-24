package com.atguigu.mr.order.backup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 16:03 2021/1/2
 */


public class ODriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(ODriver.class);
        job.setMapperClass(OMapper.class);
        job.setReducerClass(OReducer.class);

        job.setMapOutputKeyClass(OBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(OBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.setNumLinesPerSplit(job, 5);

//        job.setPartitionerClass(OPartitioner.class);
//        job.setNumReduceTasks(2);

        FileInputFormat.setInputPaths(job, new Path("other/GroupCompartor"));
        FileOutputFormat.setOutputPath(job, new Path("output111"));
        job.setGroupingComparatorClass(OGroupingComparator.class);

        boolean res = job.waitForCompletion(true);

        System.exit(res ? 0 : 1);

    }

}
