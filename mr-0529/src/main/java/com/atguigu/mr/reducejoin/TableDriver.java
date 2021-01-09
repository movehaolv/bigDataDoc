package com.atguigu.mr.reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 21:29 2021/1/3
 */


// id pid amount pname 以pid排序，二次排序以pname降序，将pd.txt行在组的首行，利用reduce机制，取出首行并替换组内其余行的pname值
public class TableDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(TableDriver.class);
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReducer.class);

        job.setMapOutputKeyClass(TableBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setGroupingComparatorClass(TableComparator.class);

        FileInputFormat.setInputPaths(job, new Path("other/Join"));
        FileOutputFormat.setOutputPath(job, new Path("out1"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);


    }
}
