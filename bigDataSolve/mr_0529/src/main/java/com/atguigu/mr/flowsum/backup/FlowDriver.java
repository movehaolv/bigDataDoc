package com.atguigu.mr.flowsum.backup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.nio.channels.FileLock;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 19:06 2020/12/13
 */


public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{ "src/main/java/com/atguigu/mr/flowsum/phone_data.txt", "output6" };
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(FlowDriver.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path((args[1])));

        boolean b = job.waitForCompletion(true);

        System.out.println(b ? 1 : 0);

    }
}
