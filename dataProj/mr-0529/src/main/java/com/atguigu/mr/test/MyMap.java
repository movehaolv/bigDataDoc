package com.atguigu.mr.test;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 14:26 2020/12/26
 */


public class MyMap extends Mapper<LongWritable, Text, NullWritable, Text> {

    NullWritable k = NullWritable.get();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(k, value);
    }
}
