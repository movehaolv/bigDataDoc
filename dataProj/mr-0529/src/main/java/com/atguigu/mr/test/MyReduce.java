package com.atguigu.mr.test;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 14:26 2020/12/26
 */


public class MyReduce extends Reducer<Text, LongWritable, Text, LongWritable> {

    LongWritable v = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values,	Context context) throws IOException, InterruptedException {

        long sum = 0L;

        // 1 汇总统计
        for (LongWritable value : values) {
            sum += value.get();
        }

        v.set(sum);

        // 2 输出
        context.write(key, v);
    }
}
