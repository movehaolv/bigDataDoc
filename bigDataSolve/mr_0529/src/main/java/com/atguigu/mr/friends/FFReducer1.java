package com.atguigu.mr.friends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 11:44 2021/1/10
 */


public class FFReducer1 extends Reducer<Text, Text, Text, Text> {

    StringBuilder sb = new StringBuilder();
    Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        sb.delete(0, sb.length());
        for(Text value:values){
            sb.append(value.toString()).append(",");
        }
        v.set(sb.toString());
        context.write(key, v);
    }
}
