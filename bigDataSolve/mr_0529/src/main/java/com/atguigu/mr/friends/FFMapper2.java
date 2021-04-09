package com.atguigu.mr.friends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 11:44 2021/1/10
 */


public class FFMapper2 extends Mapper<LongWritable, Text, Text, Text> {

    Text k = new Text();
    Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        v.set(fields[0]);
        String[] fields1 = fields[1].split(",");
        for(int i=0;i<fields1.length;i++){
            for(int j=i+1;j<fields1.length;j++){
                if(fields1[i].compareTo(fields1[j]) > 0){ // 从小到大存放
                    k.set(fields1[j] + "-" + fields1[i]);
                }else{
                    k.set(fields1[i] + "-" + fields1[j]);
                }
                context.write(k, v);
            }
        }
    }
}
