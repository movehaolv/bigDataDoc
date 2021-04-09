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


public class FFMapper1 extends Mapper<LongWritable, Text, Text, Text> {

    Text k = new Text();
    Text v = new Text();

    /***  A:B,C /  B:A / C:A
     第一步拆分：
     A -> B
     A -> C
     B -> A
     C -> A
     第二步context.write()
     B,A
     C,A
     A,B
     A,C 经过reduce即可找出A，B,C被谁关注
     */

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(":");
        String[] fields1 = fields[1].split(",");
        for(String field1:fields1){
            k.set(field1); //
            v.set(fields[0]); //
          context.write(k, v);
        }
    }
}
