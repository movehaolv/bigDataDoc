package com.atguigu.mr.flowsum.backup;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 19:06 2020/12/13
 */


public class FlowMapper extends Mapper<LongWritable, Text, Text,FlowBean> {

    FlowBean flowBean = new FlowBean();
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split("\t");
        Long upFlow = Long.parseLong(fields[fields.length - 3]);
        Long downFlow = Long.parseLong(fields[fields.length - 2]);

        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);

        k.set(fields[1]);

        context.write(k, flowBean);

    }
}
