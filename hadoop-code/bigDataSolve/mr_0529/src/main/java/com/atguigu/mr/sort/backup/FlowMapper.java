package com.atguigu.mr.sort.backup;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 17:38 2020/12/20
 */

//  Mapper的输出的key是FlowBean,是因为框架自动会对key排序
public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    FlowBean flowBean =  new FlowBean();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split("\t");

        String phoneNum = fields[0];

        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);
        long sumFlow = Long.parseLong(fields[3]);

        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setSumFlow(sumFlow);
        v.set(phoneNum);

        context.write(flowBean, v);
    }
}
