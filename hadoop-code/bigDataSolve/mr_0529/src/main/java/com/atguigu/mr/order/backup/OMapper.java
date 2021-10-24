package com.atguigu.mr.order.backup;

import com.atguigu.mr.order.OrderBean;
import com.google.common.collect.Iterators;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 16:03 2021/1/2
 */


public class OMapper extends Mapper<LongWritable, Text, OBean, NullWritable> {

    OBean oBean = new OBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        oBean.setOrderId(fields[0]);
        oBean.setPrice(Double.parseDouble(fields[2]));
        oBean.setId(fields[3]);
        context.write(oBean, NullWritable.get());
    }
}
