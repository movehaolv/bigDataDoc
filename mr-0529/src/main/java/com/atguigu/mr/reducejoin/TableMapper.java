package com.atguigu.mr.reducejoin;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 21:29 2021/1/3
 */


public class TableMapper extends Mapper<LongWritable, Text, TableBean, NullWritable> {

    TableBean tableBean = new TableBean();
    String filename;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        filename = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        if(filename.equals("order.txt")){
            tableBean.setId(fields[0]);
            tableBean.setPid(fields[1]);
            tableBean.setAmount(Integer.parseInt(fields[2]));
            tableBean.setpName("");
            context.write(tableBean, NullWritable.get());
        }else if(filename.equals("pd.txt")){
            tableBean.setPid(fields[0]);
            tableBean.setpName(fields[1]);
//            System.out.println(fields[1].length()  + "  aaaaa   " + fields[1]);
            tableBean.setId("");
            tableBean.setAmount(0);

            context.write(tableBean, NullWritable.get());
        }

    }
}
