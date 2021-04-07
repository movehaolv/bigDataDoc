package com.atguigu.mr.outputformat.backup;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 14:49 2021/1/3
 */


public class MyOutputFormat extends FileOutputFormat<LongWritable, Text> {

    @Override
    public RecordWriter<LongWritable, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        MyRecordWriter recordWriter = new MyRecordWriter();
        recordWriter.initialize(job);
        return recordWriter;
    }
}
