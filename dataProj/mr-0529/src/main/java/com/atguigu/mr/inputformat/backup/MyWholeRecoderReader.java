package com.atguigu.mr.inputformat.backup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 12:21 2020/12/26
 */


public class MyWholeRecoderReader extends RecordReader<Text, BytesWritable> {

    FileSplit split;
    Boolean notRead = true;
    Configuration configuration;
    Text k = new Text();
    BytesWritable v = new BytesWritable();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
       this.split = (FileSplit) split;
       configuration = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(notRead){

            Path path = split.getPath();
            FileSystem fileSystem = path.getFileSystem(configuration);
            FSDataInputStream open = fileSystem.open(path);

            byte[] buf = new byte[(int) split.getLength()];

            IOUtils.readFully(open, buf, 0, buf.length);

            k.set(path.toString());
            v.set(buf, 0, buf.length);
            IOUtils.closeStream(open);
            notRead = false;
            return true;
        }

        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return v;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }

}
