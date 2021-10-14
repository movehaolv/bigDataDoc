package com.atguigu.mr.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 14:43 2021/1/9
 */


public class JoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Map<String, String> pdMap = new HashMap<String, String>();
    private Text text = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath().toString();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())){
            String[] fields = line.split("\t");
            pdMap.put(fields[0], fields[1]);
        }
        reader.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split("\t");
        text.set(fields[0] + "\t" + pdMap.get(fields[1]) + "\t" + fields[2]);
        context.write(text, NullWritable.get());

    }
}
