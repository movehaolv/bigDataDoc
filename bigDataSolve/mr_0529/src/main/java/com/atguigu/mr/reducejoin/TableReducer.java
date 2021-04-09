package com.atguigu.mr.reducejoin;

import com.google.common.collect.Iterators;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 21:29 2021/1/3
 */


public class TableReducer extends Reducer<TableBean, NullWritable, TableBean, NullWritable> {
    @Override
    protected void reduce(TableBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<NullWritable> iterator = values.iterator();
        iterator.next();
        String pName = key.getpName();
        while (iterator.hasNext()){
            iterator.next();
            key.setpName(pName);
            context.write(key, NullWritable.get());
        }
    }
}
