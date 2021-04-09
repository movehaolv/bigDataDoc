package com.atguigu.mr.order.backup;

import com.google.common.collect.Iterators;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 16:03 2021/1/2
 */


public class OReducer extends Reducer<OBean, NullWritable, OBean, NullWritable> {

    @Override
    protected void reduce(OBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
//        context.write(key, NullWritable.get());

        // 取出Top2价格的
//        Iterator<NullWritable> iterator = values.iterator();
//        for(int i=0;i<2;i++){
//            if(iterator.hasNext()){
//                NullWritable next = iterator.next();
//                context.write(key, next);
//            }
//        }

    }
}
