package com.atguigu.mr.order.backup;

import com.atguigu.mr.sort.backup.MyPartitioner;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 19:54 2021/1/2
 */


public class OPartitioner extends Partitioner<OBean, NullWritable> {

    @Override
    public int getPartition(OBean oBean, NullWritable nullWritable, int numPartitions) {
        if(oBean.getPrice() > 100 ){
            return 0;
        }else{
            return 1;
        }

    }
}
