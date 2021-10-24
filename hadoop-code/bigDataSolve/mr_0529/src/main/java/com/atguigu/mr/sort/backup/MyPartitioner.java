package com.atguigu.mr.sort.backup;

import org.apache.hadoop.io.Text;


/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 18:48 2020/12/20
 */


public class MyPartitioner extends org.apache.hadoop.mapreduce.Partitioner<FlowBean, Text> {


    @Override
    public int getPartition(FlowBean bean, Text text, int numPartitions) {
        switch (text.toString().substring(0, 3)){
            case "137":
                return 0;
            case "139":
                return 1;
            case "159":
                return 2;
            case "145":
                return 3;
            default:
                return 4;
        }
    }
}
