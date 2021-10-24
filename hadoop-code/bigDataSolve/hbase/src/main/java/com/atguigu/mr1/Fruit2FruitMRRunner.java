package com.atguigu.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 22:54 2021/4/10
 */


public class Fruit2FruitMRRunner extends Configured implements Tool {



    @Override
    public int run(String[] strings) throws Exception {
        //得到 Configuration
        Configuration conf = this.getConf();

        //创建 Job 任务
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(Fruit2FruitMRRunner.class);

        // 配置Job
        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setCaching(500);

       //设置 Mapper，注意导入的是 mapreduce 包下的，不是 mapred 包下的，后者是老版本
        TableMapReduceUtil.initTableMapperJob(
                "fruit", //数据源的表名
                scan, //scan 扫描控制器
                ReadFruitMapper.class,//设置 Mapper 类
                ImmutableBytesWritable.class,//设置 Mapper 输出 key 类型
                Put.class,//设置 Mapper 输出 value 值类型
                job//设置给哪个 JOB
        );
        //设置 Reducer
        TableMapReduceUtil.initTableReducerJob("fruit_mr", WriteFruitMRReducer.class, job);
        //设置 Reduce 数量，最少 1 个
        job.setNumReduceTasks(1);
        boolean isSuccess = job.waitForCompletion(true);
        if(!isSuccess){
            throw new IOException("Job running with error");
        }
        System.out.println("isSuccess" + isSuccess);
        return isSuccess ? 0 : 1;
    }

    public static void main(String[] args) {

        int status = 0;
        try {
            Configuration conf = HBaseConfiguration.create();
//        Configuration conf = new Configuration();
            status = ToolRunner.run(conf, new Fruit2FruitMRRunner(), args);
            System.out.println("status"  +  status);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(status);
    }

}

