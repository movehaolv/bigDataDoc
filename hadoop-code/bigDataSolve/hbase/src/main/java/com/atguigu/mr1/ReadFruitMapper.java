package com.atguigu.mr1;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 21:54 2021/4/10
 */

// 将 fruit 表中的一部分数据，通过 MR 迁入到 fruit_mr 表中。
public class ReadFruitMapper extends TableMapper<ImmutableBytesWritable, Put> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //将 fruit 的 name 和 color 提取出来，相当于将每一行数据读取出来放入到 Put 对象中
        Put put = new Put(key.get());

        for(Cell cell:value.rawCells()){
            if("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))){
                if("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                    put.add(cell);
                }else if("color".equals(Bytes.toString(CellUtil.cloneQualifier(cell))) ){
                    put.add(cell);
                }
            }
        }
        context.write(key, put);
    }
}
