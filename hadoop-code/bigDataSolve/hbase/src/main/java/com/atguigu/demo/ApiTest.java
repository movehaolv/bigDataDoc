package com.atguigu.demo;
/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 14:21 2021/4/2
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import javax.swing.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ApiTest {
    public static Configuration conf;
    static{
//使用 HBaseConfiguration 的单例方法实例化
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.17.129");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    public static boolean isTableExist(String tableName) throws IOException {
//        Connection connection =ConnectionFactory.createConnection(conf);
//        HBaseAdmin hBaseAdmin = (HBaseAdmin) connection.getAdmin();
        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
        return hBaseAdmin.tableExists(tableName);
    }

    public static void createTable(String tableName, String... columnFamily) throws IOException {
        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
        if(isTableExist(tableName)){
            System.out.println("table  " + tableName  + "  is exists");
        }else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for(String cf: columnFamily){
                hTableDescriptor.addFamily(new HColumnDescriptor(cf));
            }
            hBaseAdmin.createTable(hTableDescriptor);
            System.out.println("表  " + tableName + "  创建成功");
        }

    }

    public static void deleteTable(String tableName) throws IOException {
        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
        if(isTableExist(tableName)){
            hBaseAdmin.disableTable(tableName);
            hBaseAdmin.deleteTable(tableName);
            System.out.println("表  " + tableName + "  删除成功");
        }else{
            System.out.println("表  " + tableName + "  不存在");
        }
    }

    public static void addRowData(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {
        // 向表中插入数据
        ArrayList<Put> objects = new ArrayList<Put>();
        HTable hTable = new HTable(conf, tableName);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        hTable.put(put);
        hTable.close();
        System.out.println("插入数据成功 ");
    }

    public static void deleteMultiRow(String tableName, String... rows) throws IOException {
        // 删除多行数据
        HTable hTable = new HTable(conf, tableName);
        ArrayList<Delete> deleteArrayList = new ArrayList<Delete>();
        for(String row:rows){
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteArrayList.add(delete);
        }
        hTable.delete(deleteArrayList);
        hTable.close();
    }

    public static void getAllRows(String tableName) throws IOException {
        // 获取所有数据
        HTable hTable = new HTable(conf, tableName);
        // 1. 普通过滤
        Scan scan = new Scan(Bytes.toBytes("1003_"), Bytes.toBytes("1003|"));

        // 2 使用Filter
//        Scan scan = new Scan();
//        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.NO_OP.EQUAL, new SubstringComparator("b_"));
//        scan.setFilter(rowFilter);

        ResultScanner scanner = hTable.getScanner(scan);
        for(Result result: scanner){
            Cell[] cells = result.rawCells();
            for(Cell cell:cells){
                System.out.print("行 键 " +  Bytes.toString(CellUtil.cloneRow(cell)) + "  ");
                System.out.print("列 族 " + Bytes.toString(CellUtil.cloneFamily(cell)) + "  ");
                System.out.print("列 " + Bytes.toString(CellUtil.cloneQualifier(cell)) + "  ");
                System.out.println("值 " + Bytes.toString(CellUtil.cloneValue(cell)) + "  ");
            }
        }
    }

    public static void getRow(String tableName, String rowKey) throws IOException {
        // 获取某一行数据
        HTable hTable = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
//        get.setMaxVersions();
//        get.setTimeStamp(new Long("1617519923830"));
        Result result = hTable.get(get);
        Cell[] cells = result.rawCells();
        for(Cell cell:cells){
            System.out.println("行 键 " +  Bytes.toString(CellUtil.cloneRow(cell)));
            System.out.println("列 族 " + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列 " + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值 " + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳：" + cell.getTimestamp());
        }
    }

    public static void getRowQualifier(String tableName, String rowKey, String family, String qualifier) throws IOException {
        // 获取某一行指定“列族:列”的数据
        HTable hTable = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        Result result = hTable.get(get);
        for(Cell cell:result.rawCells()){
            System.out.println("行 键 " +  Bytes.toString(CellUtil.cloneRow(cell)));
            System.out.println("列 族 " + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列 " + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值 " + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳：" + cell.getTimestamp());
        }
    }

    public static void main(String[] args) throws IOException {
//        boolean isExist =  isTableExist("student");
//        createTable("s1", "info1", "info2");
//        deleteTable("s1");
//        addRowData("s", "1008", "info", "joo", "aaa");
//        deleteMultiRow("s1","1002", "1001");
        getAllRows("weibo:content");
//        getRow("weibo:content", "1002_1618742830942");
//        getRowQualifier("s", "1002", "info", "name");

    }

}







