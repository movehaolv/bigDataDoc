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
import org.apache.hadoop.hbase.TableDescriptors;
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

public class Test {
    public static Configuration conf;
    static{
    //使用 HBaseConfiguration 的单例方法实例化
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.17.133");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    public static boolean isTableExist(String tableName) throws IOException {
       HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
       return hBaseAdmin.tableExists(TableName.valueOf(tableName));
    }

    public static void createTable(String tableName, String... columnFamily) throws IOException {
        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
        if(isTableExist(tableName)){
            System.out.println("table exists");
        }else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for(String cf:columnFamily){
                hTableDescriptor.addFamily(new HColumnDescriptor(cf));
            }
            hBaseAdmin.createTable(hTableDescriptor);
            System.out.println("建表成功");
        }
    }

    public static void deleteTable(String tableName) throws IOException {
        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
        if(isTableExist(tableName)){
            hBaseAdmin.disableTable(TableName.valueOf(tableName));
            hBaseAdmin.deleteTable(tableName);
            System.out.println("删表成功");
        }else {
            System.out.println("表不存在");
        }
    }

    public static void addRowData(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        hTable.put(put);
        hTable.close();
    }

    public static void deleteMultiRow(String tableName, String... rows) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        ArrayList<Delete> deleteArrayList = new ArrayList<>();
        for(String row:rows){
            deleteArrayList.add(new Delete(Bytes.toBytes(row)));
        }
        hTable.delete(deleteArrayList);
        hTable.close();
    }

    public static void getAllRows(String tableName) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        Scan scan = new Scan(Bytes.toBytes("1001"), Bytes.toBytes("1002"));
//        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.NO_OP.EQUAL, new SubstringComparator("1001"));
//        scan.setFilter(rowFilter);
        ResultScanner scanner = hTable.getScanner(scan);
        for(Result result:scanner){
            for(Cell cell:result.rawCells()){
                System.out.println("行键 " + Bytes.toString(CellUtil.cloneRow(cell)) );
                System.out.println("列族 " + Bytes.toString(CellUtil.cloneFamily(cell)) );
                System.out.println("列限定符 " + Bytes.toString(CellUtil.cloneQualifier(cell)) );
                System.out.println("值 " + Bytes.toString(CellUtil.cloneValue(cell)) );
                System.out.println("TimeStamp " + cell.getTimestamp());
            }
        }

    }

    public static void getRow(String tableName, String rowKey) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = hTable.get(get);
        for(Cell cell:result.rawCells()){
            System.out.println("行键 " + Bytes.toString(CellUtil.cloneRow(cell)) );
            System.out.println("列族 " + Bytes.toString(CellUtil.cloneFamily(cell)) );
            System.out.println("列限定符 " + Bytes.toString(CellUtil.cloneQualifier(cell)) );
            System.out.println("值 " + Bytes.toString(CellUtil.cloneValue(cell)) );
            System.out.println("TimeStamp " + cell.getTimestamp());
        }
        hTable.close();
    }

    public static void getRowQualifier(String tableName, String rowKey, String family, String qualifier) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        Result result = hTable.get(get);
        for(Cell cell:result.rawCells()){
            System.out.println("行键 " + Bytes.toString(CellUtil.cloneRow(cell)) );
            System.out.println("列族 " + Bytes.toString(CellUtil.cloneFamily(cell)) );
            System.out.println("列限定符 " + Bytes.toString(CellUtil.cloneQualifier(cell)) );
            System.out.println("值 " + Bytes.toString(CellUtil.cloneValue(cell)) );
            System.out.println("TimeStamp " + cell.getTimestamp());
        }

        hTable.close();

    }

    public static void main(String[] args) throws IOException {
//        createTable("s1", "info1", "info2");
//        addRowData("s1", "1001", "info1", "l1", "a1");
//        addRowData("s1", "1001","info2", "l1", "a2");
//        addRowData("s1", "1002", "info1", "l1", "a1");
//        addRowData("s1", "1002","info2", "l1", "a2");
        getAllRows("s1");
//        getRow("s1", "1001");
//        getRowQualifier("s1", "1001", "info1", "l1");
//        deleteMultiRow("s1", "1001");
    }
}







