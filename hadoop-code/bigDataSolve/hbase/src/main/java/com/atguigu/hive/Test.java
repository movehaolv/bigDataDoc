package com.atguigu.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 14:05 2022/1/6
 */


//public class Test {
//
//    public static  byte[][] getSplitKeys() {
//        String[] keys = new String[] {"10|", "20|", "30|", "40|", "50|", "60|", "70|", "80|", "90|" };
//        byte[][] splitKeys = new byte[keys.length][];
//        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);//升序排序
//        for (int i = 0; i < keys.length; i++) {
//            rows.add(Bytes.toBytes(keys[i]));
//        }
//
//        Iterator<byte[]> rowKeyIter = rows.iterator();
//        int i=0;
//        while (rowKeyIter.hasNext()) {
//            byte[] tempRow = rowKeyIter.next();
//            rowKeyIter.remove();
//            splitKeys[i] = tempRow;
//            i++;
//        }
//        return splitKeys;
//    }
//
//    public static void main(String[] args) {
//        byte[][] splitKeys = getSplitKeys();
//        for(int i=0;i<9;i++){
//            System.out.println(new String(splitKeys[i]));
//            System.out.println(splitKeys[i][0] +" " + splitKeys[i][1] +" " + splitKeys[i][2]);
//        }
//    }
//
//}


public class Test{
    public static Configuration conf;
    static{
        //使用 HBaseConfiguration 的单例方法实例化
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.17.133");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }


    /**
     * 创建索引表
     */
    public static void main(String[] args) throws IOException {
        String tableNameIndex=  "t1";
        String familyArray[]={"c"};
        initUserTable(tableNameIndex, familyArray,true);
        HBaseAdmin hadmin = new HBaseAdmin(conf);
//        Table tableIndex=HbaseConnectionUtils.getInstance().getTable( tableNameIndex);
//        Table tableIndex=TableName.valueOf(tableNameIndex);
        Table tableIndex = new HTable(conf, tableNameIndex);
        batchPutIndex(tableIndex);//索引表
    }
    /**
     提前创建预分区。划分10个区域，预计100数据量，每个分区10
     *      *用户id小于10的都划分在“00”
     *      *用户id大于10且小于20的都划分在“10区域”
     *     依次类推.....
     *     rowkey的策略:
     *     分区编号(根据用户id所在划分的分区编号获得，见上面解释)+"-u"+u.getUserId();
     */
    public  static void batchPutIndex(Table hTable) {
        List<Put> list = new ArrayList<Put>();
        for (int i = 1; i <= 100; i++) {
            String regionNo="00";
            regionNo= getRegionNo(i,regionNo);
            String k=i+"";
            if(k.length()<3){
                StringBuffer sb=new StringBuffer();
                for(int m=0;m<3-k.length();m++){
                    sb.append("0");
                }
                k=sb.toString()+k;
            }
            byte[] rowkey = Bytes.toBytes(regionNo +"-u"+k);
            Put put = new Put(rowkey);
            put.addColumn("c".getBytes(), "info".getBytes(), Bytes.toBytes("zs" + i));
            list.add(put);
        }
        try {
            hTable.put(list);
        } catch (IOException e) {
            e.printStackTrace();
        }
        list.clear();
        System.out.println("添加数据成功..........................");
    }
    public static String getRegionNo(int i,String regionNo){
        if(i<10){
            regionNo="00";
        }
        else if(i>=10&&i<20){
            regionNo="10";
        }
        else if(i>=20&&i<30){
            regionNo="20";
        }
        else if(i>=30&&i<40){
            regionNo="30";
        }
        else if(i>=50&&i<60){
            regionNo="40";
        }
        else if(i>=50&&i<60){
            regionNo="50";
        }
        else if(i>=60&&i<70){
            regionNo="60";
        }
        else if(i>=70&&i<80){
            regionNo="70";
        }
        else if(i>=80&&i<90){
            regionNo="80";
        }
        else{
            regionNo="90";
        }
        return regionNo;
    }
    public static void initUserTable(String tableName,String familyArray[],boolean partionFlag){
        List<String> list=new ArrayList<String>();
        try {
            HBaseAdmin hadmin = new HBaseAdmin(conf);
//            Admin hadmin = HbaseConnectionUtils.getInstance().getConnection().getAdmin();
            TableName tm = TableName.valueOf(tableName);
            if (!hadmin.tableExists(TableName.valueOf(tableName))) {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tm);
                for(String colFa:familyArray){
                    HColumnDescriptor family = new HColumnDescriptor(colFa);
                    family.setMaxVersions(1);
                    hTableDescriptor.addFamily(family);
                }
                if(partionFlag){
                    hadmin.createTable(hTableDescriptor, getSplitKeys());
                }
                else {
                    hadmin.createTable(hTableDescriptor);//不分区
                }
                hadmin.close();
            }
            else {
                System.out.println("................新建表:"+tableName+"已存在..........................");
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("................................................create hbase table "+tableName+" successful..........");
    }
    public static  byte[][] getSplitKeys() {
        String[] keys = new String[] {"10-", "20-", "30-", "40-", "50-", "60-", "70-", "80-", "90-" };
        byte[][] splitKeys = new byte[keys.length][];
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);//升序排序
        for (int i = 0; i < keys.length; i++) {
            rows.add(Bytes.toBytes(keys[i]));
        }
        Iterator<byte[]> rowKeyIter = rows.iterator();
        int i=0;
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            splitKeys[i] = tempRow;
            i++;
        }
        return splitKeys;
    }

}
