package com.atguigu.weibo.dao;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 12:08 2021/4/15
 */

import com.atguigu.weibo.contants.Constants;
import com.sun.tools.internal.jxc.ap.Const;
import javafx.scene.control.Tab;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;




/**
 * 业务相关：
 * 1.发布微博
 * 2.删除微博
 * 3.关注用户
 * 4.取关用户
 * 5.获取用户的初始化页面
 * 6.获取用户微博详情
 */
public class HBaseDao {
    /**
     * 1.发布微博
     * a、微博内容表中添加 1 条数据
     * b、微博收件箱表对所有粉丝用户添加数据
     */
    public static void publishWeiBo(String uid, String content) throws IOException {
        //获取connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        /*
         *第一部分：操作微博内容表content
         * */
        //1.1 获取微博内容表对象
        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        //1.2 获取当前的时间戳
        long timeStamp = System.currentTimeMillis();
        //1.3 获取rowKey
        String rowKey = uid + "_" + timeStamp;
        //1.4 创建put对象
        Put contentPut = new Put(Bytes.toBytes(rowKey));
        //1.5 给put对象赋值(传列族，列名，值)
        contentPut.addColumn(Bytes.toBytes(Constants.CONTENT_TABLE_CF), Bytes.toBytes("content"),
                Bytes.toBytes(content));
        //1.6 执行插入数据操作
        contentTable.put(contentPut);
        /*
         * 第二部分：操作微博收件箱表inbox，给所有粉丝更新数据
         * */
        //2.1 获取用户关系表对象
        Table relationTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));
        //2.2 获取当前发布人的fans列族数据
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes(Constants.RELATION_TABLE_CF2));
        Result fansResult = relationTable.get(get);

        //2.3构建一个集合inboxPutsList，用于存放微博内容表的put对象
        ArrayList<Put> inboxPutsList = new ArrayList<>();
        //2.4 遍历粉丝
        for (Cell cell : fansResult.rawCells()) {
            //2.5 构建微博收件箱表的Put对象
            Put inboxPut = new Put(CellUtil.cloneQualifier(cell));
            //2.6 给收件箱表的put对象赋值
            inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF), Bytes.toBytes(uid), Bytes.toBytes(rowKey));
            //2.7 将收件箱表的put对象存入inboxPutsList结合
            inboxPutsList.add(inboxPut);

        }
        //2.8 判断inboxPutsList集合是否为空，如果有粉丝的话
        if(!inboxPutsList.isEmpty()){
            //获取收件箱表对象
            Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
            //插入数据
            inboxTable.put(inboxPutsList);
            //关闭收件箱表
            inboxTable.close();
        }
        //关闭资源（关系表，内容表，conn）
        relationTable.close();
        contentTable.close();
        connection.close();
    }



    /**
     * 2.删除微博
     */
    public static void deleteWeiBo(String rowKey) {
    }

    /**
     * 3.关注用户
     * uid是当前用户，attends可变形参表示要关注的用户数(数组类型)
     * a、在微博用户关系表中，对当前主动操作的用户添加新关注的好友
     * b、在微博用户关系表中，对被关注的用户添加新的粉丝
     * c、微博收件箱表中添加所关注的用户发布的微博
     */
    public static void addAttends(String uid, String... attends) throws IOException {
        if(attends.length < 1){
            System.out.println("请选择关注的人！");
            return;
        }
            // 关系表添加关注者和粉丝
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Table relaTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));
        ArrayList<Put> relaPuts = new ArrayList<>();
        Put uidPut = new Put(Bytes.toBytes(uid));
        for (String attend : attends) {
            uidPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF1), Bytes.toBytes(attend),
                    Bytes.toBytes(attend));
            // 添加粉丝
            Put attendPut = new Put(Bytes.toBytes(attend));
            attendPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF2), Bytes.toBytes(uid), Bytes.toBytes(uid));
            relaPuts.add(attendPut);
        }
        relaPuts.add(uidPut);
        //执行批量插入操作
        relaTable.put(relaPuts);
        //第二部分：操作收件箱表
        Table contTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        Put inboxPut = new Put(Bytes.toBytes(uid));
        for (String attend : attends) {
            //获取当前被关注者的近期发布的微博
            Scan scan = new Scan(Bytes.toBytes(attend + "_"), Bytes.toBytes(attend + "|"));
            ResultScanner resultScanner = contTable.getScanner(scan);
            long currentTimeMillis = System.currentTimeMillis();
            for (Result result : resultScanner) {
                inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF), Bytes.toBytes(attend), currentTimeMillis++,
                        result.getRow());
            }
        }

        if (!inboxPut.isEmpty()) {
            Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
            inboxTable.put(inboxPut);
            inboxTable.close();
        }
        relaTable.close();
        contTable.close();
        connection.close();
    }

    /**
     * 4.取关用户
     * a、在微博用户关系表中，对当前主动操作的用户移除取关的好友(attends)
     * b、在微博用户关系表中，对被取关的用户移除粉丝
     * c、微博收件箱中删除取关的用户发布的微博
     */
    public static void removeAttends(String uid, String... attends) throws IOException {
        //判空
        if (uid == null || uid.length() <= 0 || attends == null || attends.length <= 0)
            return;

        //获取connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //a、在微博用户关系表中，删除已关注的好友
        //获取用户关系表操作对象
        Table relationTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));
        //创建一个集合，用于存放用户关系的delete对象
        List<Delete> relationDeletesList = new ArrayList<>();

        //创建取关操作者的 uid 对应的 Delete 对象
        Delete uidDelete = new Delete(Bytes.toBytes(uid));

        //遍历取关
        for (String attend : attends) {
            //给操作者的delete对象赋值
            uidDelete.addColumns(Bytes.toBytes(Constants.RELATION_TABLE_CF1), Bytes.toBytes(attend));
            //创建被取关者的delete对象
            Delete delete = new Delete(Bytes.toBytes(attend));
            //给取关者delete对象赋值（删除取关者的一个粉丝uid）
            delete.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF2), Bytes.toBytes(uid));
            //将被取关者的delete对象添加到集合
            relationDeletesList.add((delete));
        }
        //将操作者的delete对象添加到集合
        relationDeletesList.add(uidDelete);
        //执行用户关系表的删除操作
        relationTable.delete(relationDeletesList);
        //c、 得到微博收件箱表的操作对象
        Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
        // 创建操作者的delete对象
        Delete recDelete = new Delete(Bytes.toBytes(uid));
        // 给操作者的delete对象赋值
        for (String attend : attends) {
            recDelete.addColumns(Bytes.toBytes(Constants.INBOX_TABLE_CF), Bytes.toBytes(attend));
        }
        //执行收件箱的删除操作
        inboxTable.delete(recDelete);

        //关闭资源
        relationTable.close();
        inboxTable.close();
        connection.close();
    }

    /**
     * 5.获取初始化，获取关注的人的微博内容
     */
    public static void getInit(String uid) throws IOException {
        //获取connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        //获得收件箱表的操作对象
        Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));

        //a、从收件箱中取得微博 rowKey
        Get get = new Get(Bytes.toBytes(uid));
        //设置最大版本号
        get.setMaxVersions(3);
        Result result = inboxTable.get(get);

        List<byte []> rowKeys = new ArrayList<>();
        for(Cell cell:result.rawCells()){
            rowKeys.add(CellUtil.cloneValue(cell));
        }

        //b、根据取出的所有 rowkey 去微博内容表中检索数据
        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        List<Get> gets = new ArrayList<>();
        //根据 rowKey 取出对应微博的具体内容
        for(byte[] rowKey:rowKeys){
            Get get1 = new Get(rowKey);
            gets.add(get1);
        }
        //得到所有的微博内容的 result 对象
        Result[] results = contentTable.get(gets);
        for (Result res : results) {

            for (Cell cell : res.rawCells()) {
                String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                String userId = rowKey.split("_")[0];
                String timestamp = rowKey.split("_")[1];
                String content = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println(rowKey + " " + userId + " " + timestamp + " " + content);
            }
        }

    }

    /**
     * 6.获取某个人的所有微博详情
     */
    public static void getWeiBo(String uid) throws IOException {
        //获得conn连接对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        //获得content表的操作对象
        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        //构建scan对象
        Scan scan = new Scan();
        //构建过滤器，并且设置给scan对象
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(uid + "_"));
        scan.setFilter(rowFilter);
        // 获得扫描结果
        ResultScanner resultScanner = contentTable.getScanner(scan);
        // 遍历扫描结果
        for (Result result : resultScanner) {
            for (Cell cell : result.rawCells()) {
                System.out.print("行键： " + Bytes.toString(CellUtil.cloneRow(cell)) );
                System.out.print("\t列族： " + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.print("\t列： " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("\t值： " + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        //关闭资源
        contentTable.close();
        connection.close();
    }
}



