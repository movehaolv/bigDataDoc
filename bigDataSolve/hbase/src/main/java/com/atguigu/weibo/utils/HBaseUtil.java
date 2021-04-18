package com.atguigu.weibo.utils;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 11:02 2021/4/15
 */

import com.atguigu.weibo.contants.Constants;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * content_table
 *           info
 *           content
 * uid1_ts   content_value
 *
 * relation_table
 *
 *         attents         fans
 *          uid2 uid3      uid2   uid5
 * uid1     uid2 uid3      uid2   uid5
 *
 * inbox_table
 *          info
 *          uid2       uid3
 * uid1     uid2_ts    uid3_ts
 *
 *
 *
 */


/**
* 需求：
        * 1.创建命名空间
        * 2.判断表是否存在
        * 3.创建表（一共有三张表）
        */
public class HBaseUtil {

    /**
     * 1.创建命名空间
     */
    public static void createNameSpace(String nameSpace) throws IOException, IOException {
        //（1）获取Connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        //（2）获取Admin对象
        Admin admin = connection.getAdmin();
        //（3）构建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
        //（4）创建命名空间
        try{
            admin.createNamespace(namespaceDescriptor);
        }catch (Exception e){
            System.out.println("命名空间已经存在");
        }
        //（5）关闭资源
        admin.close();
        connection.close();
    }

    /**
     * 2.判断表是否存在
     */
    public static boolean isTableExist(String tableName) throws IOException {
        //（1）获取Connection对象
        Connection connection = ConnectionFactory.createConnection();
        //（2）获取Admin对象
        Admin admin = connection.getAdmin();
        //（3）执行判断表是否存在
        boolean answer = admin.tableExists(TableName.valueOf(tableName));
        //（4）关闭资源
        admin.close();
        connection.close();
        //（5）返回结果
        return answer;
    }


    /**
     * 3.创建表（三张表）
     * 列族数目不一定，使用可变形参
     */
    public static void createTable(String tableName, int versions, String... cfs) throws IOException {
        //（1）判断是否传入了列族信息
        if(cfs.length == 0){
            System.out.println("请设置列族信息");
            return;
        }
        //（3）获取connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        //（4）获取admin对象
        Admin admin = connection.getAdmin();
        //（2）判断表是否存在
        if(isTableExist(tableName)) {
            if (!admin.isTableDisabled(TableName.valueOf(tableName))){
                admin.disableTable(TableName.valueOf(tableName));
            }
            admin.truncateTable(TableName.valueOf(tableName), true);
            System.out.println("表已经存在了！清空表");
            return;
        }

        //（5）创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //（6）循环添加列族信息
        for (String cf : cfs) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            //（7）设置版本
            hColumnDescriptor.setMaxVersions(versions);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        //（8）创建表操作
        admin.createTable(hTableDescriptor);
        //（9）关闭资源
        admin.close();
        connection.close();
    }
}
