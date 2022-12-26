package com.lh.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.Array;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;


public class JdbcUtil {
    public static <T> List<T>   queryList(Connection connection, String sql, Class<T> cls,Boolean isScoreToCamel) throws Exception {

        List<T> ret = new ArrayList<>();
        System.out.println("sql   >>>>>>>>>>>>>  " + sql);
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        T t = cls.newInstance();
        while (resultSet.next()){
            for(int i=1;i<metaData.getColumnCount()+1;i++){
                String columnName = metaData.getColumnName(i);
                Object val = resultSet.getObject(i);
                if(isScoreToCamel){
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                }
                BeanUtils.setProperty(t, columnName, val);
            }
            ret.add(t);
        }

        preparedStatement.close();
        resultSet.close();
        return ret;

    }

    public static void main(String[] args) throws SQLException {
        Driver driver = new com.mysql.cj.jdbc.Driver();
//	Driver driver = new com.mysql.jdbc.Driver();

        String url = "jdbc:mysql://192.168.17.133:3306/gmall2021";
        //将用户名和密码封装在Properties中
        Properties info = new Properties();
        info.setProperty("user","root");
        info.setProperty("password","000000");
        Connection conn = driver.connect(url,info);
        PreparedStatement preparedStatement = conn.prepareStatement("select * from base_province");
        ResultSet resultSet = preparedStatement.executeQuery();

        resultSet.next();
        Object object = resultSet.getObject(5);
        System.out.println(object);
        String to = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, object.toString());
        System.out.println(to);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("a", "{a:3}");
        String a = "{a:3}";
        JSONObject jsonObject1 = JSON.parseObject(a);
        System.out.println(jsonObject1.getString("a"));

    }
}
