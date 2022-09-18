package com.lh.app.function;

import com.alibaba.fastjson.JSONObject;
import com.lh.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.Set;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 16:42 2022/8/21
 */


public class DimSinkHbase extends RichSinkFunction<JSONObject> {

    Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        connection.setAutoCommit(true);
    }


    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;
        JSONObject after = value.getJSONObject("after");
        String upsertSql = getUpsertSql(after, value.getString("sinkTable"));
        try{
            preparedStatement = connection.prepareStatement(upsertSql);
            System.out.println(">>>>>>>>>>>>>>>> 开始执行插入hbase " + upsertSql);
            preparedStatement.executeUpdate();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(preparedStatement!=null){
                preparedStatement.close();
            }

        }
    }

    private String getUpsertSql(JSONObject after, String tableName) {
        Set<String> keys = after.keySet();
        Collection<Object> values = after.values();
        String sql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + tableName + "(" +
                StringUtils.join(keys, ",") + ") values('" +
                StringUtils.join(values, "','") + "')";

        return sql;
    }
}
