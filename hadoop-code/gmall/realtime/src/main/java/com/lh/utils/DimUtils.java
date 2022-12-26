package com.lh.utils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lh.common.GmallConfig;
import redis.clients.jedis.Jedis;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;


public class DimUtils {
    public static JSONObject  getDimInfo(String tableName, Connection connection, String id) throws Exception{
        // redis
        String redisKey = "DIM:" + tableName + ":" + id;
        Jedis jedis = RedisUtil.getJedis();
        JSONObject value = JSON.parseObject(jedis.get(redisKey)) ;
        if(value==null){

            String sql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id ='" +id + "'";

            List<JSONObject> jsonObjects = JdbcUtil.queryList(connection, sql, JSONObject.class, false);
            value = jsonObjects.get(0);
            jedis.set(redisKey, value.toJSONString());
        }
        jedis.expire(redisKey, 24*60*60);
        jedis.close();
        return value;
    }
}
