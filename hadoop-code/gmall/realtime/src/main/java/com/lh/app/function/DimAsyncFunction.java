package com.lh.app.function;

import com.alibaba.fastjson.JSONObject;
import com.lh.common.GmallConfig;
import com.lh.utils.DimUtils;
import com.lh.utils.ThreadPoolUtil;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 16:36 2022/10/25
 */


public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimAsyncJoinFunction<T> {

    ThreadPoolExecutor threadPoolExecutor;
    String tableName;
    Connection conn;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }



    @Override
    public void open(Configuration parameters) throws Exception {
        threadPoolExecutor = ThreadPoolUtil.getTreadPoolExecutor();

        Class.forName(GmallConfig.PHOENIX_DRIVER);
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    String key =  getKey(input);

                    JSONObject dims = DimUtils.getDimInfo(tableName, conn, key);
                    if(dims!=null){
                        join(input, dims);
                    }
                    resultFuture.complete(Collections.singletonList(input));

                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });
    }


}
