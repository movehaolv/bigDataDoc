package com.lh.app.dws;

import com.lh.bean.ProvinceStats;
import com.lh.utils.MyKafkaUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.util.Date;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 20:44 2022/11/13
 */


public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);
        env.setParallelism(1);


        String ddl = "CREATE TABLE orderWide (" +
                "`province_id` BIGINT," +
                "`province_name` STRING," +
                "`province_area_code` STRING," +
                "`province_iso_code` STRING," +
                "`province_3166_2_code` STRING," +
                "`order_id` BIGINT," +
                "`total_amount` DECIMAL," +
                "`split_total_amount` DECIMAL," +
                "`feight_fee` DECIMAL," +
                "`create_time` String," +
                " `rt` as TO_TIMESTAMP(create_time) ," +
                "  WATERMARK FOR rt AS rt - INTERVAL '1' SECOND " +
                ") with (" +
                "'connector' = 'kafka', " +
                "'topic' = 'dwm_order_wide'," +
                "'properties.bootstrap.servers' = 'node01:9092'," +
//                "'properties.group.id' = 'testGroup'," +
                "'scan.startup.mode' = 'earliest-offset'," +
                "'format' = 'json')";
        tableEnv.executeSql(ddl);

        Table tableResult = tableEnv.sqlQuery("select " +
                "FROM_UNIXTIME(1547718199000 /1000,'yyyy-MM-dd HH:mm:ss'), " +
                "FROM_UNIXTIME(1547718199000/1000) from " +
//                "TO_TIMESTAMP(FROM_UNIXTIME(1547718199000 /1000,'yyyy-MM-dd HH:mm:ss')), " +
//                "TO_TIMESTAMP(FROM_UNIXTIME(1547718199000/1000)) from " +
                "orderWide");
        tableEnv.toAppendStream(tableResult, Row.class).print();

//        Table table = tableEnv.sqlQuery(
//                "select  a.province_id,a.province_name,a.province_area_code," +
//                "a.province_iso_code,a.province_3166_2_code," +
//                "count(order_id) as order_count," +
//                "sum(total_amount) as total_amount," +
//                "sum(split_total_amount) as split_total_amount," +
//                "sum(feight_fee) as feight_fee,stt,edt," +
//                "unix_timestamp()*1000 as ts from " +
//                        "(select " +
//                        "province_id," +
//                        "province_name," +
//                        "province_area_code," +
//                        "province_iso_code," +
//                        "province_3166_2_code, " +
//                        "order_id, " +
//                        "sum(total_amount)/count(total_amount) as total_amount," +
//                        "sum(split_total_amount) as split_total_amount," +
//                        "sum(feight_fee)/count(feight_fee) as feight_fee," +
//                        "date_format(tumble_start(rt, interval '10' second), 'yyyy-MM-dd HH:mm:ss') as stt, " +
//                        "date_format(tumble_end(rt, interval '10' second), 'yyyy-MM-dd HH:mm:ss') as edt " +
//                        "from orderWide " +
//                    "group by " +
//                        "province_id,province_name,province_area_code," +
//                        "province_iso_code,province_3166_2_code,order_id," +
//                        "tumble(rt, interval '10' second)) a" +
//                " group by a.province_id,a.province_name,a.province_area_code,a.province_iso_code" +
//                ",a.province_3166_2_code,stt,edt"
//        );
//        tableEnv.toAppendStream(table, ProvinceStats.class).print();

        //TODO 5.打印数据并写入ClickHouse
//        tableEnv.toAppendStream(table, ProvinceStats.class).print();
        env.execute();
    }
}
