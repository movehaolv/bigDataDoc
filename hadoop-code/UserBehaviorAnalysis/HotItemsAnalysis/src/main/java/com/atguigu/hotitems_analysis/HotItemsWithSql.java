package com.atguigu.hotitems_analysis;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: UserBehaviorAnalysis
 * Package: com.atguigu.hotitems_analysis
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/11/14 16:35
 */

import com.atguigu.hotitems_analysis.beans.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @ClassName: HotItemsWithSql
 * @Description:
 * @Author: wushengran on 2020/11/14 16:35
 * @Version: 1.0
 */
public class
HotItemsWithSql {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 读取数据，创建DataStream
        DataStream<String> inputStream = env.readTextFile("D:\\workLv\\learn\\proj\\hadoop-code\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\TestData\\UserBehaviorTest.csv");

        // 3. 转换为POJO，分配时间戳和watermark
        DataStream<UserBehavior> dataStream = inputStream
                .map(line -> {
                    String[] fields = line.split(",");
                    return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        // 4. 创建表执行环境，用blink版本
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 5. 将流转换成表
        Table dataTable = tableEnv.fromDataStream(dataStream, "itemId, behavior, timestamp.rowtime as ts");

        // 6. 分组开窗
        // table api
        Table windowAggTable = dataTable
                .filter("behavior = 'pv'")
                .window(Slide.over("1.hours").every("5.minutes").on("ts").as("w") )
                .groupBy("itemId, w")
                .select("itemId, w.end as windowEnd, itemId.count as cnt");
        windowAggTable.printSchema();

        // 7. 利用开窗函数，对count值进行排序并获取Row number，得到Top N
        // SQL
        DataStream<Row> aggStream = tableEnv.toAppendStream(windowAggTable, Row.class);
        tableEnv.createTemporaryView("agg", aggStream, "itemId, windowEnd, cnt");

        /*
        tableEnv.createTemporaryView("agg", windowAggTable);
        需要将windowAggTable转换成流再创建View，如果直接使用windowAggTable来创建流，调用的是org\apache\flink\table\api\TableEnvironment.java
        不是org\apache\flink\table\api\java\StreamTableEnvironment.java，后续操作会有问题，使用ROW_NUMBER会有问题

        PS：若直接使用如下会提示OVER windows' ordering in stream mode must be defined on a time attribute. 需要加上where row_num <= 5
        tableEnv.toRetractStream(tableEnv.sqlQuery( " select *, ROW_NUMBER() over (partition by windowEnd order by " +
                "cnt ) as row_num from agg"), Row.class).print("res ");

        */

        Table resultTable = tableEnv.sqlQuery("select * from " +
                "  ( select *, ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num " +
                "  from agg) " +
                " where row_num <= 5 ");

        tableEnv.toRetractStream(resultTable, Row.class).print();


        // 纯SQL实现
        tableEnv.createTemporaryView("data_table", dataStream, "itemId, behavior, timestamp.rowtime as ts");
        Table resultSqlTable = tableEnv.sqlQuery("select * from " +
                "  ( select *, ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num " +
                "  from ( " +
                "    select itemId, count(itemId) as cnt, HOP_END(ts, interval '5' minute, interval '1' hour) as windowEnd " +
                "    from data_table " +
                "    where behavior = 'pv' " +
                "    group by itemId, HOP(ts, interval '5' minute, interval '1' hour)" +
                "    )" +
                "  ) " +
                " where row_num <= 5 ");

//        tableEnv.toRetractStream(resultSqlTable, Row.class).print();

        env.execute("hot items with sql job");
    }
}
