package com.lh;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lh.common.GmallConfig;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 21:09 2022/8/10
 */



public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);


        String filePath = "D:\\workLv\\learn\\proj\\hadoop-code\\bigDataSolve\\FlinkTutorial\\src\\main\\resources" +
                "\\sensor.txt";
        String out = "D:\\workLv\\learn\\proj\\hadoop-code\\bigDataSolve\\FlinkTutorial\\src\\main\\resources" +
                "\\test.txt";
//        String filePath = "D:\\workLv\\learn\\proj\\hadoop-code\\bigDataSolve\\FlinkTutorial\\src\\main\\resources\\sensor.txt";
//        final Schema schema = new Schema()
//                .field("id", DataTypes.STRING())
//                .field("timestamp", DataTypes.BIGINT())
//                .field("temp", DataTypes.DOUBLE());
//
//        tableEnv.connect(new FileSystem().path(path))
//                .withFormat(new Csv())
//                .withSchema(schema)
//                .createTemporaryTable("inputTable");

        tableEnv.connect( new FileSystem().path(filePath))
                .withFormat( new Csv().fieldDelimiter(','))
                .withSchema( new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");

        tableEnv.connect( new FileSystem().path(out))
                .withFormat( new Csv().fieldDelimiter(','))
                .withSchema( new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temp", DataTypes.DOUBLE())
                )
                .createTemporaryTable("output");



        Table sqlQuery = tableEnv.sqlQuery("select id, temp from inputTable");
        tableEnv.toAppendStream(sqlQuery, Row.class).print("sqlQuery");
        sqlQuery.executeInsert("output");
        env.execute();

    }


}







