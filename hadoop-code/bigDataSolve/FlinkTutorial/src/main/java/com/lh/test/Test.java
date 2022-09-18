package com.lh.test;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.ConnectTableDescriptor;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.descriptors.StreamTableDescriptor;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 15:14 2022/9/18
 */


public class Test {
    public static void main(String[] args) throws Exception {



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings  = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);
        env.setParallelism(1);

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

//        tableEnv.connect( new FileSystem().path(filePath))
//                .withFormat( new Csv().fieldDelimiter(','))
//                .withSchema( new Schema()
//                        .field("id", DataTypes.STRING())
//                        .field("timestamp", DataTypes.BIGINT())
//                        .field("temp", DataTypes.DOUBLE())
//                )
//                .createTemporaryTable("inputTable");
//
//        tableEnv.connect( new FileSystem().path(out))
//                .withFormat( new Csv().fieldDelimiter(','))
//                .withSchema( new Schema()
//                        .field("id", DataTypes.STRING())
//                        .field("timestamp", DataTypes.BIGINT())
//                        .field("temp", DataTypes.DOUBLE())
//                )
//                .createTemporaryTable("output");
//
//
//
//        Table sqlQuery = tableEnv.sqlQuery("select id, temp from inputTable where id = 'sensor_6'");
//
//        Table sqlAggTable = tableEnv.sqlQuery("select id, count(id) as cnt, avg(temp) as avgTemp from inputTable group by id");
//
//
//        tableEnv.toAppendStream(sqlQuery, Row.class).print("sqlQuery");


        env.readTextFile(filePath).print();

        env.execute();

    }
}


