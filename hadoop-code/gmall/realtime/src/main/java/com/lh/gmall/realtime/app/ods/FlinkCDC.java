package com.lh.gmall.realtime.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import com.lh.gmall.realtime.app.function.CustomerDeserialization;
import com.lh.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 10:54 2022/8/10
 */


public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DebeziumSourceFunction<String> build = MySQLSource.<String>builder()
                .hostname("node01")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall2021")
                .tableList("gmall2021.base_trademark")
                .startupOptions(StartupOptions.initial())
                .deserializer(new CustomerDeserialization())
                .build();

        DataStreamSource<String> source = env.addSource(build);

        source.print();
        source.addSink(MyKafkaUtil.getProducer("ods_base_db"));

        env.execute("flinkTest");


    }
}
