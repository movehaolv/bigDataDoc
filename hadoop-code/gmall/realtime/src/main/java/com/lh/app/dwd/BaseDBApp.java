package com.lh.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.lh.app.function.CustomerDeserialization;
import com.lh.app.function.DimSinkHbase;
import com.lh.app.function.TableProcessFunction;
import com.lh.bean.TableProcess;
import com.lh.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 9:35 2022/8/17
 */


// Web - nginx - springboot - mysql - flinkCDC - kafka - springboot(basedbapp-根据tableProcess表分流) - kafka/hbase

public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取kafka的数据 ods_base_db
        FlinkKafkaConsumer<String> consumer = MyKafkaUtil.getConsumer("ods_base_db", "odsBaseDBApp");
//        mainDs.print();

        SingleOutputStreamOperator<JSONObject> mainDs = env.addSource(consumer).map(JSON::parseObject)
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        //取出数据的操作类型
                        String type = value.getString("type");

                        return !"delete".equals(type);
                    }
                });

        DebeziumSourceFunction<String> build = MySQLSource.<String>builder()
                .hostname("node01")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall-210325-realtime")
                .tableList("gmall-210325-realtime.table_process")
                .deserializer(new CustomerDeserialization())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> sideDS = env.addSource(build);
//        sideDS.print(">>>>>>>>>>>>>>>sideDS");
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastDS = sideDS.broadcast(mapStateDescriptor);
        BroadcastConnectedStream<JSONObject, String> connectDS = mainDs.connect(broadcastDS);

        OutputTag<JSONObject> hBaseTag = new OutputTag<JSONObject>("hbase"){};
        SingleOutputStreamOperator<JSONObject> processDS =
                connectDS.process(new TableProcessFunction(mapStateDescriptor, hBaseTag));
        processDS.print("kafka>>>>>>>>>>>>>>>>>>>>>>>");
        processDS.getSideOutput(hBaseTag).print("hbase>>>>>>>>>>>>>>>>>>>>>>>>");

        processDS.addSink(MyKafkaUtil.getProducer(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                return new ProducerRecord<byte[], byte[]>(element.getString("sinkTable"),
                        element.getString("after").getBytes());
            }
        }));
        processDS.getSideOutput(hBaseTag).addSink(new DimSinkHbase());


        env.execute("BaseDBApp");


    }
}
