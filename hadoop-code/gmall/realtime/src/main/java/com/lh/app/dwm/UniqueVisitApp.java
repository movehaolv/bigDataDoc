package com.lh.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.lh.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;
import java.time.Duration;


/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 15:37 2022/10/14
 */


public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String groupId = "unique_visit_app_210325";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";

        FlinkKafkaConsumer<String> kafkaDs = MyKafkaUtil.getConsumer(sourceTopic, groupId);
        DataStreamSource<String> sourceDS = env.addSource(kafkaDs);

//        DataStreamSource<String> sourceDS = env.socketTextStream("node01", 9999);


        SingleOutputStreamOperator<JSONObject> uniqueDs =
                sourceDS.map(JSON::parseObject).keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        }).filter(new RichFilterFunction<JSONObject>() {

            ValueState<String> dataState;
            SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {

                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<String>("data-state",
                        String.class);
                StateTtlConfig build = StateTtlConfig.newBuilder(Time.hours(24)).build();
                valueStateDescriptor.enableTimeToLive(build);
                dataState = getRuntimeContext().getState(valueStateDescriptor);
                sdf = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                String last_page_id = value.getJSONObject("page").getString("last_page_id");
                String lastDate = dataState.value();
                String curDate = sdf.format(value.getLong("ts"));
                if (last_page_id == null || last_page_id.length() <= 0) {
                    if (!curDate.equals(lastDate)) {
//                        System.out.println("state is null");
                        dataState.update(curDate);
                        return true;
                    }
//                    System.out.println("state is not null");
                }
                return false;
            }
        });

        uniqueDs.print("dwm_unique_visit>>>");
        uniqueDs.map(x-> JSON.toJSONString(x)).addSink(MyKafkaUtil.getProducer(sinkTopic));


        env.execute("UniqueVisitApp");


    }
}
