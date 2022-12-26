package com.lh.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.lh.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 10:29 2022/10/13
 */


public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String sourceTopic = "dwd_page_log";
        String groupId = "userJumpDetailApp";
        String sinkTopic = "dwm_user_jump_detail";

        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));
//        DataStreamSource<String> kafkaDS = env.socketTextStream("node01", 9999);
        WatermarkStrategy<JSONObject> wm =
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(
                new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }
        );
        SingleOutputStreamOperator<JSONObject> dsAddWm = kafkaDS.map(JSON::parseObject).assignTimestampsAndWatermarks(wm);

        Pattern<JSONObject, JSONObject> userJumpPattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String last_page_id = value.getJSONObject("page").getString("last_page_id");
                return last_page_id == null || last_page_id.length() <= 0;
            }
        }).times(2).consecutive().within(Time.seconds(10));
        PatternStream<JSONObject> patternDS = CEP.pattern(dsAddWm.keyBy(json -> json.getJSONObject("common").getString("mid")), userJumpPattern);


        OutputTag<JSONObject> outputTag = new OutputTag<JSONObject>("timeout") {
        };
        SingleOutputStreamOperator<JSONObject> selectDS = patternDS.select(outputTag,
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                JSONObject jsonObject = map.get("start").get(0);
                return jsonObject;
            }
        }, new PatternSelectFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                return map.get("start").get(0);
            }
        });

        DataStream<JSONObject> timeOutDS = selectDS.getSideOutput(outputTag);
        DataStream<JSONObject> userJumpDS = selectDS.union(timeOutDS);
//
        userJumpDS.print("userJumpDS>>>>>>>>>>>>>>>");

//        selectDS.print("selectDS>>>>>>>>>>>>>");
//        timeOutDS.print("timeOutDS>>>>>>>>>>>>>>>>>>>>>>>");

        userJumpDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getProducer(sinkTopic));

        env.execute("UserJumpDetailApp");

    }


}


/*

within(10)
{"common":{"ar":"230000","ba":"iPhone"},"page":{"during_time":16539,"item":"8","item_type":"sku_ids","last_page_id":"","page_id":"payment"},"ts":1615043100000}
{"common":{"ar":"230000","ba":"iPhone"},"page":{"during_time":16539,"item":"8","item_type":"sku_ids","last_page_id":"","page_id":"payment"},"ts":1615043109000}
{"common":{"ar":"230000","ba":"iPhone"},"page":{"during_time":16539,"item":"8","item_type":"sku_ids","last_page_id":"","page_id":"payment"},"ts":1615043110000}
{"common":{"ar":"230000","ba":"iPhone"},"page":{"during_time":16539,"item":"8","item_type":"sku_ids","last_page_id":"","page_id":"payment"},"ts":1615043111000}
// 1615043111000 延迟1s，1615043110000， 关闭[1615043100000, 1615043110000)的窗口，可以输出第一条数据

输出：
{"common":{"ar":"230000","uid":"21"},"page":{"page_id":"payment","item":"8","during_time":16539,"item_type":"sku_ids","last_page_id":""},"ts":1615043100000}
//



 */