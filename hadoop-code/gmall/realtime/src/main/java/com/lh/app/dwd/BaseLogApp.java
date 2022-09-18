package com.lh.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.lh.utils.MyKafkaUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 9:41 2022/8/13
 */

// web - nginx - weologger - kafka - baselogapp - fenliu
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputDS = env.addSource(MyKafkaUtil.getConsumer("ods_base_log", "gmall2021_ods_base_log"));
//        inputDS.print("input++++++++++++++++++++++++++++");

        OutputTag<String> dirty = new OutputTag<String>("dirty"){};
        // 先转为json
        SingleOutputStreamOperator<JSONObject> str2Json = inputDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(dirty, s);
                }
            }
        });

//        str2Json.print();
        str2Json.getSideOutput(dirty).print("dirty>>>");


        OutputTag<JSONObject>  startTag = new OutputTag<JSONObject>("start"){};
        OutputTag<JSONObject>  displayTag = new OutputTag<JSONObject>("display"){};

        // 确定是否新用户
        SingleOutputStreamOperator<JSONObject> process = str2Json.keyBy(jsonObject -> jsonObject.getJSONObject(
                "common").get("mid").toString())
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("isNew", String.class));
                    }

                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                        String isNew = value.getJSONObject("common").get("is_new").toString();
                        if ("1".equals(isNew)) {
                            if (valueState.value() != null) {
                                value.getJSONObject("common").put("is_new", "0");
                            } else {
                                valueState.update("1");
                            }
                        }
                        JSONObject start = value.getJSONObject("start");
                        if (start != null) {
                            ctx.output(startTag, value);
                        } else {
                            out.collect(value);
                            JSONArray displays = value.getJSONArray("displays");
                            if (displays != null) {
                                String pageId = value.getJSONObject("page").get("page_id").toString();
                                for (int i = 0; i < displays.size(); i++) {
                                    JSONObject display = (JSONObject) displays.get(i);
                                    display.put("page_id", pageId);
                                    ctx.output(displayTag, display);
                                }
                            }
                        }
                    }
                });

        process.getSideOutput(startTag).print("start>>>>>>>>>>>>>>>>");
        process.print("page>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        process.getSideOutput(displayTag).print("display>>>>>>>>>>>>>>>>>>>>>>>>>>>");


        env.execute("BaseLogApp");

    }
}

