package com.lh.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lh.bean.VisitorStats;
import com.lh.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 13:34 2022/11/6
 */


public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String groupId = "visitor_stats_app_210325";

        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        String pageViewSourceTopic = "dwd_page_log";
        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getKafkaConsumer(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> ujDS = env.addSource(MyKafkaUtil.getKafkaConsumer(userJumpDetailSourceTopic, groupId));
        DataStreamSource<String> pvDS = env.addSource(MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId));

        SingleOutputStreamOperator<VisitorStats> uvCount = uvDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                String ar = jsonObject.getJSONObject("common").getString("ar");
                String vc = jsonObject.getJSONObject("common").getString("vc");
                String ch = jsonObject.getJSONObject("common").getString("ch");
                String is_new = jsonObject.getJSONObject("common").getString("is_new");
                Long during_time = jsonObject.getJSONObject("page").getLong("during_time");
                Long ts = jsonObject.getLong("ts");
                return new VisitorStats("0", "0", vc, ch, ar, is_new, 1L, 0L, 0L, 0L, 0L, ts);
            }
        });

        SingleOutputStreamOperator<VisitorStats> ujCount = ujDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                String ar = jsonObject.getJSONObject("common").getString("ar");
                String vc = jsonObject.getJSONObject("common").getString("vc");
                String ch = jsonObject.getJSONObject("common").getString("ch");
                String is_new = jsonObject.getJSONObject("common").getString("is_new");
                Long during_time = jsonObject.getJSONObject("page").getLong("during_time");
                Long ts = jsonObject.getLong("ts");
                return new VisitorStats("0", "0", vc, ch, ar, is_new, 0L, 0L, 0L, 1L, 0L, ts);
            }
        });


        SingleOutputStreamOperator<VisitorStats> pvCount = pvDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                String ar = jsonObject.getJSONObject("common").getString("ar");
                String vc = jsonObject.getJSONObject("common").getString("vc");
                String ch = jsonObject.getJSONObject("common").getString("ch");
                String is_new = jsonObject.getJSONObject("common").getString("is_new");
                Long during_time = jsonObject.getJSONObject("page").getLong("during_time");
                String last_page_id = jsonObject.getJSONObject("page").getString("last_page_id");
                Long ts = jsonObject.getLong("ts");
                Long sv = 0L;
                if(last_page_id==null || last_page_id.length()<0){
                    sv=1L;
                }
                return new VisitorStats("0", "0", vc, ch, ar, is_new, 0L, 1L, sv, 0L, during_time, ts);
            }
        });


        DataStream<VisitorStats> unionDS = uvCount.union(ujCount).union(pvCount);

        SingleOutputStreamOperator<VisitorStats> reduceDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(11)).withTimestampAssigner(
                new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }
        )).keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return new Tuple4<String, String, String, String>(value.getVc(), value.getCh(), value.getAr(),
                        value.getIs_new());
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<VisitorStats>() {
                    @Override
                    public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                        value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                        value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                        value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                        value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                        value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());
                        value1.setTs(Math.max(value1.getTs(), value2.getTs()));
                        return value1;
                    }
                }, new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void process(Tuple4<String, String, String, String> tuple, Context context, Iterable<VisitorStats> elements, Collector<VisitorStats> out) throws Exception {
                        VisitorStats value = elements.iterator().next();
                        long end = context.window().getEnd();
                        long start = context.window().getStart();
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        value.setStt(sdf.format(new Date(start)));
                        value.setEdt(sdf.format(new Date(end)));
                        out.collect(value);
                    }
                });

        reduceDS.print(">>>>>>>>>>>>>>>>>>>>>>>>");
//        reduceDS.map(JSON::toJSONString).addSink(MyKafkaUtil.getProducer("dws_vs1"));
        env.execute("VisitorStatsApp");

    }
}
