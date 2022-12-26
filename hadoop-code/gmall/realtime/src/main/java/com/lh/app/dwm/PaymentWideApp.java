package com.lh.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lh.bean.OrderWide;
import com.lh.bean.PaymentInfo;
import com.lh.bean.PaymentWide;
import com.lh.utils.MyKafkaUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 20:55 2022/11/5
 */


public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";


        DataStreamSource<String> orderSource = env.addSource(MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId));
        DataStreamSource<String> paymentSource = env.addSource(MyKafkaUtil.getKafkaConsumer(paymentInfoSourceTopic,
                groupId));

        SingleOutputStreamOperator<OrderWide> orderSourceWithWM =
                orderSource.map(x->JSON.parseObject(x, OrderWide.class)).assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderWide>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                    @SneakyThrows
                    @Override
                    public long extractTimestamp(OrderWide element, long recordTimestamp) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        return sdf.parse(element.getCreate_time()).getTime();
                    }
                })
        );



        SingleOutputStreamOperator<PaymentInfo> paymentSourceWithWM =
                paymentSource.map(x->JSON.parseObject(x, PaymentInfo.class)).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @SneakyThrows
                            @Override
                            public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                return sdf.parse(element.getCreate_time()).getTime();
                            }
                        })
                );


        SingleOutputStreamOperator<PaymentWide> paymentWideDS = orderSourceWithWM.keyBy(new KeySelector<OrderWide, Long>() {
            @Override
            public Long getKey(OrderWide value) throws Exception {
                return value.getOrder_id();
            }
        }).intervalJoin(paymentSourceWithWM.keyBy(new KeySelector<PaymentInfo, Long>() {
            @Override
            public Long getKey(PaymentInfo value) throws Exception {
                return value.getOrder_id();
            }
        })).between(Time.seconds(-15), Time.seconds(5)).process(
                new ProcessJoinFunction<OrderWide, PaymentInfo, PaymentWide>() {

                    @Override
                    public void processElement(OrderWide left, PaymentInfo right, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(right, left));
                    }
                }
        );

        paymentWideDS.print(">>>>>>>>>>>");

        env.execute("paymentWideDS");
    }
}
