package com.atguigu.orderpay_detect;

import com.atguigu.orderpay_detect.beans.OrderEvent;
import com.atguigu.orderpay_detect.beans.OrderResult;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 21:24 2021/11/28
 */


public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 读取数据并转换成POJO类型
        URL resource = OrderPayTimeout.class.getResource("/OrderLog.csv");
        DataStream<OrderEvent> orderEventStream = env.readTextFile(resource.getPath())
                .map( line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                } )
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("create").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "create".equals(value.getEventType());
            }
        }).followedBy("pay").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "pay".equals(value.getEventType());
            }
        }).within(Time.minutes(15));

        OutputTag<OrderResult> outputTag = new OutputTag<OrderResult>("orderTimeOut") {
        };

        PatternStream<OrderEvent> patternStream = CEP.pattern(orderEventStream.keyBy(OrderEvent::getOrderId), pattern);

        SingleOutputStreamOperator<OrderResult> select = patternStream.select(outputTag, new TimeOutEvent(), new NormalEvent());
        select.print("normal ");
//        DataStream<OrderResult> orderTimeOut = select.getSideOutput(new OutputTag<OrderResult>("orderTimeOut"){});
        DataStream<OrderResult> orderTimeOut = select.getSideOutput(outputTag);
        orderTimeOut.print("timeOut ");


        env.execute();


    }

    public static class TimeOutEvent implements PatternTimeoutFunction<OrderEvent, OrderResult>{

        @Override
        public OrderResult timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
//            System.out.println(pattern.size() + "  timeout");
            OrderEvent firstEvent = pattern.get("create").get(0);
            return new OrderResult(firstEvent.getOrderId(), "timeOut " + timeoutTimestamp);
        }
    }

    public static class NormalEvent implements PatternSelectFunction<OrderEvent, OrderResult>{

        @Override
        public OrderResult select(Map<String, List<OrderEvent>> pattern) throws Exception {
//            System.out.println(pattern.size() + "  OrderResult");
            OrderEvent firstEvent = pattern.get("pay").get(0);
            return new OrderResult(firstEvent.getOrderId(), "payed");
        }
    }

}
