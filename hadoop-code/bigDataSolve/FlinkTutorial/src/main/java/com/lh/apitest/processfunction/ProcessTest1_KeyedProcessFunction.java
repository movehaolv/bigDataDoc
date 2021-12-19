package com.lh.apitest.processfunction;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTutorial
 * Package: com.atguigu.apitest.processfunction
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/11/11 10:25
 */

import com.lh.apitest.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @ClassName: ProcessTest1_KeyedProcessFunction
 * @Description:
 * @Author: wushengran on 2020/11/11 10:25
 * @Version: 1.0
 */
public class ProcessTest1_KeyedProcessFunction {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // socket文本流
        DataStream<String> inputStream = env.socketTextStream("192.168.17.133", 7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading sensorReading) {
                return sensorReading.getTimestamp() * 1000L;
            }
        });

        // 测试KeyedProcessFunction，先分组然后自定义处理
        dataStream.keyBy("id")
                .process( new MyProcess() )
                .print();

        env.execute();
    }

    // 实现自定义的处理函数
    public static class MyProcess extends KeyedProcessFunction<Tuple, SensorReading, Integer>{
        ValueState<Long> tsTimerState;
        ValueState<Long> count;

        @Override
        public void open(Configuration parameters) throws Exception {
            tsTimerState =  getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-timer", Long.class));
            count =  getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class, 0L));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<Integer> out) throws Exception {
            out.collect(value.getId().length());

            // context
            ctx.timestamp();
            ctx.getCurrentKey();
//            ctx.output();
            ctx.timerService().currentProcessingTime();
            ctx.timerService().currentWatermark();
//            System.out.println("当前时间 " + ctx.timerService().currentProcessingTime());
            if(count.value() == 0){
                System.out.println("注册定时器触发时间 " + (value.getTimestamp() * 1000L + 5000L));
//            ctx.timerService().registerProcessingTimeTimer( ctx.timerService().currentProcessingTime() + 5000L);
                ctx.timerService().registerEventTimeTimer(value.getTimestamp() * 1000L + 5000L);
                count.update(1L);
            }

            tsTimerState.update(ctx.timerService().currentProcessingTime() + 1000L);
//            ctx.timerService().registerEventTimeTimer((value.getTimestamp() + 10) * 1000L);
//            ctx.timerService().deleteProcessingTimeTimer(tsTimerState.value());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
            System.out.println("定时器触发时间： " + timestamp +  "  OnTimerContext.getCurrentKey：" + ctx.getCurrentKey());
            // timestamp 注册的定时时间，当WM超过该定时时间就触发
            ctx.getCurrentKey();
//            ctx.output();
            ctx.timeDomain();
            out.collect((int)timestamp);
        }

        @Override
        public void close() throws Exception {
            tsTimerState.clear();
        }
    }
}
