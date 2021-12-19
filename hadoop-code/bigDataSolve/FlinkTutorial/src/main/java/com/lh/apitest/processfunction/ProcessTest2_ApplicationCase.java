package com.lh.apitest.processfunction;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTutorial
 * Package: com.atguigu.apitest.processfunction
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/11/11 10:54
 */

import com.lh.apitest.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.net.URL;

/**
 * @ClassName: ProcessTest2_ApplicationCase
 * @Description: 监控温度传感器的温度值，如果温度值在 10 秒钟之内(processing time)
 * 连续上升， 则报警。
 * @Author: wushengran on 2020/11/11 10:54
 * @Version: 1.0
 */
public class ProcessTest2_ApplicationCase {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // socket文本流
        URL resource = ProcessTest2_ApplicationCase.class.getResource("/sensor.txt");
        DataStream<String> inputStream = env.readTextFile(resource.getPath());
//        DataStream<String> inputStream = env.socketTextStream("node01", 7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 测试KeyedProcessFunction，先分组然后自定义处理
        dataStream.keyBy("id")
                .process( new TempConsIncreWarning(5) )
                .print();

        env.execute();
    }

    // 实现自定义处理函数，检测一段时间内的温度连续上升，输出报警
    public static class TempConsIncreWarning extends KeyedProcessFunction<Tuple, SensorReading, String>{
        // 定义私有属性，当前统计的时间间隔
        private Integer interval;

        public TempConsIncreWarning(Integer interval) {
            this.interval = interval;
        }

        // 定义状态，保存上一次的温度值，定时器时间戳
        private ValueState<Double> lastTempState;
        private ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class, Double.MIN_VALUE));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            // 取出状态
            Double lastTemp = lastTempState.value();
            Long timerTs = timerTsState.value();
            System.out.println("当前温度 " + value.getTemperature() + "上次温度 " + lastTemp  + " WM " + ctx.timerService().currentWatermark());
            // 如果温度上升并且没有定时器，注册10秒后的定时器，开始等待
            if( value.getTemperature() >= lastTemp && timerTs == null ){
                // 计算出定时器时间戳
//                System.out.println("温度上升 当前温度 " + value.getTemperature() + "上次温度 " + lastTemp );
                Long ts = ctx.timerService().currentProcessingTime() + interval * 1000L;
                ctx.timerService().registerProcessingTimeTimer(ts);
                timerTsState.update(ts);
            }
            // 如果温度下降，那么删除定时器
            else if( value.getTemperature() < lastTemp && timerTs != null ){
//                System.out.println("温度下降 当前温度 " + value.getTemperature() + "上次温度 " + lastTemp );
                ctx.timerService().deleteProcessingTimeTimer(timerTs);
                timerTsState.clear();
            }

            // 更新温度状态
            lastTempState.update(value.getTemperature());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，输出报警信息
            out.collect("传感器" + ctx.getCurrentKey().getField(0) + "温度值连续" + interval + "s上升");
            timerTsState.clear();
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
        }
    }
}
