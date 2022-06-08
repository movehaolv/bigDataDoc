package com.atguigu.loginfail_detect;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 16:21 2021/11/28
 */


import com.atguigu.loginfail_detect.LoginFail;
import com.atguigu.loginfail_detect.beans.LoginEvent;
import com.atguigu.loginfail_detect.beans.LoginFailWarning;
import com.sun.deploy.util.BlackList;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.common.protocol.types.Field;

import java.net.URL;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;


/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 16:11 2021/11/12
 */

// LoginFailWarning{userId=1035, firstFailTime=1558430842, lastFailTime=1558430843, warningMsg='login fail 2 times in 2s'}
public class Test {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 1. 从文件中读取数据
        URL resource = LoginFail.class.getResource("/LoginLog.csv");
        DataStream<LoginEvent> loginEventStream = env.readTextFile(resource.getPath())
//        DataStream<LoginEvent> loginEventStream = env.socketTextStream("node01", 7777)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new LoginEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(LoginEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });
        Pattern<LoginEvent, LoginEvent> within = Pattern.<LoginEvent>begin("firstFail").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getLoginState());
            }
        }).next("secFail").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getLoginState());
            }
        }).within(Time.seconds(2));

        Pattern<LoginEvent, LoginEvent> firstFail = Pattern.<LoginEvent>begin("firstFail").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getLoginState());
            }
        }).times(3).consecutive().within(Time.seconds(5));





        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventStream.keyBy(LoginEvent::getUserId), firstFail);
        SingleOutputStreamOperator<LoginFailWarning> select = patternStream.select(new LoginFailEvent());
        select.print();
        env.execute();


    }

    public static class LoginFailEvent implements PatternSelectFunction<LoginEvent, LoginFailWarning>{

        @Override
        public LoginFailWarning select(Map<String, List<LoginEvent>> pattern) throws Exception {
//
//            LoginEvent firstFail = pattern.get("firstFail").get(0);
//            LoginEvent secFail = pattern.get("secFail").get(0);
//            return new LoginFailWarning(firstFail.getUserId(), firstFail.getTimestamp(), secFail.getTimestamp(), "login fail 2 times in 2s");
            LoginEvent firstFail = pattern.get("firstFail").get(0);
            LoginEvent lastLogin = pattern.get("firstFail").get(pattern.get("firstFail").size() - 1);
            return new LoginFailWarning(firstFail.getUserId(), firstFail.getTimestamp(), lastLogin.getTimestamp(), "login fail 2 times in 2s");

        }
    }


}

