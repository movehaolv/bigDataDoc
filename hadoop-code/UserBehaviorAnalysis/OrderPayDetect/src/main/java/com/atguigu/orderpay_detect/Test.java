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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 21:24 2021/11/28
 */


public class Test {
    public static void main(String[] args) throws Exception {
        LinkedList<String> list = new LinkedList<>();

    }
}

