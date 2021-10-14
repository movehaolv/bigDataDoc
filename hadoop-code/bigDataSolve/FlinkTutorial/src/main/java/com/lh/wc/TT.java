package com.lh.wc;

import com.lh.apitest.beans.SensorReading;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 10:40 2021/8/24
 */


public class TT {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("D:\\workLv\\learn\\proj\\bigDataSolve\\FlinkTutorial\\src\\main\\java\\resources\\sensor.txt");

        DataStream<SensorReading> dataStream = inputStream.map( line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        } );

        SplitStream<SensorReading> splitStream = dataStream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading value) {
                return value.getTemperature() > 30 ? Collections.singletonList("high") : Collections.singletonList(
                        "low");
            }
        });

        DataStream<SensorReading> highStream = splitStream.select("high");
        DataStream<SensorReading> lowStream = splitStream.select("low");
        DataStream<SensorReading> allStream = splitStream.select("low", "high");

        highStream.print("high");
        lowStream.print("low");
        allStream.print("all");


        // 2. 合流 connect，将高温流转换成二元组类型，与低温流连接合并之后，输出状态信息

        SingleOutputStreamOperator<Tuple2<String, Double>> warnStream = highStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), value.getTemperature());
            }
        });

        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStreams = warnStream.connect(lowStream);

        SingleOutputStreamOperator<Object> map = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3<String, Double, String>(value.f0, value.f1, "high temp warn");
            }

            @Override
            public Object map2(SensorReading value) throws Exception {
                return new Tuple2<String, String>(value.getId(), "normal");
            }
        });

        map.print();

        lowStream.union(highStream).print("res   ");

        env.execute();


    }

}

