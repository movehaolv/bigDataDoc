package com.lh.test;

import akka.stream.impl.ReducerState;
import com.lh.apitest.beans.SensorReading;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.common.inject.Singleton;
import org.mortbay.util.SingletonList;
import scala.App;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 14:31 2021/8/10
 */


public class P {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.socketTextStream("node01", 7777);
        SingleOutputStreamOperator<SensorReading> mapStream = inputStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.parseDouble(fields[2]));
            }
        });

//        SingleOutputStreamOperator<Integer> mapCnt = mapStream.map(new OperatorState());
//        mapCnt.print();

        mapStream.keyBy("id").map(new KeyedState()).print();


        env.execute();


    }


    public static class KeyedState extends RichMapFunction<SensorReading, Integer>{

        private ValueState<Integer> valueState;
        private ListState<String> listState;
        private MapState mapState;
        private ReducerState reducerState;

        @Override
        public Integer map(SensorReading value) throws Exception {
            Integer value1 = valueState.value();
            if(value1 == null){
                value1 = 0;
            }
            Integer cnt = value1 + 1;
            valueState.update(cnt);
            for(String str:listState.get()){
                System.out.println("listState " + str);
            }
            listState.add(value.toString());

            return  cnt;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("valueState", Integer.class));
            listState = getRuntimeContext().getListState(new ListStateDescriptor<String>("listState", String.class));

        }
    }


    public static class OperatorState implements MapFunction<SensorReading, Integer>, ListCheckpointed<Integer> {

        private Integer count = 0;

        @Override
        public Integer map(SensorReading value) throws Exception {
            count++;
            return count;
        }

        @Override
        public List snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(count);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            for(Integer cnt: state){
                count += cnt;
            }
        }


    }



}


