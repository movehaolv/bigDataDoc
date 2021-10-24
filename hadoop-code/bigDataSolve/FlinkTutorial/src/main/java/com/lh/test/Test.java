package com.lh.test;


import cn.hutool.core.io.LineHandler;
import cn.hutool.core.lang.copier.Copier;
import com.lh.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntConsumer;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 10:31 2021/8/9
 */


// AbstractRichFunction

public class Test {


    public static void main(String[] args) {
        List<Integer> list = new ArrayList<Integer>() {{
            add(1);
            add(3);
            add(5);
        }};

        List<Integer> l1 = new ArrayList<>();
        Copier<ArrayList> arrayListCopier1 = ArrayList::new;
        IntConsumer intConsumer = l1::add;
        list.iterator().forEachRemaining(x -> intConsumer.accept(x));

//        list.iterator().forEachRemaining(ArrayList::add);

        System.out.println(l1);

        LineHandler startsWith = Something::startsWith;
        startsWith.handle("abc");
        LineHandler lineHandler1 = Something::new;
        LineHandler lineHandler = Something::new;
        lineHandler.handle("req");

        Copier<ArrayList> arrayListCopier = ArrayList::new;
    }





}

class Something {

    // constructor methods
    Something() {}

    Something(String something) {
        System.out.println(something);
    }

    // static methods
    static String startsWith(String s) {
        System.out.println("startsWith " + s);
        return String.valueOf(s.charAt(0));
    }

    // object methods
    String endWith(String s) {
        System.out.println(s);
        return String.valueOf(s.charAt(s.length()-1));
    }

    void endWith() {}
}




