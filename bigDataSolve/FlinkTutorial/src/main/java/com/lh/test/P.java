package com.lh.test;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;
import scala.App;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 14:31 2021/8/10
 */


public class P {
    public static void main(String[] args) {


    }

    public static <T> void test(Collection<? super Base> collection) {
        collection.add(new Base());
        collection.add(new Sub());
        collection.forEach((e) -> System.out.println(e));

    }
}
class Base{}

class Sub extends Base{

    void fun(){
        System.out.println(:fun);
    }
}
