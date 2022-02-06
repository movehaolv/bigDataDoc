package com.atguigu.hive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 14:05 2022/1/6
 */


public class Test {
    public static void getPartition(String name){
        System.out.println(name + "  " + (name.hashCode() & Integer.MAX_VALUE) % 3 );

    }

    public static void main(String[] args) {
        List<String> list = Arrays.asList("tom", "jan", "alice", "jan", "bee", "jan", "tom", "alice");
        list.forEach(ele -> getPartition(ele));
    }

}



