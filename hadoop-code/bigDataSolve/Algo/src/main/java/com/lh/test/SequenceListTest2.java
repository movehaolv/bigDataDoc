package com.lh.test;

import com.lh.linear.SequenceList;

import java.util.Arrays;

public class SequenceListTest2 {

    public static void main(String[] args) {
        SequenceList<String> sl = new SequenceList<>(5);
        sl.insert("张三");
        sl.insert("李四");
        sl.insert("王五");
        sl.insert("赵六");
        sl.remove(2);
        for(String s :sl){
            System.out.println(s);
        }

    }
}
