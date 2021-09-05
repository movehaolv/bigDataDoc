package com.lh.wc;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.flink.api.common.time.Time;
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
    public static void main(String[] args) throws IOException {

        System.out.println(System.currentTimeMillis());
        System.out.println(Time.minutes(1));
        System.out.println(Time.minutes(1).toMilliseconds());


//        Boolean flag = false;
//        List<Boolean> list = new ArrayList<>(Arrays.asList(true, false, false, false, false));
//
//        for(Boolean i:list){
//            flag |= i;
//            System.out.println("ele: " + i + "  flag: " +flag );
//        }
//        System.out.println(flag);
//        Boolean f = false;
//        System.out.println(f |= false);
        Path path = new Path("D:\\workLv\\learn\\proj\\bigDataSolve\\FlinkTutorial\\src\\main\\java\\resources\\sensor.txt");
        System.out.println(path.getName());
    }

    public static Boolean test(int a){
        if(a == 0){
            return false;
        }else{
            return true;
        }
    }
}

class Animal{
    protected String name;

    protected Integer age;


    public Animal(String name){
        this.name = name;
    }

    public void fun(){}

}

class Bird extends Animal{

    protected String name;

    public String sex;

    public Bird(String name) {
    super(name);
    }

    public void fun(){}
}

abstract class Man{
    public static void man(){
        System.out.println("man");
    }
}
