package com.atguigu.mr.flowsum.backup;

import java.util.Comparator;
import java.util.TreeSet;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 15:29 2020/12/20
 */


public class Test {
    public static void main(String[] args) {
        Student s1 = new Student("tom");
        Student s2 = new Student("tom");
        boolean equals = s1.equals(s2);
        System.out.println(equals);
        System.out.println(s1.hashCode());
        System.out.println(s2.hashCode());
    }
}

class Student {
    private String name;

    public Student(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object obj) {
        if(this.name == null || !(obj instanceof Student )){
            return false;
        }
        if(this == obj){
            return true;
        }
        Student s = (Student) obj;
        return this.name == s.name;
    }

    @Override
    public int hashCode() {
        return this.name.hashCode();
    }
}

