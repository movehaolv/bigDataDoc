package com.lh.test;

import java.util.Arrays;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 13:41 2021/9/21
 */
import java.util.Arrays;


public class LvTest {
    public static void main(String[] args) {

        Person []persons = new Person[]{
                new Person("张三",15),
                new Person("李四",25),
                new Person("王五",20)
        };
        Arrays.sort(persons);
        System.out.println(Arrays.toString(persons));
        System.out.println(persons[0]==persons[1]);
        System.out.println(persons[0].equals(persons[1]));

    }

    static class Person{
        int age;
        String name;

        public Person(String name,int age){
            this.name = name;
            this.age = age;
        }

        @Override
        public String toString() {
            // TODO Auto-generated method stub
            return "姓名:"+name+","+"年龄:"+age+";";
        }

//        @Override
        public int compareTo(Person o) {
            // TODO Auto-generated method stub
            if(this.age>o.age){
                return 1;
            }else if(this.age<o.age){
                return -1;
            }
            //当然也可以这样实现
            // return Integer.compare(this.age, o.age);
            return 0;

        }
    }
}


