package com.lh.test;


/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 10:31 2021/8/9
 */


// AbstractRichFunction

public class Test {


    public static void main(String[] args) throws Exception {
            T t = new T("bb");
            System.out.println(t.name);


        }


    static class T{
        String name = "aaa";

        public T(String name) {
            this.name = name;
        }
    }

}




