package com.lh.test;


import com.lh.wc.TT;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 10:31 2021/8/9
 */


// AbstractRichFunction

public class Test {
    public static void main(String[] args) throws Exception {
        P p = new P();
        System.out.println(p);

    }


}


abstract class Animal{

    abstract void t1();
    abstract void t2();

    void f1(){}

}

abstract class Bird extends Animal{

    @Override
    void t1() {
        System.out.println("a");
    }
}

class B extends Bird{

    @Override
    void t2() {
    }
}


