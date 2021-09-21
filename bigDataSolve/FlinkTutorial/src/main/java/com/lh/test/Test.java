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

        Man man = new Man();
        man.SetCnt();
        System.out.println(man.count);
        man.func();

    }

    public static class Man{
        int count;

        public Integer SetCnt(){

            return count++;
        }

        public void func(){
            System.out.println(111);
        }

    }

}




