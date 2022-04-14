package com.lh.test;

import com.lh.linear.Queue;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 22:22 2022/4/1
 */


public class Test {
    public static void main(String[] args) {
        fun(1);

    }


    public static void fun(int n){


        if(n==1){
            n += 1;
            System.out.println(n);
        }
        if(n>0){
            n+=1;
            System.out.println(n);
        }

    }
}

class Node{
    Node next;

    public Node(Node next) {
        this.next = next;
    }
}
