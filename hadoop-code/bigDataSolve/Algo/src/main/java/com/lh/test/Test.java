package com.lh.test;


import com.lh.linear.Stack;

import java.util.ArrayList;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 22:22 2022/4/1
 */


public class Test {
    public static void main(String[] args) {

        Stack<Integer> stack = new Stack<>();
        stack.push(1);
        stack.push(2);
        stack.push(3);
        System.out.println(stack.pop());

    }


    public static void fun(int n) {


        if (n == 1) {
            n += 1;
            System.out.println(n);
        }
        if (n > 0) {
            n += 1;
            System.out.println(n);
        }

    }
}
