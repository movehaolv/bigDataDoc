package com.atguigu;


import java.util.Arrays;

public class App{
    public static void main(String[] args) {
        String a = "atguigu";
        for(int i =0;i<5;i++){
            byte[] bytes = (a + i).getBytes();
            System.out.println(Arrays.toString(bytes));

        }
    }
}

