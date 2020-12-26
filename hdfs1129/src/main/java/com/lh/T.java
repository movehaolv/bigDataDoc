package com.lh;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 17:42 2020/12/10
 */


public class T {
    public static void main(String[] args) {
        int i,j,k;
        for(i=0;i<3;i++){
            for(j=1;j<4;j++){
                for(k=2;k<5;k++){
                    if((i==j)&&(j==k)){
                        System.out.println(i);
                    }
                }
            }
        }
    }
}



class ClassDemo{
    public static int sum=1;
    public ClassDemo(){
        sum = sum + 5;
    }
}