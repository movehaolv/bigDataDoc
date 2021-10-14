package com.lh;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 14:10 2021/9/16
 */


public class BubbleSort {
    public static void main(String[] args) {
        int[] sorts = bubbleSort(new int[]{3,2,1,7,5,6});
        for(int ele:sorts){
            System.out.println(ele);
        }
    }

    public static int[] bubbleSort(int[] arr) {
        for (int i = arr.length - 1; i > 0; i--) {
            for (int j = 0; j < i; j++) {
                exchange(arr, j, j+1);
            }
        }
        return arr;
    }

    public static void exchange(int[] arr, int a, int b) {
        if (arr[a] > arr[b]) {
            int temp = arr[a];
            arr[a] = arr[b];
            arr[b] = temp;
        }

    }


}
