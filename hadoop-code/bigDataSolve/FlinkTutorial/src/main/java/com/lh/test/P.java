package com.lh.test;

import java.util.Arrays;

public class P {

    private static Comparable[] assist;

    public static Boolean less(Comparable a, Comparable b){
        return a.compareTo(b) < 0;
    }

    public static void sort(Comparable[] arr, int lo, int hi){
        if(lo>=hi){
            return;
        }

        int mid = lo + (hi-lo) / 2;
        // 左子组拆分
        sort(arr, lo, mid);
      // 右子组
        sort(arr, mid+1, hi);

        merge(arr, lo, mid, hi);


    }

    public static void merge(Comparable[] arr,int lo, int mid, int hi ){

        int i = lo;
        int p1 = lo;
        int p2 = mid+1;
        while (p1<=mid && p2<=hi){
            if(less(arr[p1], arr[p2])){
                assist[i++] = arr[p1++];
            }else {
                assist[i++] = arr[p2++];
            }
        }

        while (p1<=mid){
            assist[i++] = arr[p1++];
        }

        while (p2<=hi){
            assist[i++] = arr[p2++];
        }

        for(int j=lo;j<=hi;j++){
            arr[j] = assist[j];
        }

    }

    public static void main(String[] args) {
        Integer[] testArr = {8,7,6,5,4,3,2,1};
        assist = new Comparable[testArr.length];
        sort(testArr, 0, 7);
        System.out.println(Arrays.toString(testArr));
    }


}

