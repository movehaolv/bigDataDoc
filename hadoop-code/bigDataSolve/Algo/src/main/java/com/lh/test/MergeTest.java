package com.lh.test;

import com.lh.sort.Merge;

import java.util.Arrays;

public class MergeTest {

    public static void main(String[] args) {
        Integer[] a = {8,7,6,5,4,3,2,1};
        Merge.sort(a);
        System.out.println(Arrays.toString(a));//{1,2,3,4,5,6,7,8}

    }
}
