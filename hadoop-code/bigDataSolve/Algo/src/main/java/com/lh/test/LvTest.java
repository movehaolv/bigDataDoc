package com.lh.test;


import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Queue;

public class LvTest {
    public static void main(String[] args) {

        int[] nums = {7,1,5,3,6,4};
        new Solution().insertSort(nums);
        System.out.println(Arrays.toString(nums));

    }
}

class Solution {
    public void bubbleSort(int[] nums){
        for(int i=1;i<=nums.length-1;i++){
            for(int j=0;j<=nums.length-1-i;j++){
                if(nums[j]>nums[j+1]){
                    swap(j, j+1, nums);
                }
            }
        }
    }


    public void selectSort(int[] nums){
        for(int i=0;i<=nums.length-2;i++){
            int min=i;
            for(int j=i+1;j<nums.length;j++){
                if(nums[j]<nums[min]){
                    min = j;
                }
            }
            swap(i, min, nums);
        }
    }


    public void insertSort(int[] nums){
        for(int i=1;i<nums.length;i++){
            for(int j=i;j>0;j--){
                if(nums[j]<nums[j-1]){
                    swap(j, j-1, nums);
                }else{
                    break;
                }
            }
        }
    }

    private void swap(int i, int j, int[]nums){
        int tmp = nums[i];
        nums[i] = nums[j];
        nums[j] = tmp;
    }
}

