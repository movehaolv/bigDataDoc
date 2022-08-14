package com.lh.test;


import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;

public class LvTest {
    public static void main(String[] args) {
        System.out.println(LvTest.class.getClassLoader().getResource("road_find.txt").getPath());
    }
}


// [0,1,2,4,4,4,5,6,6,7] [4,5,6,6,7,0,1,2,4,4]
class Solution {
    public int search(int[] nums, int target) {
        int len = nums.length;
        if (len == 0) {
            return -1;
        }
        int l = 0;
        int r = len - 1;
        while (l <= r) {
            int mid = (l + r) / 2;
            if (nums[mid] == target) {
                return mid;
            }
            if (nums[0] <= nums[mid]) { // 有序部分为前半段，在前半段判断  0也可以用l替代
                if (nums[0] <= target && target < nums[mid]) {
                    r = mid - 1;
                } else {
                    l = mid + 1;
                }
            } else { // 有序部分为后半段，在后半段判断
                if (target > nums[mid] && target <= nums[len - 1]) {  // len-1 可以用r替代
                    l = mid + 1;
                } else {
                    r = mid - 1;
                }
            }
        }
        return -1;
    }
}