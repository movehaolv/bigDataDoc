package com.lh.test;


import com.lh.graph.Digraph;
import com.lh.graph.DirectedCycle;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.function.IntConsumer;
import java.math.BigInteger;

class P{
    String name="tom";
}
public class LvTest {

    public static void main(String[] args) throws Exception {
        System.out.println(Integer.MAX_VALUE);

    }

    public static int find(int[] nums, int target, int s, int e){
        if(s>e){
            return -1;
        }
        int len = nums.length;
        int half = (e-s) / 2 + s;
        if(nums[half] == target){
            return half;
        }
        if(nums[half] < target){
            return find(nums, target, half+1, e);
        }
        else{
            return find(nums, target, s, half-1);
        }


    }


    static void f(ListNode n){
        n.val=100;
    }

}

class ListNode{
    ListNode next;
    int val;

    public ListNode() {
    }


    public ListNode(int val) {
        this.val = val;
    }

    public ListNode(ListNode next, int val) {
        this.next = next;
        this.val = val;
    }
}



