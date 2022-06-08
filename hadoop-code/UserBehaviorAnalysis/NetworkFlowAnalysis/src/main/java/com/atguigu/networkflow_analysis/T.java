package com.atguigu.networkflow_analysis;
import java.util.*;
import java.lang.Comparable;

/**
 * @desc "Comparator"和“Comparable”的比较程序。
 *   (01) "Comparable"
 *   它是一个排序接口，只包含一个函数compareTo()。
 *   一个类实现了Comparable接口，就意味着“该类本身支持排序”，它可以直接通过Arrays.sort() 或 Collections.sort()进行排序。
 *   (02) "Comparator"
 *   它是一个比较器接口，包括两个函数：compare() 和 equals()。
 *   一个类实现了Comparator接口，那么它就是一个“比较器”。其它的类，可以根据该比较器去排序。
 *
 *   综上所述：Comparable是内部比较器，而Comparator是外部比较器。
 *   一个类本身实现了Comparable比较器，就意味着它本身支持排序；若它本身没实现Comparable，也可以通过外部比较器Comparator进行排序。
 */
public class T {
    public static void main(String[] args) {
        System.out.println(1 << 4);
        System.out.println(10 & (1<<2-1));
        System.out.println(45495360552L & (536870912-1));
    }
}
// 100
// 011
// 010

// result 45495360552 398203944  536870912
