package com.lh.test;

import com.lh.priority.IndexMinPriorityQueue;

public class IndexMinPriorityQueueTest {


    public static void main(String[] args) {
        //创建索引最小优先队列对象
        IndexMinPriorityQueue<String> queue = new IndexMinPriorityQueue<>(10);

        //往队列中添加元素
        queue.insert(3,"A");
        queue.insert(1,"C");
        queue.insert(2,"F");
        queue.insert(5,"B");



        //测试修改
//        queue.changeItem(2,"B");

//        queue.delete(1);

        //测试删除
//        while(!queue.isEmpty()){
//            int index = queue.delMin();
//            System.out.print(index+" ");
//        }


    }
}
