package com.lh.test;

import com.lh.linear.Queue;
import com.lh.tree.BinaryTree;

public class BinaryTreeTest {
    public static void main(String[] args) {
        //创建二叉查找树对象
        BinaryTree<Integer, String> tree = new BinaryTree<>();
//
//        //测试插入
//        tree.put(1,"张三");
//        tree.put(2,"李四");
//        tree.put(3,"王五");
//        System.out.println("插入完毕后元素的个数："+tree.size());
//
//        //测试获取
//        System.out.println("键2对应的元素是："+tree.get(2));
//
//        //测试删除
//
//        tree.delete(3);
//        System.out.println("删除后的元素个数："+tree.size());
//        System.out.println("删除后键3对应的元素:"+tree.get(3));


        //测试插入
        tree.put(20,"20");
        tree.put(10,"10");
        tree.put(8,"8");
        tree.put(6,"6");
        tree.put(9,"9");
        tree.put(16,"16");
//        tree.put(14,"14");
        tree.put(17,"17");
        tree.put(12,"12");
        tree.put(25,"25");
        tree.put(23,"23");
        tree.put(26,"26");
        System.out.println("插入完毕后元素的个数："+tree.size());

        //测试获取
        System.out.println("键2对应的元素是："+tree.get(2));

        //测试删除

        tree.delete(10);
        System.out.println("删除后的元素个数："+tree.size());
        System.out.println("删除后键3对应的元素:"+tree.get(10));


        Queue<Integer> keys = tree.midErgodic();
        for (Integer key : keys) {
            String value = tree.get(key);
            System.out.println(key+"----"+value);
        }
    }

}
