
package com.lh.test;



import com.lh.linear.Queue;
import com.lh.symbol.OrderSymbolTable;

import java.sql.Struct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 13:41 2021/9/21          Comparable[] a = {6,5,2,1,3,4,5};
 */

public class LvTest{

    public static void main(String[] args) {
//        BinTree<Integer, String> tree = new BinTree();
//        //测试插入
//        tree.put(20,"20");
//        tree.put(10,"10");
//        tree.put(8,"8");
//        tree.put(6,"6");
//        tree.put(9,"9");
//        tree.put(16,"16");
//        tree.put(14,"14");
//        tree.put(17,"17");
//        tree.put(12,"12");
//        tree.put(25,"25");
////        tree.put(23,"23");
////        tree.put(26,"26");
//
//
//
//
//        Queue<Integer> keys = tree.midEr();
//        for (Integer key : keys) {
//            String value = tree.get(key);
//            System.out.println(key+"----"+value);
////        }

//    }

// down down up down down up up
        PageFold<String> stringPageFold = new PageFold<>();
        stringPageFold.printTree(stringPageFold.createTree(3));

    }




}

class PageFold<T>{

    public void printTree(Node<T> n){
        if(n==null){
            return;
        }
        if(n.left != null){
            printTree(n.left);
        }
        System.out.println(n.item);
        if(n.left != null){
            printTree(n.right);
        }

    }

    public Node<String> createTree(int n){
        Node<String> root = null;
        for(int i=0;i<n;i++) {
            if (i == 0) {
                root = new Node<>("down", null, null);
                continue;
            }
            Queue<Node<String>> queue = new Queue<>();
            queue.enqueue(root);
            while (!queue.isEmpty()){
                Node<String> x = queue.dequeue();
                if(x.left != null){
                    queue.enqueue(x.left);
                }
                if(x.right != null){
                    queue.enqueue(x.right);
                }
                if(x.left ==null && x.right == null){
                    x.left = new Node<>("down", null, null);
                    x.right = new Node<>("up", null, null);
                }
            }

        }
        return root;

    }

    static class Node<T>{
        T item;
        Node<T> left;
        Node<T> right;

        public Node(T item, Node<T> left, Node<T> right) {
            this.item = item;
            this.left = left;
            this.right = right;
        }
    }
}


class BinTree<K extends Comparable<K>, V>{

    Node root;
    int N;

    private class Node{
        K key;
        V val;
        Node left;
        Node right;

        public Node(K key, V val, Node left, Node right) {
            this.key = key;
            this.val = val;
            this.left = left;
            this.right = right;
        }
    }

    public BinTree() {
        root = null;
        N = 0;
    }

    public int maxDepth(){

        return maxDepth(root);
    }

    public int maxDepth(Node n){
        if(n==null){
            return 0;
        }

        int max = 0;
        int maxLeft = 0;
        int maxRight = 0;

        if(n.left != null){
            maxLeft = maxDepth(n.left);
        }
        if(n.right != null){
            maxRight = maxDepth(n.right);
        }

        max = maxLeft > maxRight?maxLeft+1:maxRight+1;

        return max;
    }



    public List<Integer> max(){
        if(root == null){
            return null;
        }
        List<Integer> list = new ArrayList<>();
        max(root, 1, list);
        return list;
    }

    public void max(Node n,int sum, List<Integer> list){
        if(n.left == null && n.right == null){
            list.add(sum);
            return;
        }
        if(n.left != null){
            max(n.left, sum+1, list);
        }
        if(n.right != null){
            max(n.right, sum+1, list);
        }

    }

    public Queue<V> ciEnr(){
        Queue<Node> keys = new Queue<>();
        Queue<V> values = new Queue<>();
        if(root == null){
            return null;
        }
        keys.enqueue(root);
        ciEnr(keys, values);
        return values;
    }

    public void ciEnr(Queue<Node> keys, Queue<V> values){
        if(keys.isEmpty()){
            return;
        }
        Node n = keys.dequeue();
        values.enqueue(n.val);
        if(n.left != null){
            keys.enqueue(n.left);
        }
        if(n.right != null){
            keys.enqueue(n.right);
        }
        ciEnr(keys, values);
    }

    public int size(){
        return N;
    }

    public void put(K key, V val){
        root = put(root, key, val);
    }

    public Node put(Node n,K key, V val){
        if(n == null){
            N++;
            return new Node(key, val, null, null);
        }

        int comp = n.key.compareTo(key);
        if(comp < 0){
            n.right = put(n.right, key, val);
        }else if(comp > 0) {
            n.left = put(n.left, key, val);
        }else{
            n.val = val;
        }

        return n;
    }

    public V get(K key){
         return get(root, key);
    }
    public V get(Node n, K key){
        if(n == null){
            return null;
        }
        int com = key.compareTo(n.key);
        if(com < 0){
            return get(n.left, key);
        }else if(com > 0){
            return get(n.right, key);
        }else {
            return n.val;
        }
    }

    public void delete(K key){
        root = delete(root, key);
    }

    public Node delete(Node x, K key){
        if(x == null){
            return x;
        }
        int com = x.key.compareTo(key);
        if(com > 0){
            x.left = delete(x.left, key);
        }else if(com < 0){
            x.right = delete(x.right, key);
        }else {
            N--;

            if(x.left == null){
                return x.right;
            }
            if(x.right == null){
                return x.left;
            }

            Node n = x.right;
            Node minNode = null;
            while (n.left !=null){
                if(n.left.left == null){
                    minNode = n.left;
                    n.left = null;

                }else{
                    n = n.left;
                }
            }

            minNode.left = x.left;
            minNode.right = x.right;

            return minNode;

        }
        return x;

    }

    public Queue<K> midEr(){
        Queue<K> keys = new Queue<>();
        midEr(root, keys);
        return keys;
    }

    public void midEr(Node n, Queue<K> keys){
//        if(n == null){
//            System.out.println("aaaaa");
//            return;
//        }
        if(n.left != null){
            midEr(n.left, keys);
        }
        keys.enqueue(n.key);
        if(n.right != null){
            midEr(n.right, keys);
        }

    }

}




