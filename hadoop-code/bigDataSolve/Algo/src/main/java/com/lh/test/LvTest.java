package com.lh.test;


import com.lh.tree.RedBlackTree;

public class LvTest{
    static Integer a;
    public static void main(String[] args) {
        RBTree<String, String> rbTree = new RBTree<>();
        //往树中插入元素
        rbTree.put("2","张三");
        rbTree.put("1","李四");
        rbTree.put("3","王五");
        //从树中获取元素
        String r1 = rbTree.get("1");
        System.out.println(r1);


        String r2 = rbTree.get("2");
        System.out.println(r2);

        String r3 = rbTree.get("3");
        System.out.println(r3);
        System.out.println(rbTree.size());

    }


}


class RBTree<K extends Comparable<K>, V>{

    private static final boolean RED = true;
    private static final boolean BLACK = true;
    private Node root;
    private int N;

    public RBTree() {

    }
    private Node rotateLeft(Node h){
        Node x = h.right;
        h.right = x.left;
        x.left = h;
        x.color = h.color;
        h.color = RED;
        return x;

    }

    private Node rotateRight(Node h){
        Node x = h.left;
        h.left = x.right;
        x.right = h;
        x.color = h.color;
        h.color = RED;
        return x;



    }
    public int size() {
        return N;
    }
    private void flip(Node h){
        h.left.color = BLACK;
        h.right.color = BLACK;
        h.color = RED;
    }

    public void put(K key, V value){
        root = put(root, key,value);
        root.color = BLACK;
    }

    private boolean isRed(Node x){
        if(x==null){
            return false;
        }
        return x.color == RED;
    }

    public Node put(Node h, K key, V value){
        if(h == null){
            N++;
            return new Node(key, value, null, null, RED);
        }
        int comp = key.compareTo(h.key);
        if(comp < 0){
            h.left = put(h.left, key,value);
        }else if(comp >0){
            h.right = put(h.right, key, value);
        }else {
            h.value =value;
        }

        if(isRed(h.left.left) && isRed(h.left)){
            h = rotateRight(h);
        }
        if(isRed(h.right)){
            h = rotateLeft(h);
        }
        if(isRed(h.left) && isRed(h.right)){
            flip(h);
        }
        return h;

    }

    public V get(K key){
        return get(root, key);
    }


    private V get(Node x, K key){
        if(x==null){
            return null;
        }
        int comp = key.compareTo(x.key);
        if(comp<0){
           return get(x.left, key);
        }else if(comp>0){
            return get(x.right, key);
        }else {
            return x.value;
        }
    }

    class Node{
        K key;
        V value;
        Node left;
        Node right;
        Boolean color;

        public Node(K key, V value, Node left, Node right, Boolean color) {
            this.key = key;
            this.value = value;
            this.left = left;
            this.right = right;
            this.color = color;
        }
    }

}













