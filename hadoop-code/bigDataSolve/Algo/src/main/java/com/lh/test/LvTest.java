
package com.lh.test;



import com.lh.linear.LinkList;

import java.util.Iterator;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 13:41 2021/9/21          Comparable[] a = {6,5,2,1,3,4,5};
 */



public class LvTest{

    public static void main(String[] args) {

        TwoLinkedList<String> sl = new TwoLinkedList<>();
        sl.insert("姚明");
        sl.insert("科比");
        sl.insert("麦迪");
        sl.insert(2,"詹姆斯");

        for (String s : sl) {
            System.out.println(s);
        }

        System.out.println("--------------------------------------");
        System.out.println("第一个元素是："+sl.getFirst());
        System.out.println("最后一个元素是："+sl.getLast());

        System.out.println("------------------------------------------");

        //测试获取
        String getResult = sl.get(1);
        System.out.println("获取索引1处的结果为："+getResult);
        //测试删除
        String removeResult = sl.remove(0);
        System.out.println("删除的元素是："+removeResult);
        //测试清空
        sl.clear();
        System.out.println("清空后的线性表中的元素个数为:"+sl.length());




    }

    static class TwoLinkedList<T> implements Iterable<T>{
        private Node head;
        private Node last;
        private int N;

        public TwoLinkedList() {
            head = new Node(null, null, null);
            last = null;
            N = 0;
        }

        public int length(){
            return N;
        }

        public void clear(){
            head.next = null;
            last = null;
            N = 0;
        }

        public Boolean isEmpty(){
            return N == 0;
        }

        public T get(int i){
            Node cur = head;
            for(int index=0;index<=i;index++){
                cur = cur.next;
            }
            return cur.item;
        }

        public void insert(T item){
            if(isEmpty()){
                Node newNode = new Node(head, item, null);
                head.next = newNode;
                last = newNode;
            }else {
                Node oldLast = last;
                Node newNode = new Node(oldLast, item, null);
                oldLast.next = newNode;
                last = newNode;
            }
            N++;
        }

        public void insert(int i, T item){
            Node cur = head;
            if(i==N){
                insert(item);
            }else if(i<N) {
                for (int index = 0; index <= i; index++) {
                    cur = cur.next;
                }
                Node pre = cur.pre;
                Node next = cur;
                Node newNode = new Node(pre, item, next);
                pre.next = newNode;
                next.pre = newNode;
            }
            N++;
        }

        public T remove(int i){ // -----------------------------------------------------+++++++++++++++
            if(N-1 <i){
                return null;
            }else if(i==N-1){ // 删除的是最后一个元素
                Node pre = last.pre;
                pre.next = null;
                last.pre = null;
                last = pre;
                N--;
                return last.item;
            }else {
                Node cur = head;
                for (int index = 0; index <= i; index++) {
                    cur = cur.next;
                }
                Node pre = cur.pre;
                Node next = cur.next;
                pre.next = next;
                next.pre = pre;
                N--;
                return cur.item;
            }

        }

        public int indexOf(T item){
            Node cur = head;
            for(int i=0;i<N;i++){
                cur = cur.next;
                if(cur.item.equals(item)){
                    return i;
                }
            }
            return -1;
        }

        public T getFirst(){
            if(N==0){
                return null;
            }
            return head.next.item;
        }

        public T getLast(){
            if (isEmpty()){
                return null;
            }
            return last.item;
        }




        class Node{
            private Node pre;
            private T item;
            private Node next;

            public Node(Node pre, T item, Node next) {
                this.pre = pre;
                this.item = item;
                this.next = next;
            }
        }

        @Override
        public Iterator<T> iterator() {
            return new TWIterator();
        }

        class TWIterator<T> implements Iterator{

            private Node n;

            public TWIterator() {
                this.n = head;
            }

            @Override
            public boolean hasNext() {
                return n.next != null;
            }

            @Override
            public Object next() {
                n = n.next;
                return n.item;
            }
        }


    }



    static class LinkedList<T> implements Iterable<T>{
        private Node head;
        int N;

        private class Node<T>{
            T item;
            Node next;

            private Node(T item, Node next) {
                this.item = item;
                this.next = next;
            }
        }

        private LinkedList() {
            this.head = new Node(null, null);
            N = 0;
        }

        public Boolean isEmpty(){
            return N == 0;
        }

        public int length(){
            return N;
        }

        public T get(int index){
            Node<T> cur = head;
            for(int i=0;i<=index;i++){
                cur = cur.next;
            }
            return cur.item;
        }

        public void insert(T item){
            Node cur = head;
            while(cur.next != null){
                cur = cur.next;
            }
            Node nextNode = new Node(item, null);
            cur.next = nextNode;
            N++;
        }

        public void insert(int index, T item){
            Node pre = head;
            for(int i=0;i<index;i++){
                pre = pre.next;
            }
            Node cur = new Node(item, pre.next);
            pre.next = cur;
            N++;
        }

        public T remove(int index){
            Node<T> pre = head;
            for(int i=0;i<index;i++){
                pre = pre.next;
            }
            Node<T> cur = pre.next;
            pre.next = cur.next;
            N--;
            return cur.item;
        }

        public int indexOf(T item){
            Node cur = head;
            for(int i=0;i<N;i++){
                cur = cur.next;
                if(cur.item.equals(item)){
                    return i;
                }
            }
            return -1;
        }

        public void clear(){
            N = 0;
        }

        public Node reverse(Node cur){
            if(cur.next == null){
                head.next = cur;
                return cur;
            }
            Node pre = reverse(cur.next);
            pre.next = cur;
            cur.next = null;
            return cur;
        }

        public void reverse(){
            if(N==0){
                return;
            }
            reverse(head.next);
        }


        @Override
        public Iterator<T> iterator() {
            return new LIterator();
        }

        class LIterator implements Iterator<T>{

            private Node<T> n;

            public LIterator() {
                n = head;
            }

            @Override
            public boolean hasNext() {
                return n.next != null;
            }

            @Override
            public T next() {
                n = n.next;
                return n.item;
            }
        }

    }

}


