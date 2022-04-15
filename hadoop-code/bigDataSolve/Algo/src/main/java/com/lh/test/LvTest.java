
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
        HeapSortClass<Integer> heap = new HeapSortClass<>();
        Integer[] a = new Integer[]{4,1,2,3,6,8,1};

        heap.sort(a);
        System.out.println(Arrays.toString(a));


    }
}



class HeapSortClass<T extends Comparable>{

    private static  boolean less(Comparable[] heap, int i, int j) {
        return heap[i].compareTo(heap[j])<0;
    }

    //交换heap堆中i索引和j索引处的值
    private static  void exch(Comparable[] heap, int i, int j) {
        Comparable tmp = heap[i];
        heap[i] = heap[j];
        heap[j] = tmp;
    }

    public void sort(Comparable<T>[] source){

        Comparable<T>[] heap = createHeap(source);
//        for(int i=heap.length-2;i>=1;i--){
//            exch(heap, 1, i+1);
//            sink(heap, 1, i);
//        }
        int N = heap.length - 1;
        while (N!=1){
            exch(heap, 1, N);
            N--;
            sink(heap, 1, N);
        }

        System.arraycopy(heap, 1, source, 0, source.length);
    }

    public Comparable<T>[] createHeap(Comparable<T>[] source){
        Comparable<T>[] heap =  new Comparable[source.length+1];
        System.arraycopy(source, 0, heap, 1, source.length);
        for(int i=source.length/2;i>=1;i--){
            sink(heap, i, source.length);
        }
        return heap;
    }

    public void sink(Comparable<T>[] heap, int target, int range){
        while (target*2 <= range){
            int max=0;
            if(target*2+1 <= range){
                if(less(heap,target*2, target*2+1)){
                    max = target*2+1;
                }else {
                    max = target*2;
                }
            }else{
                max = target*2;
            }
            if(!less(heap, target, max)){
                break;
            }
            exch(heap, target, max);
            target = max;

        }
    }

}

class Heap<T extends Comparable> {

    T[] items;
    int N;

    public Heap(int capacity) {
        items = (T[])new Comparable[capacity + 1];
        N = 0;
    }

    public boolean less(int i, int j){
        return items[i].compareTo(items[j]) < 0;
    }

    public void exch(int i, int j){
        T temp = items[j];
        items[j] = items[i];
        items[i] = temp;
    }

    public void insert(T t){
        items[++N] = t; // 从1开始
        swim(N);
    }

    public void swim(int k){
        while (k > 1){
            if(less(k/2, k)){
                exch(k/2, k);
                k = k/2;
            }else{
                break;
            }
        }
    }

    public T delMax(){
        if(N==0){
            return null;
        }
        T maxItem = items[1];
        exch(1, N);
        items[N] = null;
        N--;
        sink(1);
        return maxItem;
    }

    public void sink1(int k){
        while (2*k <= N){
            if(2*k+1 <= N){
                if(less(2*k, 2*k+1)){
                    if(less(k, 2*k+1)){
                        exch(k, 2*k+1);
                        k = 2*k+1;
                    }else{
                        break;
                    }
                }else{
                    if(less(k, 2*k)){
                        exch(k, 2*k);
                        k = 2*k;
                    }else{
                        break;
                    }
                }
            }else{
                if(less(k, 2*k)){
                    exch(k, 2*k);
                }
                k = 2*k;
            }

          }
    }


    public void sink(int k){
        while (2*k <= N){
            int max = 0;
            if(2*k+1 <= N){
                if(less(2*k, 2*k+1)){
                    max = 2*k+1;
                }else{
                    max = 2*k;
                }
            }else{
                max = 2*k;
            }
            if(!less(k, max)){
                break;
            }
            exch(k, max);
            k = max;

        }
    }



}










