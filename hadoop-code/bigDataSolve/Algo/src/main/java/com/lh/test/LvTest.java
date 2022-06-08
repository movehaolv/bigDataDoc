package com.lh.test;


import com.lh.graph.Digraph;
import com.lh.graph.DirectedCycle;
import com.sun.deploy.util.StringUtils;

import javax.swing.tree.TreeNode;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
    String name;

    public P(String name) {
        this.name = name;
    }
}
public class LvTest {

    public static void main(String[] args) throws Exception {
        Path path = Paths.get("E:\\cc\\aa.py");
        findJava(path);

    }


    public static void findJava(Path path){
        if(Files.isDirectory(path)){
            try {
                DirectoryStream<Path> paths = Files.newDirectoryStream(path);
                for(Path file :paths){
                    if(Files.isRegularFile(file)){
                        System.out.println(file.getFileName());
                    }else{
                        System.out.println( file);
                        findJava(file);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else{
            System.out.println(path.getFileName());
        }
    }

}

class SyncThread implements Runnable{
    private static int count=0;

    @Override
    public void run() {
            synchronized(this) {
                for (int i = 0; i < 5; i++) {
                        try {
                            System.out.println(Thread.currentThread().getName() + ":" + (count++));
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }

}

