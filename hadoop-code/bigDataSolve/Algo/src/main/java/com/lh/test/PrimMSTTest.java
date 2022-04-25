package com.lh.test;

import com.lh.graph.Edge;
import com.lh.graph.EdgeWeightedGraph;
import com.lh.graph.PrimMST;
import com.lh.linear.Queue;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class PrimMSTTest {

    public static void main(String[] args) throws Exception{


        //准备一副加权无向图
        BufferedReader br = new BufferedReader(new InputStreamReader(PrimMSTTest.class.getClassLoader().getResourceAsStream("min_create_tree_test.txt")));
        int total = Integer.parseInt(br.readLine());
        EdgeWeightedGraph G = new EdgeWeightedGraph(total);

        int edgeNumbers = Integer.parseInt(br.readLine());
        for (int e = 1;e<=edgeNumbers;e++){
            String line = br.readLine();//4 5 0.35

            String[] strs = line.split(" ");

            int v = Integer.parseInt(strs[0]);
            int w = Integer.parseInt(strs[1]);

            double weight = Double.parseDouble(strs[2]);

            //构建加权无向边
            Edge edge = new Edge(v, w, weight);
            G.addEdge(edge);
        }

        //创建一个PrimMST对象，计算加权无向图中的最小生成树
        PrimMST primMST = new PrimMST(G);

        //获取最小生成树中的所有边
        Queue<Edge> edges = primMST.edges();

        //遍历打印所有的边
        for (Edge e : edges) {
            int v = e.either();
            int w = e.other(v);
            double weight = e.weight();
            System.out.println(v+"-"+w+" :: "+weight);

        }


    }
}
