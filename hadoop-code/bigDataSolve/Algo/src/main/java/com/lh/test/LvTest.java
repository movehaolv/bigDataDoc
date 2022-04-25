package com.lh.test;


import com.lh.linear.Queue;
import com.lh.linear.Stack;
import com.lh.priority.IndexMinPriorityQueue;
import com.lh.priority.MinPriorityQueue;
import com.lh.uf.UF_Tree_Weighted;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

public class LvTest{
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(LvTest.class.getClassLoader().getResourceAsStream(
                "min_route_test.txt")));
        int V = Integer.parseInt(br.readLine());
        String s = br.readLine();
        WeiDGraph G = new WeiDGraph(V);
        while ((s=br.readLine())!=null){
            String[] s1 = s.split(" ");
            int v = Integer.parseInt(s1[0]);
            int w = Integer.parseInt(s1[1]);
            double weight = Double.parseDouble(s1[2]);
            DirectedEdge directedEdge = new DirectedEdge(v, w, weight);
            G.addEdge(directedEdge);
        }

        Dijkstra dijkstra = new Dijkstra(G, 0);
        Queue<DirectedEdge> queue = dijkstra.pathTo(0, 3);

        while (!queue.isEmpty()){
            DirectedEdge edge = queue.dequeue();
            System.out.println(edge.from() + "-" + edge.to() + " " +edge.getWeight() );
        }



    }


}

class Dijkstra{

    private DirectedEdge[] edgeTo;
    private double[] distTo;
//    private boolean[] onStack;
    private IndexMinPriorityQueue<Double> pq;

    public Dijkstra(WeiDGraph G, int s) {
        edgeTo = new DirectedEdge[G.getV()];
        distTo = new double[G.getV()];
        for (int i = 0; i < distTo.length; i++) {
            distTo[i] = Double.POSITIVE_INFINITY;
        }
//        onStack = new boolean[G.getV()];
        pq = new IndexMinPriorityQueue<>(G.getV());
        pq.insert(s, 0.0);
        distTo[0] = 0.0;
        while (!pq.isEmpty()){
            relax(G, pq.delMin());
        }
    }

    public void relax(WeiDGraph G, int v){
        for(DirectedEdge e:G.adj(v)){
            double weight = e.getWeight();
            int w = e.to();
            if(distTo[v] + weight < distTo[w]){
                distTo[w] = distTo[v] + weight;
                edgeTo[w] = e;
                if(pq.contains(w)){
                    pq.changeItem(w, distTo[w]);
                }else {
                    pq.insert(w, distTo[w]);
                }
            }
        }

    }


    public boolean hasPathTo(int v){
        return distTo[v] != Double.POSITIVE_INFINITY;
    }


    public Queue<DirectedEdge> pathTo(int s, int v){
        Queue<DirectedEdge> queue = new Queue();
        if(hasPathTo(v)){
            for(int i=v;i!=s;i=edgeTo[i].from()){
                queue.enqueue(edgeTo[i]);
            }
        }
        return queue;
    }

}

class DirectedEdge{
    private int v;
    private int w;
    private double weight;

    public DirectedEdge(int v, int w, double weight) {
        this.v = v;
        this.w = w;
        this.weight = weight;
    }

    public double getWeight() {
        return weight;
    }

    public int from(){
        return v;
    }

    public int to(){
        return w;
    }

}

class WeiDGraph{
    private Queue<DirectedEdge>[] adj;
    private int V;
    private int E;

    public WeiDGraph(int v) {
        V = v;
        adj = new Queue[v];
        for (int i = 0; i < v; i++) {
            adj[i] = new Queue<>();
        }
    }

    public void addEdge(DirectedEdge edge){
        int v = edge.from();
        adj[v].enqueue(edge);
        E++;
    }

    public int getV() {
        return V;
    }

    public int getE() {
        return E;
    }

    public Queue<DirectedEdge> adj(int v){
        return adj[v];
    }

    public Queue<DirectedEdge> edges(){
        Queue<DirectedEdge> queue = new Queue<>();
        for (int i = 0; i < V; i++) {
            for(DirectedEdge e:adj[i]){
                queue.enqueue(e);
            }
        }
        return queue;
    }


}


class Ka{
    private MinPriorityQueue<Edge> queue;
    private Queue<Edge> allEdges;
    private UFTreeWei uf;
    private Queue<Edge> mst;

    public Ka(Queue<Edge> allEdges) {
        this.allEdges = allEdges;
        queue = new MinPriorityQueue<>(allEdges.size());
        uf = new UFTreeWei(allEdges.size());
        mst = new Queue<>();
    }

    public Queue<Edge> getMst() {
        return mst;
    }

    public void edges(){
        for (Edge edge :
                allEdges) {
            queue.insert(edge);
        }
        while (!queue.isEmpty() && mst.size()<=7){
            Edge minEdge = queue.delMin();
            int v = minEdge.either();
            int w = minEdge.other(v);
            if(!uf.connected(v, w)){
                mst.enqueue(minEdge);
                uf.union(v, w);
            }
        }

    }

}


class PrimT{
    private IndexMinPriorityQueue<Double> pq;
    private boolean[] marked;
    private Edge[] edgeTo;
    private double[] distTo;

    public PrimT(WeightedGraph G) {
        pq = new IndexMinPriorityQueue<>(G.V());
        marked = new boolean[G.V()];
        edgeTo = new Edge[G.V()];
        distTo = new double[G.V()];
        for (int i = 0; i < distTo.length; i++) {
            distTo[i] = Double.POSITIVE_INFINITY;
        }
        find(G);
    }

    public void find(WeightedGraph G){
        pq.insert(0, 0.0);
        for(int v=0;v<G.V()-1;v++){
            find(G, pq.delMin());
        }
    }

    public void find(WeightedGraph G, int v){
        marked[v] = true;
        for(Edge e:G.adj(v)){
            int w = e.other(v);
            if(!marked[w]){
                if(e.getWeight() < distTo[w]){
                    distTo[w] = e.getWeight();
                    edgeTo[w] = e;
                    if(pq.contains(w)){
                        pq.changeItem(w, e.getWeight());
                    }else{
                        pq.insert(w, e.getWeight());
                    }
                }
            }
        }
    }

    public Queue<Edge> edges(){
        Queue<Edge> queue = new Queue<>();
        for (int i = 1; i < edgeTo.length; i++) {
            queue.enqueue(edgeTo[i]);
        }
        System.out.println(queue.isEmpty());
        return queue;
    }
}

class WeightedGraph{
    private int V;
    private int E;
    private Queue<Edge>[] adj;

    public WeightedGraph(int v) {
        V = v;
        E = 0;
        adj = new Queue[v];
        for (int i = 0; i < adj.length; i++) {
            adj[i] = new Queue<>();
        }
    }

    public void addEdge(int v, int w, double weight){
        Edge edge = new Edge(v, w, weight);
        adj[v].enqueue(edge);
        adj[w].enqueue(edge);
        E++;
    }

    public Queue<Edge> adj(int v){
        return adj[v];
    }

    public Queue<Edge> edges(){
        Queue<Edge> allEdges = new Queue<>();
        for(int v=0;v<V;v++){
            for(Edge edge:adj[v]){
                if(v<edge.other(v)){
                    allEdges.enqueue(edge);
                }
            }
        }
        return allEdges;
    }


    public int V(){
        return V;
    }

    public int E(){
        return E;
    }


}

class Edge implements Comparable<Edge>{

    private int v;
    private int w;
    private double weight;


    public Edge(int v, int w, double weight) {
        this.v = v;
        this.w = w;
        this.weight = weight;
    }

    public int either(){
        return v;
    }

    public int other(int v){
        if(this.v == v){
            return w;
        }
        return this.v;
    }

    public double getWeight(){
        return weight;
    }


    @Override
    public int compareTo(Edge that) {
        if(this.weight < that.weight){
            return -1;
        }else if(this.weight > that.weight){
            return 1;
        }else {
            return 0;
        }
    }
}


class DepthFirstSort{
    private boolean marked[];
    private Stack<Integer> reversePost;

    public DepthFirstSort(DGraph G) {
        marked = new boolean[G.V()];
        reversePost = new Stack<>();
        for(int v=0;v<G.V();v++){
            if(!marked[v]){
                dfs(G, v);
            }
        }
    }

    public void dfs(DGraph G, int v){
        marked[v] = true;

        for(int w:G.adj(v)){
            if(!marked[w]){
                dfs(G, w);
            }
        }
        reversePost.push(v);
    }


    public Stack<Integer> getReversePost() {
        return reversePost;
    }
}


class DirectCycle{
    private boolean[] marked;
    private boolean[] onStack;
    private boolean hasCycle;

    public DirectCycle(DGraph G) {
        marked = new boolean[G.V()];
        onStack = new boolean[G.V()];
        for(int v=0;v<G.V();v++){
            if(!marked[v]){
                dfs(G, v);
            }
            if(hasCycle){
                break;
            }
        }
    }


    public boolean hasCycle(){
        return hasCycle;
    }

    public void dfs(DGraph G, int v){
        marked[v] = true;
        onStack[v] = true;
        for(int w:G.adj(v)){
            if(!marked[w]){
                dfs(G, w);
            }
            if(onStack[w]){
                hasCycle = true;
                return;
            }
        }
        onStack[v] = false;
    }


}

class DGraph{
    private final int V;
    private int E;
    private Queue<Integer>[] adj;


    public DGraph(int V) {
        this.V = V;
        adj = new Queue[V];
        for (int i = 0; i < adj.length; i++) {
            adj[i] = new Queue<>();
        }
    }

    public void addEdge(int v, int w){
        adj[v].enqueue(w);
        E++;
    }

    public Queue<Integer> adj(int v){
        return adj[v];
    }

    private DGraph reverse(){
        DGraph graph = new DGraph(V);
        for(int v=0;v<V;v++){
            for(int w:adj[v]){
                graph.addEdge(w, v);
            }
        }
        return graph;
    }

    public int V(){
        return V;
    }


}

class BreadthFirstSearch{
    private boolean[] marked;
    private Queue<Integer> queue;
    private Queue<Integer> pathQueue;

    public BreadthFirstSearch(Graph G, int v) {
        this.marked = new boolean[G.V()];
        queue = new Queue<>();
        pathQueue = new Queue<>();
        bfs(G, v);

    }

    private void bfs(Graph G, int v){
        queue.enqueue(v);
        pathQueue.enqueue(v);
        marked[v] = true;
        while(!queue.isEmpty()){
            for(int w:G.get(queue.dequeue())){
                if(!marked[w]){
                    marked[w] = true;
                    pathQueue.enqueue(w);
                    queue.enqueue(w);
                }
            }
        }
    }

    public Queue<Integer> pathBfs(){
        return pathQueue;
    }

    public boolean contains(int v){
        return marked[v];
    }


}


class DeepFirstSearch{
    private boolean[] marked;
    private int count;
    private int[] edgeTo;

    public DeepFirstSearch(Graph G, int v) {
        marked = new boolean[G.V()];
        edgeTo = new int[G.V()];
        dfs(G, v);

    }


    public void dfs( Graph G, int v){
        marked[v] = true;
        for(int w:G.get(v)){
            if(marked[w]){
                continue;
            }
            edgeTo[w] = v;
            dfs(G, w);
            count++;
        }
    }

    public void printlnPath(int x, int y){
        while (y != x){
            System.out.print(y + "->" );
            y = edgeTo[y];
        }
        System.out.print(x);

//        Stack<Integer> stack = new Stack<>();
//        for(int i=y;i!=x;i=edgeTo[i]){
//            stack.push(i);
//        }
//        stack.forEach(ele-> System.out.print(ele + "-"));

    }

    public int count(){
        return count;
    }


}

class Graph{
    private final int V;
    private int E;
    private Queue<Integer>[] adj;

    public Graph(int v) {
        V = v;
        adj = new Queue[v];
        for (int i = 0; i < adj.length; i++) {
            adj[i] = new Queue<>();
        }
    }

    public int V(){
        return V;
    }

    public int E(){
        return E;
    }

    public void addEdge(int v, int w){
        adj[v].enqueue(w);
        adj[w].enqueue(v);
        E++;
    }

    public Queue<Integer> get(int v){
        return adj[v];
    }


}



class UFTreeWei{
    private int count;
    private int[] eleAndGroup;
    private int[] sz;

    public UFTreeWei(int N) {
        this.count = N;
        eleAndGroup = new int[N];
        sz = new int[N];
        Arrays.fill(sz, 1);
        for (int i = 0; i < eleAndGroup.length; i++) {
            eleAndGroup[i] = i;
        }
    }

    public int count(){
        return count;
    }

    public boolean connected(int p, int q){
        return find(p) == find(q);
    }

    public int find(int p){
        if(p == eleAndGroup[p]){
            return p;
        }
        while (p != eleAndGroup[p]){
            p = eleAndGroup[p];
        }
        return p;
    }

    public void union(int p, int q){

        int pGroup = find(p);
        int qGroup = find(q);

        if(pGroup == qGroup){
            return;
        }
        if(sz[pGroup] <= sz[qGroup]){
            eleAndGroup[pGroup] = qGroup;
            sz[qGroup] = sz[qGroup] + sz[pGroup];

        }else {
            eleAndGroup[qGroup] = pGroup;
            sz[pGroup] = sz[pGroup] + sz[qGroup];
        }

        count--;

    }


}



class UF_Tree{
    private int count;
    private int[] eleAndGroup;

    public UF_Tree(int N) {
        this.count = N;
        eleAndGroup = new int[N];
        for (int i = 0; i < eleAndGroup.length; i++) {
            eleAndGroup[i] = i;
        }
    }

    public int count(){
        return count;
    }

    public boolean connected(int p, int q){
        return eleAndGroup[p] == eleAndGroup[q];
    }

    public int find(int p){
        if(p == eleAndGroup[p]){
            return p;
        }
        while (p != eleAndGroup[p]){
            p = eleAndGroup[p];
        }
        return p;
    }

    public void union(int p, int q){

        int pGroup = find(p);
        int qGroup = find(q);

        if(pGroup == qGroup){
            return;
        }

        eleAndGroup[pGroup] = qGroup;

        count--;

    }


}

class UF{
    private int count;
    private int[] eleAndGroup;

    public UF(int N) {
        this.count = N;
        eleAndGroup = new int[N];
        for (int i = 0; i < eleAndGroup.length; i++) {
            eleAndGroup[i] = i;
        }
    }

    public int count(){
        return count;
    }

    public boolean connected(int p, int q){
        return eleAndGroup[p] == eleAndGroup[q];
    }

    public int find(int p){
        return eleAndGroup[p];
    }

    public void union(int p, int q){
        if(connected(p, q)){
            return;
        }
        int pWithGroup = eleAndGroup[p];

        for (int i = 0; i < eleAndGroup.length; i++) {
            if(eleAndGroup[i] == pWithGroup){
                eleAndGroup[i] = eleAndGroup[q];
            }
        }
        count--;

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













