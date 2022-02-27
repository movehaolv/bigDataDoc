##### windowsAll && window

```tex
需求：将无限流数据按5秒一个窗口，处理数据批量写入phoenix

实现方式：有6个topic数据是WindowAll的方式，有1个topic数据是KeyBy Window

1）.timeWindowAll(Time.seconds(5)).apply(new AllWindowFunction ...)

2）.keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(5))).apply(new WindowFunction ...)

对比：

    WindowAll并行度只能1，不可设置，适用于数据量不大的情况。

    KeyBy Window的并行度，可按Key设置，适用于大数据量。
```



##### OnTimer触发

```java
// Flink保证同步调用onTimer()和processElement() 。因此用户不必担心状态的并发修改。
// FLink的定时器触发，是通过waterMark进行触发的，混乱程度 2s,  定时器只能触发一次ctx.timerService().currentWatermark()为上一个WM
// 注册的定时器是针对key的，使用的是socketTextStream，时间间隔>200ms,所以每条数据会生成WM
@Override
public void processElement(SensorReading value, Context ctx, Collector<Integer> out) throws Exception {
    System.out.println("上一个WM " + ctx.timerService().currentWatermark() + " 当前WM " + (value.getTimestamp() * 1000L - 2000L) + " 事件时间 " + (value.getTimestamp() * 1000L) + " 定时器触发时间 " + (value.getTimestamp() * 1000L + 1000L));
    ctx.timerService().registerEventTimeTimer(value.getTimestamp() * 1000L + 1000L);
}
@Override
public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
    System.out.println(timestamp + " 定时器触发"); // timestamp：registerEventTimeTimer注册的时间，这里的ctx.timerService().currentWatermark()为当前WM-1

}

上一个WM -9223372036854775808 当前WM 1547718197000 事件时间 1547718199000 定时器触发时间 1547718200000
上一个WM 1547718197000 当前WM 1547718198000 事件时间 1547718200000 定时器触发时间 1547718201000
上一个WM 1547718198000 当前WM 1547718199000 事件时间 1547718201000 定时器触发时间 1547718202000
上一个WM 1547718199000 当前WM 1547718200000 事件时间 1547718202000 定时器触发时间 1547718203000  // 该条数据的WM为1547718200000，触发第一条数据1547718200000 定时器触发
上一个WM 1547718200000 当前WM 1547718203000 事件时间 1547718205000 定时器触发时间 1547718206000  //  该条数据的WM为1547718203000 触发第二三四条数据
1547718201000 定时器触发
1547718202000 定时器触发
1547718203000 定时器触发
```

##### 聚合

- keyBy().timeWindow

```java
增量聚合 aggregate(new AggregateFunction） 
               
// ProcessWindowFunction是WindowFunction的加强版，可以获得上下文
全量聚合 process(new ProcessWindowFunction) apply(new WindowFunction) // 一次性处理一个窗口的数据
.aggregate(new AggregateFunction(), new WindowFunction()); // 前增量后全量
.aggregate(new AggregateFunction(), new ProcessWindowFunction()); // ProcessWindowFunction可获取上下文 

单次处理元素
.process(new KeyedProcessFunction()) // 处理每个元素，底层API 
<==>
.timeWindowAll(Time.hours(1))               
.trigger( new MyTrigger() )
.process( new ProcessAllWindowFunction() );               
```

- timeWindowAll

  ```java
   .process(new ProcessAllWindowFunction<>() {
                      @Override
   public void process(Context context, Iterable<?> elements, Collector<> out) throws Exception {
       // elements 汇集了窗口内的所有数据
  }
  ```

  

##### allowedLateness

- 如何处理延迟数据

  ```java
  .keyBy(ApacheLogEvent::getUrl)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.minutes(1))
      .aggregate(new myUrlsAgg(), new myUrlsRes())
      .print("agg")
  
  // 以url分组并累加数量
  // 第一次窗口输出
  agg > PageViewCount{url='/presentations/', windowEnd=1431829550000, count=1}
  agg > PageViewCount{url='/present', windowEnd=1431829550000, count=1}
  
  // 第二次来了一条延迟数据  {ip='83.149.9.216', userId='-', timestamp=1431829549000, method='GET', url='/presentations/'}。url='/presentations/'会累计并输出，url='/present'不会输出了
  agg > PageViewCount{url='/presentations/', windowEnd=1431829550000, count=2}
  ```

  

- 滑动窗口

```java
/** NetworkFlowAnalysis/src/main/java/com/atguigu/networkflow_analysis/HotPages.java
 * 经测试，滑动窗口使用allowedLateness(Time.minutes(1))，要进入侧输出流需要保证来的数据不被其他窗口占用 timeWindow(Time.minutes(10), Time.seconds(5))， maxOutOfOrderness=1
 *  例子：
 *  10:25:49 （该窗口结束时间为10:25:50）
 *  10:26:51  (窗口(10:15:50-10:25:50)关闭）
 *  10:25:46 (不会进入侧输出流，因为还属于(10:15:55-10:25:55),(10:16:00-10:26:00)...(10:25:45-10:35:45)
 *  10:15:51 (进入侧输出流，属于(10:15:50-10:15:55)
 */
```



##### WaterMark

-  AscendingTimestampExtractor 的WM为当前事件时间-1毫秒
- 事件时间的WM默认200ms生成一次，所以读取文件的ctx.timerService().currentWatermark()可能都是-9223372036854775808

##### Planner

 - hadoop-code/bigDataSolve/FlinkTutorial/src/main/java/com/lh/apitest/tableapi/TableTest2_CommonApi.java

##### keyBy

- keyBy后接入的参数

```java
1. 直接跟聚合函数
dataStream.keyBy("id");
		  .reduce(new ReduceFunction<SensorReading>() {})
         // .map( new MyKeyCountMapper() ); //MyKeyCountMapper extends RichMapFunction

2. 跟window后再跟WindowFunction，可直接获取一个窗口内的数据                                       SingleOutputStreamOperator<Tuple3<String, Long, Integer>> resultStream2 = dataStream.keyBy("id").timeWindow(Time.seconds(15))
//.process(new ProcessWindowFunction<SensorReading, Object, Tuple, TimeWindow>() {
//                })
           .apply(new WindowFunction<SensorReading, Tuple3<String, Long, Integer>, Tuple, TimeWindow>() {
                    @Override
  public void apply(Tuple tuple, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                        String id = tuple.getField(0);
                        Long windowEnd = window.getEnd();
                        Integer count = IteratorUtils.toList(input.iterator()).size();
                        out.collect(new Tuple3<>(id, windowEnd, count));
                    }
                });

3. window后加聚合，聚合一个window记录内的方法
    dataStream.keyBy("id")
    	.timeWindow(Time.seconds(15)).sum("temperature");
              
4. 跟Window后再跟聚合函数，AggregateFunction计算逐条记录                                         SingleOutputStreamOperator<Double> avgTempResultStream = dataStream.keyBy("id")
                .countWindow(10, 2)
                .aggregate(new MyAvgTemp()); // MyAvgTemp implements AggregateFunction：累加器

4. 跟process，处理每一个元素
dataStream.keyBy("id")
    .process( new MyProcess() ) // MyProcess extends KeyedProcessFunction
    .print(); 
// KeyedProcessFunction 会处理流的每一个元素，输出为 0 个、 1 个或者多个元素。
5. 
    dataStream
     .keyBy("id")
     
```

- keyBy用法

  ```java
  1.
  DataStream<Long> dataStream1 = env.fromElements(1L, 34L, 4L, 657L, 23L);
  KeyedStream<Long, Integer> keyedStream2 = dataStream1.keyBy(new KeySelector<Long, Integer>() {
          @Override
          public Integer getKey(Long value) throws Exception {
          return value.intValue() % 2;
          }
  });
  // 2. 写字段，但是需要输入 DataStream<SensorReading> dataStream中为POJO
  KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");
  // 3. 坐标，这种没有POJO的
  DataStream<String> inputDataStream = env.socketTextStream("node01", 8888);
  DataStream<Tuple2<String, Integer>> resultStream = inputDataStream
      .flatMap(new WordCount.MyFlatMapper())
      .keyBy(0) 
       .sum(1)
  ```

- KeyedProcessFunction

  - KeyedProcessFunction 用来操作 KeyedStream。 KeyedProcessFunction 会处理流
    的每一个元素，输出为 **0 个、 1 个或者多个元素**。所有的 Process Function 都继承自
    RichFunction **[RichMapFunction ，RichFlatMapFunction  ，RichFilterFunction  都继承RichFunction]**接口，所以都有 open()、 close()和 getRuntimeContext()等方法。而
    KeyedProcessFunction<K, I, O>还额外提供了两个方法:
    • processElement(I value, Context ctx, Collector<O> out), 流中的每一个元素都
    会调用这个方法，调用结果将会放在 Collector 数据类型中输出。

    以访问元素的时间戳，元素的 key，以及 TimerService 时间服务。 Context 还
    可以将结果输出到别的流**(side outputs)**。
    • **onTimer**(long timestamp, OnTimerContext ctx, Collector<O> out) 是一个回调
    函数。当之前注册的定时器触发时调用。参数 timestamp 为定时器所设定的
    触发的时间戳。 Collector 为输出结果的集合。 OnTimerContext 和
    processElement 的 Context 参数一样，提供了上下文的一些信息，例如定时器
    触发的时间信息(事件时间或者处理时间)。

  - 综上，如果需要用到定时器，侧输出流，则可选择KeyedProcessFunction。如果只是需要用到状态编程则只需要用RichFlatMapFunction <ref：9.3.2 键控状态>

##### SPARK VS FLINK

- key

```scala

// 1. spark
val dataRDD1:RDD[(String, Int)]= sc.makeRDD(List(("a",1),("b",2),("c",3))).foldByKey(10)(_+_) 
// reduceByKey等返回RDD[(K, V)] ,只有def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] 的Value返回集合

// 2. flink
// 签名函数：public KeyedStream<T, Tuple> keyBy(String... fields)
KeyedProcessFunction中的processElement(I value, Context ctx, Collector<O> out)，
Context可以访问元素的时间戳，元素的 key。这是因为keyBy(field)，可以选择哪个字段，返回值KeyedStream中不确定前者用了哪个key作为分流
```



- 结构

  ```java
  jobmanager:主节点，类似于spark中的master
  
  taskManager：从节点，类似于spark中的worker
  
  slot：插槽，类似于spark中executor中的线程，只不过flink中的slot是物理存在的，可以手动配置，每个slot执行一个任务，是静态概念，用来隔绝内存。但slot的个数不能多于cpu-cores。并行度上限不能大于slot的数量。
  ```

  



