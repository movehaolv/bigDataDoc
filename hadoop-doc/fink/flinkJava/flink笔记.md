##### windowsAll && window(直接看聚合章节)

```tex
需求：将无限流数据按5秒一个窗口，处理数据批量写入phoenix

实现方式：有6个topic数据是WindowAll的方式，有1个topic数据是KeyBy Window

1）.timeWindowAll(Time.seconds(5)).apply(new AllWindowFunction ...)

2）.keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(5))).apply(new WindowFunction ...)

对比：

    WindowAll并行度只能1，不可设置，适用于数据量不大的情况。

    KeyBy Window的并行度，可按Key设置，适用于大数据量。
```

#### processElement

```java
// com\atguigu\market_analysis\AdStatisticsByProvince.java
if( curCount >= countUpperBound ){
// 判断是否输出到黑名单过，如果没有的话就输出到侧输出流
		ctx.output( new OutputTag<BlackListUserWarning>("blacklist"){},
}
out.collect(value);
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

- keyBy().timeWindow (timeWindow弃用，使用.window(TumblingEventTimeWindows.of(Time.seconds(10))))

```java
增量聚合 aggregate(new AggregateFunction） 
               
// ProcessWindowFunction是WindowFunction的加强版，可以获得上下文
全量聚合 process(new ProcessWindowFunction) apply(new WindowFunction) // 一次性处理一个窗口的数据
.aggregate(new AggregateFunction(), new WindowFunction()); // 前增量后全量 WIndowFunction 的Iterable<Long> input 应该只有一个值，com.atguigu.networkflow_analysis.PageView  // reduce也有此效果
.aggregate(new AggregateFunction(), new ProcessWindowFunction()); // ProcessWindowFunction可获取上下文 

单次处理元素
.process(new KeyedProcessFunction()) // 处理每个元素，底层API  
<==>
.timeWindowAll(Time.hours(1))               
.trigger( new MyTrigger() )
.process( new ProcessAllWindowFunction() );  // com\atguigu\networkflow_analysis\UvWithBloomFilter.java     
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
  agg > PageViewCount{url='/0/', windowEnd=1431829550000, count=2}
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

##### CEP

```java
// src\main\java\com\atguigu\orderpay_detect\OrderPayTimeout.java
// 1. 定义一个带时间限制的模式  A followedBy B不仅能匹配A B,还能匹配A C B，next：严格匹配
Pattern<OrderEvent, OrderEvent> orderPayPattern = Pattern
    .<OrderEvent>begin("create").where(new SimpleCondition<OrderEvent>() {
    @Override
    public boolean filter(OrderEvent value) throws Exception {
        return "create".equals(value.getEventType());
    }
})
    .followedBy("pay").where(new SimpleCondition<OrderEvent>() {
    @Override
    public boolean filter(OrderEvent value) throws Exception {
        return "pay".equals(value.getEventType());
    }
})
    .within(Time.minutes(15));


// 2. 定义侧输出流标签，用来表示超时事件
OutputTag<OrderResult> orderTimeoutTag = new OutputTag<OrderResult>("order-timeout"){};

// 3. 将pattern应用到输入数据流上，得到pattern stream
PatternStream<OrderEvent> patternStream = CEP.pattern(orderEventStream.keyBy(OrderEvent::getOrderId), orderPayPattern);

// 4. 调用select方法，实现对匹配复杂事件和超时复杂事件的提取和处理
SingleOutputStreamOperator<OrderResult> resultStream = patternStream
    .select(orderTimeoutTag, new OrderTimeoutSelect(), new OrderPaySelect());

resultStream.print("payed normally");
resultStream.getSideOutput(orderTimeoutTag).print("timeout");

    // 实现自定义的超时事件处理函数
    public static class OrderTimeoutSelect implements PatternTimeoutFunction<OrderEvent, OrderResult>{
        @Override
        public OrderResult timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
            Long timeoutOrderId = pattern.get("create").iterator().next().getOrderId();
            return new OrderResult(timeoutOrderId, "timeout " + timeoutTimestamp);
        }
    }

    // 实现自定义的正常匹配事件处理函数
    public static class OrderPaySelect implements PatternSelectFunction<OrderEvent, OrderResult>{
        @Override
        public OrderResult select(Map<String, List<OrderEvent>> pattern) throws Exception {
            Long payedOrderId = pattern.get("pay").iterator().next().getOrderId();
            return new OrderResult(payedOrderId, "payed");
        }
    }

```



##### WaterMark

- AssignerWithPunctuatedWatermarks（为每条消息都会尝试生成水印）

  ```java
  public static class MyPunctuatedAssigner implements AssignerWithPunctuatedWatermarks<SensorReading>{
  
      private Long bound = 60 * 1000L;    // 延迟一分钟
      
      @Nullable
      @Override
      public Watermark checkAndGetNextWatermark(SensorReading lastElement, long extractedTimestamp) {
          if(lastElement.getId().equals("sensor_1"))
              return new Watermark(extractedTimestamp - bound);
          else
              return null;
      }
      
      @Override
      public long extractTimestamp(SensorReading element, long previousElementTimestamp) {
          return element.getTimestamp();
      }
  }
  
  ```

- AssignerWithPeriodicWatermarks （周期性的生成水印，不会针对每条消息都生成）

  - BoundedOutOfOrdernessTimestampExtractor
  - AscendingTimestampExtractor（AscendingTimestampExtractor 的WM为当前事件时间-1毫秒，因为数据时间是严格单调递增的，不会存在乱序，`Watermark = urrentTimestamp - 1`，这里-1是因为Watermark是左闭右开的）- 
  - 事件时间的WM默认200ms生成一次，所以读取文件的ctx.timerService().currentWatermark()可能都是-9223372036854775808

- 还有第三种策略是 无为策略：不设定watermark策略。

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
  });  // KeySelector<IN, KEY>
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

#### 框架比较

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

  

##### MR | SPARK | FLINK

![image-20220529173418419](D:\workLv\learn\proj\hadoop-doc\collection\pics\FlinkPics\mr-yarn.png)



![image-20220529173230418](D:\workLv\learn\proj\hadoop-doc\collection\pics\FlinkPics\spark-yarn.png)

Yarn 框架收到指令后会在指定的 NM 中启动ApplicationMaster；ApplicationMaster 启动 Driver 线程，执行用户的作业；

![image-20220529163957321](D:\workLv\learn\proj\hadoop-doc\collection\pics\FlinkPics\flink-yarn.png)

#### SQL & TableAPI

##### 更新模式

hadoop-code/bigDataSolve/FlinkTutorial/src/main/java/com/lh/apitest/tableapi/TableTest2_CommonApi.java

![image-20220915224114115](../../\collection\pics\FlinkPics\lh\更新模式.png)

#### 

-  toAppend 这里虽然聚合了，但是开窗后并不需要撤回修改，可用toAppend

![image-20220913152647022](../../\collection\pics\FlinkPics\lh\toAppend.png)

- retract

  ```java
  // input
  /*  
      sensor_1,1547718199,35.8,2019-01-17 09:43:19
      sensor_6,1547718201,15.4,2019-01-17 09:43:21
      sensor_7,1547718202,6.7,2019-01-17 09:43:22
      sensor_10,1547718205,38.1,2019-01-17 09:43:25
      sensor_1,1547718207,36.3,2019-01-17 09:43:27
      sensor_1,1547718209,32.8,2019-01-17 09:43:29
      sensor_1,1547718212,37.1,2019-01-17 09:43:32
  */
  // 计算逻辑
  Table aggTable = inputTable1.groupBy("id")
      .select("id, id.count as count, temp.avg as avgTemp");
  // 输出
  /*
      agg> (true,sensor_1,1,35.8)
      agg> (true,sensor_6,1,15.4)
      agg> (true,sensor_7,1,6.7)
      agg> (true,sensor_10,1,38.1)
      agg> (false,sensor_1,1,35.8)   // 1 如果是更新操作，那么会输出两条数据（上一条删除）
      agg> (true,sensor_1,2,36.05)   // 2 下一条插入
      agg> (false,sensor_1,2,36.05)
      agg> (true,sensor_1,3,34.96666666666666)
      agg> (false,sensor_1,3,34.96666666666666)
      agg> (true,sensor_1,4,35.5)
  */
  ```

- upsert

  - 需要使用支持key的数据库，相当于hashmap，因为插入更新都编码为add消息，那么key存在则更新，不存在则插入，这样不像retract需要两条数据作为更新，只要一条数据即可。视频中为涉及相关联系

##### API调用

建立环境 

建表/临时表 | 流转表

对表进行业务处理（如果使用TableAPI需先转为Table对象）

输出（）

```java
/* 
create table: 会注册到系统Catalog(默认是VvpCatalog)，持久化。适合多个query共享元素及
create temporary table:使用内存的Catalog，不持久化。适合不需共享元素据的场景，只给当前query查询
create temporary view: 简化sql语句（create temporary view v1 as select * from order;），和数据库的view不一样，不会持久化
statement set:( begin statement set; insert into t1...; insert into t2; end; ),适合需要输出到多个下游（sink）的场景
*/

// create a TableEnvironment for specific planner batch or streaming
TableEnvironment tableEnv = ...; // see "Create a TableEnvironment" section

// create an input Table
tableEnv.executeSql("CREATE TEMPORARY TABLE table1 ... WITH ( 'connector' = ... )");
// register an output Table
tableEnv.executeSql("CREATE TEMPORARY TABLE outputTable ... WITH ( 'connector' = ... )");

// create a Table object from a Table API query
Table table2 = tableEnv.from("table1").select(...);
// create a Table object from a SQL query
Table table3 = tableEnv.sqlQuery("SELECT ... FROM table1 ... ");

// emit a Table API result Table to a TableSink, same for SQL result
TableResult tableResult = table2.executeInsert("outputTable");
tableResult...
```

（**注意：tableEnv.from("table1")会返回table对象，要Table对象才能进行select等API查询， sql可直接**sqlQuery等操作）

此外，建临时表还可以通过以下方式

```java
// 1. 将流转化为表
dataStream = env.readTextFile().map()
Table sensorTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp");
tableEnv.createTemporaryView("sensor", sensorTable);
// 2 通过API建表
tableEnv.connect( new FileSystem().path(filePath))
                .withFormat( new Csv().fieldDelimiter(','))
                .withSchema( new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable")

```

输出

```java
// 1. 将表转化为流 来 输出
DataStream<ProvinceStats> provinceStatsDataStream = tableEnv.toAppendStream(table, ProvinceStats.class);
provinceStatsDataStream.addSink(kafka);

// 2. Table对象输出到定义的表
 table2.executeInsert("outputTable");

// 3. sql插入
tableEnv.executeSql("insert into outputTable select * from table1");
```

