#### Flink杂记

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

##### processElement

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

​	WM = 当前时间-乱序时间；当前WM时间下的数据到齐了

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



- 写 WM的两种方式  1.12 版本

  - ```java
    //TODO 3.将每行数据转换为JSON对象并提取时间戳生成Watermark
    SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject).assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
        @Override
        public long extractTimestamp(JSONObject element, long recordTimestamp) {
            return element.getLong("ts");
        }
    }));
    ```

  - ```java
    WatermarkStrategy<JSONObject> watermarkStrategy = WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
        @Override
        public long extractTimestamp(JSONObject element, long recordTimestamp) {
        return element.getLong("ts");
        }
    });
    SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                	context.output(new OutputTag<String>("dirty") {}, s);
                }
            }
    }).assignTimestampsAndWatermarks(watermarkStrategy);
    ```

    



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

##### 状态编程

```java
private ValueState<String> dateState;

@Override
public void open(Configuration parameters) throws Exception {
    ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("date-state", String.class);
    //设置状态的超时时间以及更新时间的方式
    StateTtlConfig stateTtlConfig = new StateTtlConfig
        .Builder(Time.hours(24))
        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .build();
    valueStateDescriptor.enableTimeToLive(stateTtlConfig);
    dateState = getRuntimeContext().getState(valueStateDescriptor);
}
```

##### FlinkCDC

```java
DebeziumSourceFunction<String> build = MySQLSource.<String>builder()
    .hostname("node01")
    .port(3306)
    .username("root")
    .password("000000")
    .databaseList("gmall2021")
    .tableList("gmall2021.base_trademark, gmall2021.order_info")
    .startupOptions(StartupOptions.initial())
    .deserializer(new CustomerDeserialization())
    .build();

DataStreamSource<String> source = env.addSource(build);
source.print()
/*
1. 修改mysql配置，后重启
vim /etc/my.cnf
    server-id = 1
    log-bin=mysql-bin
    binlog_format=row
    binlog-do-db=gmall2021
2. 监测/var/lib/mysql 下的mysql-bin.000026文件，表有任何改动会记录在里面。当然数据删除也会记录，mysql-bin.000026这个文件会增大，初次运行以上代码，会将现有数据库现有记录输出，数据格式如下，before都是{},都是insert
{"database":"gmall2021","before":{},"after":{"birthday":"1965-12-04","gender":"F","create_time":"2020-12-04 20:21:47","login_name":"hu6er28gwu39","nick_name":"伊伊","name":"伏艺","user_level":"1","phone_num":"13396193213","id":716,"email":"hu6er28gwu39@qq.com","operate_time":"2020-12-04 23:12:40"},"type":"insert","table":"user_info"}
如果是update，则before和after都不为空
如果是delete，则before有值，after为空
*/
```

##### JSONObject



```java
public class Extend {
// 从左往右自动(去除下划线)匹配进行模糊匹配
    public static void main(String[] args) {
        testJsonObject();
    }

    public static void testJsonObject(){
        Be be = JSON.parseObject("{'userId':'Atguigu', 'age':11, 'user_id':'tom','GENder':'female' }", Be.class);
        System.out.println(be); // Be(user_id=Atguigu, age=11, gender=female)
    }
}


@Data
class Be{
    String user_id;
    int age;
    String gender;
}

```

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

##### 阿里链接

http://e.betheme.net/article/show-1029277.html?action=onClick

https://www.ngui.cc/el/1759712.html?action=onClick

##### 终端执行

```sql
--  ./sql-client.sh embedded (standlone需要先启动集群./start-cluster.sh)
--  yarn进入是./sql-client.sh embedded -s yarn-session这个命令
-- ./yarn-session.sh -n 2 -s 2 -jm 1024 -tm 1024 -nm test -d
Flink SQL> CREATE TABLE tbl (
                  item STRING,
                  price DOUBLE,
                  proctime as PROCTIME()
            ) WITH (
                'connector' = 'socket',
                'hostname' = 'node01',
                'port' = '9999',
                'format' = 'csv'
           );
 select * from tbl;
 
Flink SQL> CREATE VIEW MyView3 AS
            SELECT
                TUMBLE_START(proctime, INTERVAL '10' MINUTE) AS window_start,
                TUMBLE_END(proctime, INTERVAL '10' MINUTE) AS window_end,
                TUMBLE_PROCTIME(proctime, INTERVAL '10' MINUTE) as window_proctime,
                item,
                MAX(price) as max_price
            FROM tbl
                GROUP BY TUMBLE(proctime, INTERVAL '10' MINUTE), item;

Flink SQL> DESC MyView3;
+-----------------+-----------------------------+-------+-----+--------+-----------+
|           name  |                        type |  null | key | extras | watermark |
+-----------------+-----------------------------+-------+-----+--------+-----------+
|    window_start |                TIMESTAMP(3) | false |     |        |           |
|      window_end |                TIMESTAMP(3) | false |     |        |           |
| window_proctime | TIMESTAMP_LTZ(3) *PROCTIME* | false |     |        |           |
|            item |                      STRING | true  |     |        |           |
|       max_price |                      DOUBLE |  true |     |        |           |
+-----------------+-----------------------------+-------+-----+--------+-----------+
-----------------------------------

https://blog.51cto.com/u_15318160/4860992

```



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

###### 建立环境 

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);
```

###### 建表

对表进行业务处理（如果使用TableAPI需先转为Table对象）

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

```



###### 查询

```java

// create a Table object from a Table API query
Table table2 = tableEnv.from("table1").select(...);
// create a Table object from a SQL query
Table table3 = tableEnv.sqlQuery("SELECT ... FROM table1 ... ");
// 还可以
Table table5 = tableEnv.sqlQuery("SELECT ... FROM " table2);

// emit a Table API result Table to a TableSink, same for SQL result
TableResult tableResult = table2.executeInsert("outputTable");
tableResult...
```



（**注意：tableEnv.from("table1")会返回table对象，要Table对象才能进行select等API查询， sql可直接**sqlQuery等操作）

此外，建临时表还可以通过以下方式

```java
// 1. 将流转化为表, 定义时间特性
DataStream<SensorReading> dataStream = env.readTextFile().map()
Table sensorTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp, pt.proctime");
    /*
     rt.rowtime：使用事件时间，需要先分配WM-assignTimestampsAndWatermarks
     id，timestamp， temperature为SensorReading的名字 
    */
	
// 2. 将流转化为视图
tableEnv.createTemporaryView("sensor", dataStream);

// 3 通过API建表
tableEnv.connect( new FileSystem().path(filePath))
                .withFormat( new Csv().fieldDelimiter(','))
                .withSchema( new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable")

// 4 表转为SQL操作的表
tableEnv.createTemporaryView("exampleView", table2);

// 5. 变量变为表  1.12
Table table = tEnv.fromValues(
   row(1, "ABC"),  
   row(2L, "ABCDE")
);
/*
    root
     |-- f0: BIGINT NOT NULL
     |-- f1: VARCHAR(5) NOT NULL
 */

Table table = tableEnv.fromValues(
    DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
        DataTypes.FIELD("name", DataTypes.STRING())
    ),
    row(1, "ABC"),
    row(2L, "ABCDE")
);
/*
root
|-- id: DECIMAL(10, 2)
|-- name: STRING
*/
// https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/table/tableApi.html
```

###### 输出

```java
// 1. 将表转化为流 来 输出
DataStream<ProvinceStats> provinceStatsDataStream = tableEnv.toAppendStream(table, ProvinceStats.class);
provinceStatsDataStream.addSink(kafka);

// 2. Table对象输出到定义的表
 table2.executeInsert("outputTable");

// 3. sql插入
tableEnv.executeSql("insert into outputTable select * from table1");
```

##### 时间属性

- 输入

  - 建表时 WM所需的TIMESTAMP
  - 在Flink里，Timestamp被定义为**8字节的long值**。每个算子拿到数据时，默认以毫秒精度的Unix时间戳解析这个long值，也就是自1970-01-01 00:00:00.000以来的毫秒数。当然自定义的算子可以自己定义时间戳的解析方式。

  ```java
  // 1 从kafka读取
  // 1.1 如果数据源的时间是时间戳 
  tableEnv.sqlQuery("select id, 
                    CURRENT_TIMESTAMP, 
                    TO_TIMESTAMP(FROM_UNIXTIME(1547718199000 /1000,'yyyy-MM-dd HH:mm:ss')),
                    FROM_UNIXTIME(1547718199000 /1000,'yyyy-MM-dd HH:mm:ss')  from tbl");
  // sensor_1,2022-09-26T03:43:33.936,
  // 2019-01-17T17:43:19,
  // 2019-01-17 17:43:19
  // FROM_UNIXTIME(1547718199000 /1000) 和 FROM_UNIXTIME(1547718199000 /1000,'yyyy-MM-dd HH:mm:ss') 作用一样
  
  
  // 1.2 如果数据源的时间是字符串 create_time=“2022-11-10 18:58:29”
  TO_TIMESTAMP(create_time)
      
  ```

- 输出

- ```java
  // 1
   UNIX_TIMESTAMP()*1000 ts // 返回：1668403007000L
  //2 转为java的Bean
      TUMBLE_START(rt, INTERVAL '10' SECOND) 是TIMESTAMP类型（2022-11-10T18:58:10），所以转为java的bean需要使用
      DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') stt
      再在java的bean中使用private String stt;
  ```

  





- EventTime
  
  - 在DDL中定义

```sql
CREATE TABLE user_actions (
  user_name STRING,
  data STRING,
  user_action_time TIMESTAMP(3),
  -- declare user_action_time as event time attribute and use 5 seconds delayed watermark strategy
  WATERMARK FOR user_action_time AS user_action_time - INTERVAL '5' SECOND
) WITH (
  ...
);

SELECT TUMBLE_START(user_action_time, INTERVAL '10' MINUTE), COUNT(DISTINCT user_name)
FROM user_actions
GROUP BY TUMBLE(user_action_time, INTERVAL '10' MINUTE);
```

- 	-	相关API

```java
// import org.apache.flink.table.expressions.Expression;
DataStream<String> inputStream = env.readTextFile("sensor.txt");
// 3. 转换成POJO
DataStream<SensorReading> dataStream= inputStream.assignTimestampsAndWatermarks(...); // 乱序 2s

// 4. 将流转换成表，定义时间特性
// Table dataTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp, pt.proctime");
Table dataTable = tableEnv.fromDataStream(dataStream, $("id"), $("timestamp").as("ts"), $("temperature").as("temp"), $("rt").rowtime());
// Table dataTable = tableEnv.fromDataStream(dataStream,  $("id"), $("temperature"), $("rt").rowtime()); 
/*
    root
     |-- id: STRING
     |-- ts: BIGINT
     |-- temp: DOUBLE
     |-- rt: TIMESTAMP(3) *ROWTIME*
*/

tableEnv.createTemporaryView("sensor", dataTable);

// 5. 窗口操作
// 5.1 Group Window
// table API
Table resultTable = dataTable.window(Tumble.over("10.seconds").on("rt").as("tw"))
    .groupBy("id, tw")
    .select("id, id.count, temp.avg, tw.end");
tableEnv.toAppendStream(resultTable, Row.class).print("resultTable");
/*
    resultTable> sensor_1,1,35.8,2019-01-17 09:43:20.0
    resultTable> sensor_6,1,15.4,2019-01-17 09:43:30.0
    resultTable> sensor_1,2,34.55,2019-01-17 09:43:30.0
    resultTable> sensor_10,1,38.1,2019-01-17 09:43:30.0
    resultTable> sensor_7,1,6.7,2019-01-17 09:43:30.0
    resultTable> sensor_1,1,37.1,2019-01-17 09:43:40.0

*/
// SQL
Table resultSqlTable = tableEnv.sqlQuery("select id, count(id) as cnt, avg(temp) as 					avgTemp, tumble_end(rt, interval '10' second) " +
                "from sensor group by id, tumble(rt, interval '10' second)");

// 5.2 Over Window
// table API
Table overResult = dataTable.window(Over.partitionBy("id").orderBy("rt").preceding("2.rows").as("ow"))
    .select("id, rt, id.count over ow, temp.avg over ow");

// SQL  开窗，事件时间排序，取前3个值做均值
Table overSqlResult = tableEnv.sqlQuery("select id, rt, count(id) over ow, avg(temp) over ow from sensor  window ow as (partition by id order by rt rows between 2 preceding and current row)");

tableEnv.toRetractStream(overSqlResult, Row.class).print("overSqlResult");
/**
         * overSqlResult> (true,sensor_1,2019-01-17T09:43:19,1,35.8)
         * overSqlResult> (true,sensor_6,2019-01-17T09:43:21,1,15.4)
         * overSqlResult> (true,sensor_7,2019-01-17T09:43:22,1,6.7)
         * overSqlResult> (true,sensor_10,2019-01-17T09:43:25,1,38.1)
         * overSqlResult> (true,sensor_1,2019-01-17T09:43:27,2,36.05)
         * overSqlResult> (true,sensor_1,2019-01-17T09:43:29,3,34.96666666666666)
         * overSqlResult> (true,sensor_1,2019-01-17T09:43:32,3,35.4)
*/

```

- Proctime

  - DDL中定义

  ```sql
  CREATE TABLE user_actions (
    user_name STRING,
    data STRING,
    user_action_time TIMESTAMP(3),
    -- declare user_action_time as event time attribute and use 5 seconds delayed watermark strategy
    WATERMARK FOR user_action_time AS user_action_time - INTERVAL '5' SECOND
  ) WITH (
    ...
  );
  
  SELECT TUMBLE_START(user_action_time, INTERVAL '10' MINUTE), COUNT(DISTINCT user_name)
  FROM user_actions
  GROUP BY TUMBLE(user_action_time, INTERVAL '10' MINUTE);
  // GROUP BY HOP(rowtime, INTERVAL '1' HOUR, INTERVAL '1' DAY)
// HOP(time_attr, sliding_interval, window_size_interval)
  ```




##### 映射

###### toAppendStream映射到Bean对象

```java
        Table table = tableEnv.sqlQuery("select " +
                        "province_id," +
                    "province_name" +
                    "from orderWide ");
tableEnv.toAppendStream(table, ProvinceStats.class);
// 这个table中的字段顺序可以和ProvinceStats不一致，但是数量需要一致
```

######  从kafka读取

```java
// 如果是json数据，可以从数据源选取读取几个字段（建表字段需要是数据源的字段），如果format是csv则必须全量字段读取（建表字段自己命名，本身数据源就没有字段名，只是以“，”分隔的数据）
String ddl = "CREATE TABLE orderWide (" +
    "`province_name` STRING," +
    "`province_area_code` STRING," +
    "`create_time` String," +
    " `rt` as TO_TIMESTAMP(create_time), " +
    " WATERMARK FOR rt AS rt - INTERVAL '1' SECOND " +
    ") with (" +
    "'connector' = 'kafka', " +
    "'topic' = 'dwm_order_wide'," +
    "'properties.bootstrap.servers' = 'node01:9092'," +
    //                "'properties.group.id' = 'testGroup'," +
    "'scan.startup.mode' = 'earliest-offset'," +
    "'format' = 'json')";
tableEnv.executeSql(ddl);
```

###### JSONObject映射bean

```java
public static void testJsonObject(){
    Be be = JSON.parseObject("{'userId':'Atguigu', 'age':11, 'user_id':'tom','GENder':'female' }", Be.class);
    System.out.println(be); // Be(user_id=Atguigu, age=11, gender=female)
}
```



#### 数仓流程

##### ODS

######  1. 收集日志数据

-  数据格式

  ```java
  # 页面数据
      ## page数据（有些包含display数据）
  {"common":{"ar":"440000","ba":"iPhone","ch":"Appstore","is_new":"0","md":"iPhone X","mid":"mid_6","os":"iOS 13.2.3","uid":"14","vc":"v2.1.134"},"displays":[{"display_type":"activity","item":"1","item_type":"activity_id","order":1,"pos_id":5},{"display_type":"query","item":"3","item_type":"sku_id","order":2,"pos_id":1},{"display_type":"recommend","item":"1","item_type":"sku_id","order":3,"pos_id":3},{"display_type":"recommend","item":"7","item_type":"sku_id","order":4,"pos_id":3},{"display_type":"query","item":"6","item_type":"sku_id","order":5,"pos_id":5},{"display_type":"query","item":"5","item_type":"sku_id","order":6,"pos_id":3},{"display_type":"recommend","item":"1","item_type":"sku_id","order":7,"pos_id":1},{"display_type":"promotion","item":"2","item_type":"sku_id","order":8,"pos_id":3},{"display_type":"promotion","item":"9","item_type":"sku_id","order":9,"pos_id":3},{"display_type":"query","item":"7","item_type":"sku_id","order":10,"pos_id":2}],"page":{"during_time":3730,"page_id":"home"},"ts":1668073354000}
  #  start启动数据
  {"common":{"ar":"420000","ba":"Huawei","ch":"oppo","is_new":"1","md":"Huawei Mate 30","mid":"mid_5","os":"Android 11.0","uid":"19","vc":"v2.1.132"},"start":{"entry":"notice","loading_time":8358,"open_ad_id":16,"open_ad_ms":5756,"open_ad_skip_ms":0},"ts":1668073351000}
  
  ```

- 配置前端

  - gmall2020-mock-log-2020-12-18.jar的application.yml

    ```txt
    mock.url: "http://node01:80/applog" # 发送至nginx，80可以省去
    mock.url: "http://node01:8081/applog" # 如果不用ngxin，直接用8081发送到springboot
    ```

- 配置nginx

  - nginx.cong

    ```txt
    upstream logcluster{
                server node01:8081 weight=1;
                server node02:8081 weight=1;
                server node03:8081 weight=1;
        }
    listen       80;
    ```

- 配置springboot的gmall-logger.jar，输出到**ods_base_log**

  - application.properties

    ```
    server.port=8081
    ```

###### 2. 手机业务数据

  -  通过flinkCDC将数据库的数据同步至ods_base_db数据库，同时可以多次运行gmall2020-mock-db-2020-11-27.jar 生产业务数据并同步至**ods_base_db**

      -   ```
        ## user_info这个表是增量，其余的表是清空后加入
        --------开始生成数据--------
        --------开始生成用户数据--------
        共有80名用户发生变更
        共生成0名用户
        --------开始生成收藏数据--------
        共生成收藏100条
        --------开始生成购物车数据--------
        共生成购物车1705条
        --------开始生成订单数据--------
        共生成订单115条
        --------开始生成支付数据--------
        状态更新115个订单
        共有78订单完成支付
        --------开始生成退单数据--------
        状态更新78个订单
        共生成退款27条
        共生成退款支付明细27条
        --------开始生成评价数据--------
        共生成评价114条
        ```

        

##### DWD

###### 日志数据分流

- BaseLogApp，读取 ods_base_log数据
  - 有“start”的同步至dwd_start_log
  - 有page的同步至dwd_page_log
  - 有display的同步至dwd_display_log

###### 业务数据分流

- 通过BaseDBApp同步至DWD中，并进行hbase和kafka的分流
    - spu_info,sku_info等维度数据写入hbase
    - order_info,order_detail,pay_info等事实数据写入kafka
    - 注意 BaseDBApp从ods_base_db取数据要过滤掉type=delete的数据

##### DWM

###### 独立访客 - UniqueVisitApp

  - 读取dwd_page_log

  - 根据mid手机号进行keyby，过滤（filter）出last_page_id == null（说明第一次登录），且当天是第一次登录的数据（可用状态）

      - 核心代码

          - ```java
            open{
                StateTtlConfig build = StateTtlConfig.newBuilder(Time.hours(24)).build();// 24小时过期
                valueStateDescriptor.enableTimeToLive(build);
                dataState = getRuntimeContext().getState(valueStateDescriptor);   
            }
            public boolean filter(JSONObject value) throws Exception {
                String last_page_id = value.getJSONObject("page").getString("last_page_id");
                String lastDate = dataState.value();
                String curDate = sdf.format(value.getLong("ts"));
                if (last_page_id == null || last_page_id.length() <= 0) {
                    if (!curDate.equals(lastDate)) {
                        dataState.update(curDate);
                        return true;
                    }
                }
                return false;
            }
            ```

- 输出数据就是原始的dwd_page_log数据

###### 用户跳出

 - 读取dwd_page_log

 - 根据midkey，通过cep匹配出用户首次登录app后last_page_id=null，且中途没有登录别的页面，那么下一条数据也为null的模式。等待时间10s

    - 核心代码

       - ```java
         Pattern<JSONObject, JSONObject> userJumpPattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
         @Override
         public boolean filter(JSONObject value) throws Exception {
         String last_page_id = value.getJSONObject("page").getString("last_page_id");
         return last_page_id == null || last_page_id.length() <= 0;
         }
         }).times(2).consecutive().within(Time.seconds(10)); // 10s后为超时
         PatternStream<JSONObject> patternDS = CEP.pattern(dsAddWm.keyBy(json -> json.getJSONObject("common").getString("mid")), userJumpPattern);
         patternDS.select()...
         
         ```

- 最后合并超时数据和正常数据输出到dwm_user_jump_detail，输出数据就是原始的dwd_page_log数据

###### 订单宽表

- 读取dwd_order_info，dwd_order_detail，intervalJoin两个流形成wideDS合并流

- 关联用户，地区，SPU等维度

  - 通过AsyncDataStream.unorderedWait(wideDS, AsyncFunction, tieout=60, seconds)进行异步匹配数据

    - AsyncFunction中主要作用asyncInvoke方法，异步调用方法

      - ```java
        public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        
            threadPoolExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    //获取查询的主键
                    String id = getKey(input);
                    //查询维度信息(可先查redis再查hbase当作二级查找)
                    JSONObject dimInfo = DimUtil.getDimInfo(conn, tableName, id);
                   //补充维度信息
                    join(input, dimInfo);
                    //将数据输出
                    resultFuture.complete(Collections.singletonList(input));
        
        });
        ```
      
      - redis的key是 "DIM:DIM_SKU_INFO:29" 可解释为 ”DIM:table_process的sink_table表名:hbase中维度表的字段“， 1301是存储在hbase的id，维度表都有一个唯一的id对应事实表。比如"sku_info"表的字段有【id,spu_id,price,sku_name,sku_desc】，order_detail的字段的sku_id字段就是对应sku_info表的id字段

- 输出到kafka的dwm_order_wide

  - ```
    {"activity_reduce_amount":0.00,"category3_id":473,"category3_name":"香水","coupon_reduce_amount":0.00,"create_date":"2022-11-10","create_hour":"18","create_time":"2022-11-10 18:58:29","detail_id":80654,"feight_fee":12.00,"order_id":26973,"order_price":300.00,"order_status":"1001","original_total_amount":1299.00,"province_3166_2_code":"CN-HE","province_area_code":"130000","province_id":5,"province_iso_code":"CN-13","province_name":"河北","sku_id":32,"sku_name":"香奈儿（Chanel）女士香水5号香水 粉邂逅柔情淡香水EDT 5号淡香水35ml","sku_num":1,"split_total_amount":300.00,"spu_id":11,"spu_name":"香奈儿（Chanel）女士香水5号香水 粉邂逅柔情淡香水EDT ","tm_id":11,"tm_name":"香奈儿","total_amount":1311.00,"user_age":34,"user_gender":"F","user_id":3871}
    ```

  - 

##### DWS

###### 访客主题宽表 - dwm_order_wide

目的根据维度“渠道、地区、版本、新老用户" 进行聚合，算出度量PV、 UV、跳出次数、进入页面数(session_count)、连续访问时长  

过程

- 读取dwm_unique_visit，dwm_user_jump_detail，dwd_page_log，并转换为Bean为VisitorStats的数据
- 合并3个流 

- 以渠道、地区、版本、新老用户进行keyBy，10s一个窗口，对窗口内的各项指标累加，输出数据如下，最后输出到clickhouse

```python
VisitorStats(stt=2022-11-10 17:49:40, edt=2022-11-10 17:49:50, vc=v2.1.111, ch=xiaomi, ar=110000, is_new=0, uv_ct=0, pv_ct=0, sv_ct=0, uj_ct=1, dur_sum=0, ts=1668073783000)
```

###### 商品主题宽表

与访客的 dws 层的宽表类似，也是把多个事实表的明细数据汇总起来组合成宽表。输出数据如下

过程

- 读取dwd_page_log，dwm_order_wide，dwm_payment_wide，dwd_cart_info，dwd_favor_info，dwd_order_refund_info，dwd_comment_info，并转化为ProductStats数据
- 合并7个流
- 以Sku_id做keyby，10s一个窗口，对窗口内的各项指标累加，输出数据如下

```python
ProductStats(stt=2022-11-10 18:58:10, edt=2022-11-10 18:58:20, sku_id=23, sku_name=十月稻田 辽河长粒香 东北大米 5kg, sku_price=40, spu_id=7, spu_name=十月稻田 长粒香大米 东北大米 东北香米 5kg, tm_id=6, tm_name=长粒香, category3_id=803, category3_name=米面杂粮, display_ct=0, click_ct=0, favor_ct=0, cart_ct=0, order_sku_num=0, order_amount=0, order_ct=0, payment_amount=0, paid_order_ct=0, refund_order_ct=0, refund_amount=0, comment_ct=3, good_comment_ct=1, orderIdSet=[], paidOrderIdSet=[], refundOrderIdSet=[], ts=1668077894000)
```

- 关联维度表，从redis先取再从hbase取，加上“SPU_NAME”，“TM_NAME”等字段信息，并输出到clickhouse

  

###### 地区主题宽表

过程

- 读取dwm_order_wide订单宽表
- 以地区和时间窗口分组，count(distinct order_id) order_count,sum(split_total_amount) order_amount统计出订单数量和金额



###### 关键词主题

过程

- 读取dwd_page_log 页面数据
- 过滤出last_page_id!=nuil && item!=null (item=小米，可乐等)的记录
- 对记录里的item使用IKSegmenter切分出各个关键词
- 对关键词和窗口分组，统计出窗口下关键词的数量

