##### 看哪些

- hadoop可直接先看collection的hadoop

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

##### Process Function

- ProcessFunction

- KeyedProcessFunction （ProcessFunction&KeyedProcessFunction 有OnTimer；ProcessWindowFunction中没有）

  - ```java
    ds.keyby.process(new KeyedProcessFunction<K,I,O>(){
        
       @Override
       public void processElement(I, Context, Collector<O>)
       @Override
       public void onTimer(long timestamp, OnTimerContext ctx, Collector<O> out)
    })
    ```

    

- CoProcessFunction

  - 对于两条输入流， DataStream API 提供了 CoProcessFunction 这样的 low-level
    操作。 CoProcessFunction 提供了操作每一个输入流的方法: processElement1()和
    processElement2()。  

  - ```java
    // 关联订单的支付和到账事件
    orderEventStream.keyBy(OrderEvent::getTxId)
    .connect(receiptEventStream.keyBy(ReceiptEvent::getTxId))
    .process(new TxPayMatchDetect());
    class TxPayMatchDetect extends CoProcessFunction{
        // 定义状态，保存当前已经到来的订单支付事件和到账时间
        ValueState<OrderEvent> payState; //  支付事件
        ValueState<ReceiptEvent> receiptState; // 到账事件状态
        processElement1(OrderEvent pay, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out){
                // 1. 订单支付事件来了，判断是否已经有对应的到账事件
                ReceiptEvent receipt = receiptState.value();  // 获取到账事件状态
                // 2. 如果receipt不为空，说明到账事件已经来过，输出匹配事件，清空两个状态
                // 3.1 如果receipt没来，注册一个定时器，开始等待
                  ctx.timerService().registerEventTimeTimer( (pay.getTimestamp() + 5) * 1000L ); 
                // 3.2 并更新状态
                payState.update(pay);
        }
        processElement2(ReceiptEvent receipt, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out){ // 1.到账事件来了，判断是否已经有对应的支付事件
            OrderEvent pay = payState.value();
          // 2. 如果pay不为空，说明支付事件已经来过，输出匹配事件，清空两个状态
          // 3.1如果pay没来，注册一个定时器，开始等待
            ctx.timerService().registerEventTimeTimer( (receipt.getTimestamp() + 3) * 1000L );
          // 3.2 并更新状态
          receiptState.update(receipt);
        }
        onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out){
            // 定时器触发，有可能是有一个事件没来，不匹配，也有可能是都来过了，已经输出并清空状态
            // 判断哪个不为空，那么另一个就没来
            if( payState.value() != null ) ctx.output(unmatchedPays, payState.value());
            if( receiptState.value() != null )ctx.output(unmatchedReceipts, receiptState.value());
            // 清空状态
            payState.clear();
            receiptState.clear();  
        }
    }
    ```

    

- ProcessJoinFunction

  - ```java
    // 作用跟上面的CoProcessFunction一致
    orderEventStream
                    .keyBy(OrderEvent::getTxId)
                    .intervalJoin(receiptEventStream.keyBy(ReceiptEvent::getTxId))
                    .between(Time.seconds(-3), Time.seconds(5))    // -3，5 区间范围
                    .process(new TxPayMatchDetectByJoin());
    class TxPayMatchDetectByJoin extends ProcessJoinFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>>{
            @Override
            public void processElement(OrderEvent left, ReceiptEvent right, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
                out.collect(new Tuple2<>(left, right));
            }
        }                
    ```

- BroadcastProcessFunction

  - ```java
    MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
    BroadcastStream<String> broadcastStream = tableProcessStrDS.broadcast(mapStateDescriptor);
    
    //TODO 5.连接主流和广播流
    BroadcastConnectedStream<JSONObject, String> connectedStream = odsBaseDbDS.connect(broadcastStream);
    
    //TODO 6.分流  处理数据  广播流数据,主流数据(根据广播流数据进行处理)
    OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase-tag") {};
    SingleOutputStreamOperator<JSONObject> kafka = connectedStream.process(new TableProcessFunction(hbaseTag, mapStateDescriptor));
    
    class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
        public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out){
      //写入状态,广播出去
     BroadcastState<String,TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
     broadcastState.put(key, tableProcess);
        }
    }
    
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out){
        //1.获取状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        TableProcess tableProcess = broadcastState.get(key);
    }
    ```

- KeyedBroadcastProcessFunction

- ProcessWindowFunction

  - ```java
    ds.keyBy("id").timeWindow.process(new ProcessWindowFunction<IN, OUT, KEY, W extends Window>(){
        @Override
        public void process(KEY, Context, Iterable<IN>, Collector<OUT>){} 
    })
    ```

    

- ProcessAllWindowFunction  

  - .timewindowAll.apply(ProcessAllWindowFunction  )



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
.keyby('id')
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

- 包含以下组件

  - Event Stream

  - pattern 定义

    - ```java
      Pattern.<LoginEvent>begin("begin")
      .where(new SimpleCondition)
      .next("next")
      .where(new SimpleCondition)
      .within(Time.seconds(10)
      ```

    - followedBy ：非严格近邻； next：严格近邻

  - pattern 检测  

    - patternStream  = CEP.pattern(input, pattern)  

  -  生成 Alert  

    - 一旦获得 PatternStream，我们就可以通过 select 或 flatSelect，从一个 Map 序列
      找到我们需要的警告信息。 
      - select： 实现一个 PatternSelectFunction  
      - flatSelect：实现一个PatternFlatSelectFunction  
      - 超时事件的处理  ：实现PatternTimeoutFunction 和 PatternFlatTimeoutFunction   

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

​	WM = 事件时间-乱序时间（**是一种衡量EventTime进展的机制**，用于处理乱序）；当前WM时间下的数据到齐了

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

- 流式计算分为有状态和无状态

  - 有状态：过去一小时的平均温度（依赖历史）
  - 无状态：输出温度大于30度（每个独立事件）

- 有状态算子

  - 如ProcessWindowFunction会缓存输入的数据/ProcessFunction会保存设置的定时器等

  - 主要有2中状态

    - 算子状态

      - 列表状态/联合列表状态/广播状态(一个slot中的所有task都可访问，不同slot不能访问)

    - 键控状态

      - ds.keyBy后，可以在richFunction中使用键控状态

      - ValueState/ListState<K> /MapState<K, V> （具有相同 key 的所有数据都会访问相同的状态 ）

      - 声明：ValueState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-payed", Boolean.class, false));

      - 状态设置过期

        - ```java
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

##### 状态一致性

- 一致性级别

  - at-most-once

  - at-least-once

    - 可通过幂等性（一个操作可重复多次，但只有一次导致结果更改）弥补

  - exactly-once

    - spark streaming保证exactly once，代价是只能批处理。flink保证了exactly once，且低延迟高吞吐

    - 通过端到端一致性实现

      - **内部保证** - 依赖checkpoint

      - **source端** - 外部数据源和重设数据的读取位置

      - **sink端** - 保证从故障中恢复时，数据不会重复写入外部系统

        - 幂等写入（flink未采用）：一个操作可重复多次，但只有一次导致结果更改

          1. > HashMap就是典型的幂等写入，为什么flink没采用幂等写入。比如sink端使用redis。我们在大屏检测温度 10,12,15 | 20,21,25(此时还没来得及插入barrier "|"，那么要回退到上一个barrier（15）开始重新输入)  15,20,21,25  虽然reids最终也会收到25，但是大屏显示时候会出现21->15的温度下降，这显然出现问题。故障恢复会出现暂时不一致

        - 事务写入（flink采用）：需要构建事务来写入外部系统，构建的事务对应着 checkpoint，等到 checkpoint真正完成的时候，才把所有对应的结果写入 sink 系统中。  

          - 预写日志（WAL）

            - > 预写日志把数据先保存在sink，待checkpoint完成后再写入，相当于批处理，不是严格实时。并且在最后真正写入sink时候，如果报错，还要重新写入那就会重复

          - <font color='red'>两阶段提交</font>

            - > <font color='red'>具体的两阶段提交步骤总结如下：</font>
              > 1  第一条数据来了之后，开启一个 kafka 的事务（ transaction），正常写入
              > kafka 分区日志但标记为未提交，这就是“预提交”
              > 2  jobmanager 触发 checkpoint 操作， barrier 从 source 开始向下传递，遇到barrier 的算子将状态存入状态后端，并通知 jobmanager
              > 3  **sink 连接器收到 barrier，保存当前状态，存入 checkpoint，通知**
              > **jobmanager，并开启下一阶段的事务，用于提交下个检查点的数据**
              > 4  jobmanager 收到所有任务的通知，发出确认信息，表示 checkpoint 完成
              > 5  sink 任务收到 jobmanager 的确认信息，正式提交这段时间的数据
              > 6  外部 kafka 关闭事务，提交的数据可以正常消费了。

              ![image-20221207164914276](D:\workLv\learn\proj\hadoop-doc\collection\pics\FlinkPics\flink-两阶段提交.png)

    - ![image-20221207151553926](D:\workLv\learn\proj\hadoop-doc\collection\pics\FlinkPics\flink一致性.png)

##### 检查点

flink内部使用checkpoint保证exactly-once。当checkpoint启动，JM会加入ckpt(checkpoint barriers，类似普通记录，**由算子处理（每个transform任务会触发）**，不参与计算，由JM协调各个TM触发状态**异步保存**)到数据流（env.addsouce()这个环节存入）。

比如 ds.keyBy.map操作,**keyBy算子**会保存输入流kafka的偏移量到flink的状态后端，**map算子** 保存状态到hdfs。如果失败，则["a",2]、 ["a",2]和["c",2]这几条记录将被重播 。



![image-20221207160630461](../../\collection\pics\FlinkPics\flink-检查点.png)

##### 状态后端

- MemoryStateBackend  
  - 内存级的状态后端， 会将键控状态作为内存中的对象进行管理，将它们存储
    在 TaskManager 的 JVM 堆上；而将 checkpoint 存储在 JobManager 的内存中
- FsStateBackend  
  - 将 checkpoint 存到远程的持久化文件系统（ FileSystem）上。而对于本地状
    态，跟 MemoryStateBackend 一样，也会存在 TaskManager 的 JVM 堆上  
- RocksDBStateBackend  
  - 将所有状态序列化后，存入本地的 RocksDB 中存储。
    注意： RocksDB 的支持并不直接包含在 flink 中，需要引入依赖：  

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

##### 碎片

 - slot是指TM具有的并发能力

 - 一个taskmanager是一个jvm进程，里面包含了一定数量的slot，这些slot的内存隔离，cpu不隔离

 - 依赖

   	-  one-to-one：map,flatMap (spark的窄依赖)
      	-  Redistributing  ： keyby,broadcast,rebalance 重分区（spark的宽依赖）

-  Ingestion Time：是数据进入 Flink 的时间。 Processing Time  ：进入算子时间

- - - 

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

  - jobmanager:主节点，类似于spark中的master
  
  - taskManager：从节点，类似于spark中的worker
    - slot：插槽，类似于spark中executor中的线程，只不过flink中的slot是物理存在的，可以手动配置，每个slot执行一个任务，是静态概念，用来隔绝内存。但slot的个数不能多于cpu-cores。并行度上限不能大于slot的数量。
    - 并行度：一个特定算子的子任务的个数（spark中是RDD中partition的数量）
  
  

##### MR | SPARK | FLINK

###### MR

![image-20220529173418419](../../\collection\pics\FlinkPics\mr-yarn.png)

```java
//（1） 作业提交 
第 1 步： Client 调用 job.waitForCompletion 方法，向整个集群提交 MapReduce 作业。
第 2 步： Client 向 RM 申请一个作业 id。
第 3 步： RM 给 Client 返回该 job 资源的提交路径和作业 id。
第 4 步： Client 提交 jar 包、切片信息和配置文件到指定的资源提交路径。
第 5 步： Client 提交完资源后，向 RM 申请运行 MrAppMaster。
//（2） 作业初始化
第 6 步： 当 RM 收到 Client 的请求后，将该 job 添加到容量调度器中。尚硅谷大数据技术之 Hadoop（Yarn）
第 7 步： 某一个空闲的 NM 领取到该 Job。
第 8 步： 该 NM 创建 Container， 并产生 MRAppmaster。
第 9 步：下载 Client 提交的资源到本地。
//（3） 任务分配
第 10 步： MrAppMaster 向 RM 申请运行多个 MapTask 任务资源。
第 11 步： RM 将运行 MapTask 任务分配给另外两个 NodeManager， 另两个 NodeManager
分别领取任务并创建容器。
//（4） 任务运行
第 12 步： MR 向两个接收到任务的 NodeManager 发送程序启动脚本， 这两个
NodeManager 分别启动 MapTask， MapTask 对数据分区排序。
第13步： MrAppMaster等待所有MapTask运行完毕后，向RM申请容器， 运行ReduceTask。
第 14 步： ReduceTask 向 MapTask 获取相应分区的数据。
第 15 步： 程序运行完毕后， MR 会向 RM 申请注销自己。
//（5） 进度和状态更新
YARN 中的任务将其进度和状态(包括 counter)返回给应用管理器, 客户端每秒(通过
mapreduce.client.progressmonitor.pollinterval 设置)向应用管理器请求进度更新, 展示给用户。
//（6） 作业完成
除了向应用管理器请求作业进度外, 客户端每 5 秒都会通过调用 waitForCompletion()来
检查作业是否完成。 时间间隔可以通过 mapreduce.client.completion.pollinterval 来设置。 作业
完成之后, 应用管理器和 Container 会清理工作状态。 作业的信息会被作业历史服务器存储
以备之后用户核查。
```

###### SPARK

![image-20221206104209723](../../\collection\pics\FlinkPics\spark运行.png)

spark on yarn有两种模式

<strong>Spark Client模式</strong>（01_尚硅谷大数据技术之SparkCore.docx/4.4提交流程）客户端提交程序后，会在客户端上启动Driver，再向RM申请运行AM,RM分配Container运行AM，AM向RM申请Container运行Executor，Executor启动后反向注册Driver，待Executor全部注册后运行main，构建sparkContext，遇到Action触发Job，根据宽依赖划分stage，将taskset发送到Taskscheduler，最后将task分发到各个executor上执行。

<strong>Spark Cluster模</strong> Client提交程序到集群上后，会和RM通讯申请运行AM。<font color='red'>此时AM就是Driver（客户端模式现有Driver再有AM）</font>，官网上说Diver是运行AM里，可以理解为此时AM包含了Driver的功能，既可以进行资源申请，又可以有Driver的RDD生成，Task生成和分发，向AM申请资源接着AM向RM申请资源等。





###### Flink

![image-20220529163957321](D:\workLv\learn\proj\hadoop-doc\collection\pics\FlinkPics\flink-yarn.png)

- 文字描述
  - ​	*Flink 任务提交后， Client 向 HDFS 上传 Flink 的 Jar 包和配置，之后向 Yarn*
    *ResourceManager 提交任务， ResourceManager 分配 Container 资源并通知对应的*
    *NodeManager 启动 ApplicationMaster， ApplicationMaster 启动后加载 Flink 的 Jar 包*
    *和配置构建环境，然后启动 JobManager，之后 ApplicationMaster 向 ResourceManager*
    *申 请 资 源 启 动 TaskManager ， ResourceManager 分 配 Container 资 源 后 ， 由*
    *ApplicationMaster 通 知 资 源 所 在 节 点 的 NodeManager 启 动 TaskManager ，*
    *NodeManager 加载 Flink 的 Jar 包和配置构建环境并启动 TaskManager， TaskManager*
    *启动后向 JobManager 发送心跳包，并等待 JobManager 向其分配任务。（<font color='blue'>补充TM启动后会向RM注册资源，RM向TM发出提供slots的指令，TM提供slots给JM，JM提交要在slots中执行的任务</font>）*



###### 共同点

hadoop和spark的yarn都有ApplicationMaster。

hadoop（05_尚硅谷大数据技术之Hadoop（Yarn）V3.3.pdf/1.2 Yarn 工作机制 ），MR程序提交，YarnRunner向RM申请Application，RM返回资源提交路径， MR程序资源提交完毕后，申请运行MRAppMaster，RM分配Container，在NM上启动MRAppMaster，MRAppMaster向RM申请容器运行MapTask/ReduceTask 

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

#### 面试题

##### 14 flink 的 state 是存储在哪里的
Apache Flink内部有四种state的存储实现，具体如下：

基于内存的HeapStateBackend - 在debug模式使用，不 建议在生产模式下应用；
基于HDFS的FsStateBackend - 分布式文件持久化，每次读写都产生网络IO，整体性能不佳；
基于RocksDB的RocksDBStateBackend - 本地文件+异步HDFS持久化；
基于Niagara(Alibaba内部实现)NiagaraStateBackend - 分布式持久化- 在Alibaba生产环境应用；

##### 15 flink是如何实现反压的

反压（背压）是在实时数据处理中，数据管道某个节点上游产生数据的速度大于该节点处理数据速度的一种现象。

flink的反压经历了两个发展阶段,分别是基于TCP的反压(<1.5)和基于credit的反压(>1.5)

基于 TCP 的反压
flink中的消息发送通过RS(ResultPartition),消息接收通过IC(InputGate),两者的数据都是以 LocalBufferPool的形式来存储和提取,进一步的依托于Netty的NetworkBufferPool,之后更底层的便是依托于TCP的滑动窗口机制,当IC端的buffer池满了之后,两个task之间的滑动窗口大小便为0,此时RS端便无法再发送数据

基于TCP的反压最大的问题是会造成整个TaskManager端的反压,所有的task都会受到影响

基于 Credit 的反压
RS与IC之间通过backlog和credit来确定双方可以发送和接受的数据量的大小以提前感知,而不是通过TCP滑动窗口的形式来确定buffer的大小之后再进行反压

定位造成反压问题的节点，通常有两种途径。

- 压监控面板；
- Flink Task Metrics

处理

- 尝试优化代码；
- 针对特定资源对Flink进行调优；
- 增加并发或者增加机器

##### 16 flink中的时间概念 , eventTime 和 processTime的区别



##### 17 flink中的session Window怎样使用
会话窗口主要是将某段时间内活跃度较高的数据聚合成一个窗口进行计算,窗口的触发条件是 Session Gap, 是指在规定的时间内如果没有数据活跃接入,则认为窗口结束,然后触发窗口结果

Session Windows窗口类型比较适合非连续性数据处理或周期性产生数据的场景,根据用户在线上某段时间内的活跃度对用户行为进行数据统计

val sessionWindowStream = inputStream
.keyBy(_.id)
//使用EventTimeSessionWindow 定义 Event Time 滚动窗口
.window(EventTimeSessionWindow.withGap(Time.milliseconds(10)))
.process(......)
Session Window 本质上没有固定的起止时间点,因此底层计算逻辑和Tumbling窗口及Sliding 窗口有一定的区别,

Session Window 为每个进入的数据都创建了一个窗口,最后再将距离窗口Session Gap 最近的窗口进行合并,然后计算窗口结果



##### 18 讲一下flink on yarn的部署
Flink作业提交有两种类型:

- Session-cluster 模式：  
  - 需要先启动集群，然后再提交作业，接着会向 yarn 申请一
    块空间后，资源永远保持不变。如果资源满了，下一个作业就无法提交，只能等到
    yarn 中的其中一个作业执行完成后，释放了资源，下个作业才会正常提交。所有作
    业共享 Dispatcher 和 ResourceManager；共享资源；适合规模小执行时间短的作业。
    在 yarn 中初始化一个 flink 集群，开辟指定的资源，以后提交任务都向这里提
    交。这个 flink 集群会常驻在 yarn 集群中，除非手工停止。  

- Per-Job-Cluster 模式  
  - 一个 Job 会对应一个集群，每提交一个作业会根据自身的情况，都会单独向 yarn
    申请资源，直到作业执行完成，一个作业的失败与否并不会影响下一个作业的正常
    提交和运行。独享 Dispatcher 和 ResourceManager，按需接受资源申请；适合规模大
    长时间运行的作业。
    每次提交都会创建一个新的 flink 集群，任务之间互相独立，互不影响，方便管
    理。任务执行完成之后创建的集群也会消失  

##### 19 flink 的 window 实现机制

Flink 中定义一个窗口主要需要以下三个组件。

- **Window Assigner：**用来决定某个元素被分配到哪个/哪些窗口中去。
- **Trigger：**触发器。决定了一个窗口何时能够被计算或清除，每个窗口都会拥有一个自己的Trigger。
- **Evictor：**可以译为“驱逐者”。在Trigger触发之后，在窗口被处理之前，Evictor（如果有Evictor的话）会用来剔除窗口中不需要的元素，相当于一个filter。

Window 的实现

首先上图中的组件都位于一个算子（window operator）中，数据流源源不断地进入算子，每一个到达的元素都会被交给 WindowAssigner。WindowAssigner 会决定元素被放到哪个或哪些窗口（window），可能会创建新窗口。因为一个元素可以被放入多个窗口中，所以同时存在多个窗口是可能的。注意，`Window`本身只是一个ID标识符，其内部可能存储了一些元数据，如`TimeWindow`中有开始和结束时间，但是并不会存储窗口中的元素。窗口中的元素实际存储在 Key/Value State 中，key为`Window`，value为元素集合（或聚合值）。为了保证窗口的容错性，该实现依赖了 Flink 的 State 机制（参见 [state 文档](https://ci.apache.org/projects/flink/flink-docs-master/apis/streaming/state.html)）。

每一个窗口都拥有一个属于自己的 Trigger，Trigger上会有定时器，用来决定一个窗口何时能够被计算或清除。每当有元素加入到该窗口，或者之前注册的定时器超时了，那么Trigger都会被调用。Trigger的返回结果可以是 continue（不做任何操作），fire（处理窗口数据），purge（移除窗口和窗口中的数据），或者 fire + purge。一个Trigger的调用结果只是fire的话，那么会计算窗口并保留窗口原样，也就是说窗口中的数据仍然保留不变，等待下次Trigger fire的时候再次执行计算。一个窗口可以被重复计算多次直到它被 purge 了。在purge之前，窗口会一直占用着内存。

当Trigger fire了，窗口中的元素集合就会交给`Evictor`（如果指定了的话）。Evictor 主要用来遍历窗口中的元素列表，并决定最先进入窗口的多少个元素需要被移除。剩余的元素会交给用户指定的函数进行窗口的计算。如果没有 Evictor 的话，窗口中的所有元素会一起交给函数进行计算。

计算函数收到了窗口的元素（可能经过了 Evictor 的过滤），并计算出窗口的结果值，并发送给下游。窗口的结果值可以是一个也可以是多个。DataStream API 上可以接收不同类型的计算函数，包括预定义的`sum()`,`min()`,`max()`，还有 `ReduceFunction`，`FoldFunction`，还有`WindowFunction`。WindowFunction 是最通用的计算函数，其他的预定义的函数基本都是基于该函数实现的。

Flink 对于一些聚合类的窗口计算（如sum,min）做了优化，因为聚合类的计算不需要将窗口中的所有数据都保存下来，只需要保存一个result值就可以了。每个进入窗口的元素都会执行一次聚合函数并修改result值。这样可以大大降低内存的消耗并提升性能。但是如果用户定义了 Evictor，则不会启用对聚合窗口的优化，因为 Evictor 需要遍历窗口中的所有元素，必须要将窗口中所有元素都存下来。