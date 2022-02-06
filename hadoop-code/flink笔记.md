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



##### OnTime触发

```java
// Flink保证同步调用onTimer()和processElement() 。因此用户不必担心状态的并发修改。
// FLink的定时器触发，是通过waterMark进行触发的，混乱程度 2s,  定时器只能触发一次ctx.timerService().currentWatermark()为上一个WM
// 注册的定时器是针对key的
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

   

