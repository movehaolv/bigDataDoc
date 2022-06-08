package com.atguigu.networkflow_analysis;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: UserBehaviorAnalysis
 * Package: com.atguigu.networkflow_analysis
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/11/16 9:31
 */

import akka.protobuf.ByteString;
import cn.hutool.core.lang.func.Func1;
import com.atguigu.networkflow_analysis.beans.ApacheLogEvent;
import com.atguigu.networkflow_analysis.beans.PageViewCount;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.bouncycastle.util.Times;

import java.net.URL;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @ClassName: HotPages
 * @Description:
 * 我们在这里先实现“ 热门页面浏览数”的统计，也就是读取服务器日志中的每
 * 一行 log， 统计在一段时间内用户访问每一个 url 的次数，然后排序输出显示。
 * 具体做法为： 每隔 5 秒，输出最近 10 分钟内访问量最多的前 N 个 URL。 可以
 * 看出，这个需求与之前“实时热门商品统计”非常类似，所以我们完全可以借鉴此
 * 前的代码。
 * @Author: wushengran on 2020/11/16 9:31
 * @Version: 1.0
 */
public class HotPages {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
//        env.getConfig().setAutoWatermarkInterval(200000);

        // 读取文件，转换成POJO
//        URL resource = HotPages.class.getResource("/test.log");
        URL resource = HotPages.class.getResource("/apache.log");
        DataStream<String> inputStream = env.readTextFile(resource.getPath());
//        DataStream<String> inputStream = env.socketTextStream("192.168.17.133", 7777);

        DataStream<ApacheLogEvent> dataStream = inputStream
                .map(line -> {
                    String[] fields = line.split(" ");
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                    Long timestamp = simpleDateFormat.parse(fields[3]).getTime();
                    return new ApacheLogEvent(fields[0], fields[1], timestamp, fields[5], fields[6]);
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(ApacheLogEvent element) {
                        return element.getTimestamp();
                    }
                });

        dataStream.print("data");

        // 分组开窗聚合

        // 定义一个侧输出流标签
        OutputTag<ApacheLogEvent> lateTag = new OutputTag<ApacheLogEvent>("late"){};

        SingleOutputStreamOperator<PageViewCount> windowAggStream = dataStream
                .filter(data -> "GET".equals(data.getMethod()))    // 过滤get请求
                .filter(data -> {
                    String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                    return Pattern.matches(regex, data.getUrl());
                })
                .keyBy(ApacheLogEvent::getUrl)    // 按照url分组
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(lateTag)
                .aggregate(new PageCountAgg(), new PageCountResult());
/**
 * 经测试，滑动窗口使用allowedLateness(Time.minutes(1))，要进入侧输出流需要保证来的数据不被其他窗口占用 timeWindow(Time.minutes(10), Time.seconds(5))， maxOutOfOrderness=1
 *  例子：
 *  10:25:49 （该窗口结束时间为10:25:50）
 *  10:26:51  (窗口(10:15:50-10:25:50)关闭）
 *  10:25:46 (不会进入侧输出流，因为还属于(10:15:55-10:25:55),(10:16:00-10:26:00)...(10:25:45-10:35:45)
 *  10:15:51 (进入侧输出流，属于(10:15:50-10:15:55)
 */

//        windowAggStream.print("agg");
        windowAggStream.map(new MapFunction<PageViewCount, Object>() {
            @Override
            public Object map(PageViewCount value) throws Exception {
                return value + "  "   + new Timestamp(value.getWindowEnd()) ;
            }
        }).print("agg ");
        windowAggStream.getSideOutput(lateTag).print("late");

        // 收集同一窗口count数据，排序输出
        DataStream<String> resultStream = windowAggStream
                .keyBy(PageViewCount::getWindowEnd)
                .process(new TopNHotPages(3));

        resultStream.print("resultStream ");

        env.execute("hot pages job");
    }

    // 自定义预聚合函数
    public static class PageCountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    // 实现自定义的窗口函数
    public static class PageCountResult implements WindowFunction<Long, PageViewCount, String, TimeWindow> {
        @Override
        public void apply(String url, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
            out.collect(new PageViewCount(url, window.getEnd(), input.iterator().next()));
        }
    }

    // 实现自定义的处理函数
    public static class TopNHotPages extends KeyedProcessFunction<Long, PageViewCount, String> {
        private Integer topSize;

        public TopNHotPages(Integer topSize) {
            this.topSize = topSize;
        }


        // 定义状态，保存当前所有PageViewCount到Map中
//        ListState<PageViewCount> pageViewCountListState;
        MapState<String, Long> pageViewCountMapState;

        @Override
        public void open(Configuration parameters) throws Exception {
//            pageViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<PageViewCount>("page-count-list", PageViewCount.class));
            pageViewCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("page-count-map", String.class, Long.class));
        }

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<String> out) throws Exception {
//            pageViewCountListState.add(value);
//            System.out.println("processElement  " + new Timestamp(value.getWindowEnd()));
            System.out.println("processElement  WMCUR  " + ctx.timerService().currentWatermark() + "  " + value);
            pageViewCountMapState.put(value.getUrl(), value.getCount());
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);  // 因为是以PageViewCount::getWindowEnd
            // 作为key， +1说明另一个已经窗口已经到了
            // 注册一个1分钟之后的定时器，用来清空状态（因为allowedLateness(Time.minutes(1))，说明在一分钟之内还有数据过来，一分钟之后数据会进入侧输出流，这时需要清空数据，避免数据累加 ）
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 60 * 1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            //             先判断是否到了窗口关闭清理时间，如果是，直接清空状态返回
            if( timestamp == ctx.getCurrentKey() + 60 * 1000L ){
                System.out.println("删除定时器 -----------------------------------  " + new Timestamp(timestamp));
                pageViewCountMapState.clear();
                return;
            }

            System.out.println(" onTimer WMCUR  " + ctx.timerService().currentWatermark() );
            ArrayList<Map.Entry<String, Long>> pageViewCounts = Lists.newArrayList(pageViewCountMapState.entries());

            pageViewCounts.sort(new Comparator<Map.Entry<String, Long>>() {
                @Override
                public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                    if(o1.getValue() > o2.getValue())
                        return -1;
                    else if(o1.getValue() < o2.getValue())
                        return 1;
                    else
                        return 0;
                }
            });

            // 格式化成String输出
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("当前定时器时间:  " + new Timestamp(timestamp) + " getCurrentKey: " + new Timestamp(ctx.getCurrentKey())  + " " +
                    "当前WM：" + new Timestamp(ctx.timerService().currentWatermark()) +
                    "===============\n");
            resultBuilder.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");

            // 遍历列表，取top n输出
            for (int i = 0; i < Math.min(topSize, pageViewCounts.size()); i++) {
                Map.Entry<String, Long> currentItemViewCount = pageViewCounts.get(i);
                resultBuilder.append("NO ").append(i + 1).append(":")
                        .append(" 页面URL = ").append(currentItemViewCount.getKey())
                        .append(" 浏览量 = ").append(currentItemViewCount.getValue())
                        .append("\n");
            }
            resultBuilder.append("===============================\n\n");

            // 控制输出频率
//            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());



//            pageViewCountListState.clear();
        }
    }
}


