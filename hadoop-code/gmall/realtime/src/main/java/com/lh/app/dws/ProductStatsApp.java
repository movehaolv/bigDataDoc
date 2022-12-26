package com.lh.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.lh.app.function.DimAsyncFunction;
import com.lh.bean.OrderWide;
import com.lh.bean.PaymentWide;
import com.lh.bean.ProductStats;
import com.lh.common.GmallConfig;
import com.lh.common.GmallConstant;
import com.lh.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 10:09 2022/11/7
 */


public class ProductStatsApp {
    public static void main(String[] args) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1 设置CK&状态后端
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall-flink-210325/ck"));
        //env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);

        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart());

        //TODO 2.读取Kafka 7个主题的 数据创建流
        String groupId = "product_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";
        DataStreamSource<String> pvDS = env.addSource(MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId));
        DataStreamSource<String> favorDS = env.addSource(MyKafkaUtil.getKafkaConsumer(favorInfoSourceTopic, groupId));
        DataStreamSource<String> cartDS = env.addSource(MyKafkaUtil.getKafkaConsumer(cartInfoSourceTopic, groupId));
        DataStreamSource<String> orderDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId));
        DataStreamSource<String> payDS = env.addSource(MyKafkaUtil.getKafkaConsumer(paymentWideSourceTopic, groupId));
        DataStreamSource<String> refundDS = env.addSource(MyKafkaUtil.getKafkaConsumer(refundInfoSourceTopic, groupId));
        DataStreamSource<String> commentDS = env.addSource(MyKafkaUtil.getKafkaConsumer(commentInfoSourceTopic, groupId));

//        pvDS.addSink(MyKafkaUtil.getProducer("pvDS"));
//        favorDS.addSink(MyKafkaUtil.getProducer("favorDS"));
//        cartDS.addSink(MyKafkaUtil.getProducer("cartDS"));
//        orderDS.addSink(MyKafkaUtil.getProducer("orderDS"));
//        payDS.addSink(MyKafkaUtil.getProducer("payDS"));
//        refundDS.addSink(MyKafkaUtil.getProducer("refundDS"));
//        commentDS.addSink(MyKafkaUtil.getProducer("commentDS"));




        SingleOutputStreamOperator<ProductStats> productStatsWithClickAndDisplayDS = pvDS.flatMap(new FlatMapFunction<String, ProductStats>() {
            @Override
            public void flatMap(String value, Collector<ProductStats> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject page = jsonObject.getJSONObject("page");
                Long ts = jsonObject.getLong("ts");
                String pageId = page.getString("page_id");
                if ("good_detail".equals(pageId) && "sku_id".equals(page.getString("item_type"))) {
                    out.collect(ProductStats.builder()
                            .sku_id(page.getLong("item"))
                            .click_ct(1L)
                            .ts(ts)
                            .build());
                }
                JSONArray displays = jsonObject.getJSONArray("displays");
                if (displays != null  && displays.size() > 0) {
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject display = displays.getJSONObject(i);
                        if ("sku_id".equals(display.getString("item_type"))) {
                            out.collect(ProductStats.builder().sku_id(display.getLong("item")).display_ct(1L).ts(ts).build());
                        }
                    }
                }

            }
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithFavorDS = favorDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            long create_time = sdf.parse(jsonObject.getString("create_time")).getTime();
            return ProductStats.builder().sku_id(jsonObject.getLong("sku_id")).favor_ct(1L).ts(create_time).build();
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithCartDS = cartDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            long create_time = sdf.parse(jsonObject.getString("create_time")).getTime();
            return ProductStats.builder().sku_id(jsonObject.getLong("sku_id")).cart_ct(1L).ts(create_time).build();
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithOrderDS = orderDS.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                OrderWide orderWide = JSON.parseObject(value, OrderWide.class);
                HashSet<Long> set = new HashSet<>();
                set.add(orderWide.getOrder_id());
                return ProductStats.builder()
                        .sku_id(orderWide.getSku_id())
                        .order_sku_num(orderWide.getSku_num())
                        .order_amount(orderWide.getTotal_amount())
                        .orderIdSet(set)
                        .ts(sdf.parse(orderWide.getCreate_time()).getTime())
                        .build();
            }
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithPaymentDS = payDS.map(line -> {
            PaymentWide paymentWide = JSON.parseObject(line, PaymentWide.class);
            HashSet<Long> set = new HashSet<>();
            set.add(paymentWide.getOrder_id());
            return ProductStats.builder()
                    .sku_id(paymentWide.getSku_id())
                    .payment_amount(paymentWide.getTotal_amount())
                    .paidOrderIdSet(set)
                    .ts(sdf.parse(paymentWide.getPayment_create_time()).getTime())
                    .build();
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithRefundDS = refundDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);

            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(jsonObject.getLong("order_id"));

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                    .refundOrderIdSet(orderIds)
                    .ts(sdf.parse(jsonObject.getString("create_time")).getTime())
                    .build();

        });

        SingleOutputStreamOperator<ProductStats> productStatsWithCommentDS = commentDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);

            String appraise = jsonObject.getString("appraise");
            Long goodComment = 0L;
            if (GmallConstant.APPRAISE_GOOD.equals(appraise)) {
                goodComment = 1L;
            }

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .comment_ct(1L)
                    .good_comment_ct(goodComment)
                    .ts(sdf.parse(jsonObject.getString("create_time")).getTime())
                    .build();


        });

        //TODO 4.Union  7个流
        DataStream<ProductStats> unionDS = productStatsWithClickAndDisplayDS.union(
                productStatsWithFavorDS,
                productStatsWithCartDS,
                productStatsWithOrderDS,
                productStatsWithPaymentDS,
                productStatsWithRefundDS,
                productStatsWithCommentDS);
//        unionDS.print("unionDS>>>>>>>>>>>>>>>>>>>>");
        unionDS.map(JSON::toJSONString).addSink(MyKafkaUtil.getProducer("unionDS"));


        WatermarkStrategy<ProductStats> productStatsWatermarkStrategy =
                WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                    @Override
                    public long extractTimestamp(ProductStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                });
        SingleOutputStreamOperator<ProductStats> reduceDS = unionDS.assignTimestampsAndWatermarks(productStatsWatermarkStrategy)
                .keyBy(new KeySelector<ProductStats, Long>() {
                    @Override
                    public Long getKey(ProductStats value) throws Exception {
                        return value.getSku_id();
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                        stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                        stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                        stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                        stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                        stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                        stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                        //stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                        stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                        stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                        stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                        //stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                        stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                        //stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);

                        stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                        stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());
                        return stats1;
                    }
                }, new ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void process(Long aLong, Context context, Iterable<ProductStats> elements, Collector<ProductStats> out) throws Exception {
                        ProductStats next = elements.iterator().next();

                        next.setStt(sdf.format(new Date(context.window().getStart())));
                        next.setEdt(sdf.format(new Date(context.window().getEnd())));

                        next.setOrder_ct((long) next.getOrderIdSet().size());
                        next.setPaid_order_ct((long) next.getPaidOrderIdSet().size());
                        next.setRefund_order_ct((long) next.getRefundOrderIdSet().size());
                        out.collect(next);
                    }
                });
//        reduceDS.print("reduceDS>>>>>>>>>>>>>>>>");
//        reduceDS.map(JSON::toJSONString).addSink(MyKafkaUtil.getProducer("dws_vs"));
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(reduceDS, new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
            @Override
            public String getKey(ProductStats input) {
                return String.valueOf(input.getSku_id());
            }

            @Override
            public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException {

                productStats.setSku_name(dimInfo.getString("SKU_NAME"));
                productStats.setSku_price(dimInfo.getBigDecimal("PRICE"));
                productStats.setSpu_id(dimInfo.getLong("SPU_ID"));
                productStats.setTm_id(dimInfo.getLong("TM_ID"));
                productStats.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
            }
        }, 60, TimeUnit.SECONDS);

        //7.2 关联SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS =
                AsyncDataStream.unorderedWait(productStatsWithSkuDS,
                        new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                                productStats.setSpu_name(jsonObject.getString("SPU_NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getSpu_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //7.3 关联Category维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3DS =
                AsyncDataStream.unorderedWait(productStatsWithSpuDS,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                                productStats.setCategory3_name(jsonObject.getString("NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getCategory3_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //7.4 关联TM维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDS =
                AsyncDataStream.unorderedWait(productStatsWithCategory3DS,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                                productStats.setTm_name(jsonObject.getString("TM_NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getTm_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        productStatsWithTmDS.print("productStatsWithTmDS>>>>>>>>>>>>>>>>>>>>>>");
        env.execute("ProductStatsApp");

    }
}
