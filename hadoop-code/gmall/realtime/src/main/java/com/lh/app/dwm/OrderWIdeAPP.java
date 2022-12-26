package com.lh.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.lh.app.function.DimAsyncFunction;
import com.lh.bean.OrderDetail;
import com.lh.bean.OrderInfo;
import com.lh.bean.OrderWide;
import com.lh.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 16:25 2022/10/24
 */
//value:{"sinkTable":"dim_base_trademark","database":"gmall-210325-flink","before":{"tm_name":"atguigu","id":12},"after":{"tm_name":"Atguigu","id":12},"type":"update","tableName":"base_trademark"}

public class OrderWIdeAPP {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group_0325";

        FlinkKafkaConsumer<String> infoConsumer = MyKafkaUtil.getKafkaConsumer(orderInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> detailConsumer = MyKafkaUtil.getKafkaConsumer(orderDetailSourceTopic, groupId);

        DataStreamSource<String> infoDS = env.addSource(infoConsumer);
        DataStreamSource<String> detailDS = env.addSource(detailConsumer);

        SingleOutputStreamOperator<OrderInfo> infoDSWithWM = infoDS.map(line -> {
            OrderInfo orderInfo = JSON.parseObject(line, OrderInfo.class);
            String create_time = orderInfo.getCreate_time();
            String[] fields = create_time.split(" ");
            orderInfo.setCreate_date(fields[0]);
            orderInfo.setCreate_hour(fields[1].split(":")[0]);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            long ts = sdf.parse(create_time).getTime();
            orderInfo.setCreate_ts(ts);
            return orderInfo;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(
                new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }
        ));

        SingleOutputStreamOperator<OrderDetail> detailDSWithWM = detailDS.map(x -> {
            OrderDetail orderDetail = JSON.parseObject(x, OrderDetail.class);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            long ts = sdf.parse(orderDetail.getCreate_time()).getTime();
            orderDetail.setCreate_ts(ts);
            return orderDetail;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(
                new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }
        ));

        SingleOutputStreamOperator<OrderWide> wideDS = infoDSWithWM.keyBy(new KeySelector<OrderInfo, Long>() {
            @Override
            public Long getKey(OrderInfo value) throws Exception {
                return value.getId();
            }
        }).intervalJoin(detailDSWithWM.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left, OrderDetail right, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left, right));
                    }
                });

// add dim

        // 4.1 关联用户维度

        SingleOutputStreamOperator<OrderWide> wideDSAddUser = AsyncDataStream.unorderedWait(wideDS, new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {

            @Override
            public String getKey(OrderWide input) {
                return input.getUser_id().toString();
            }

            @Override
            public void join(OrderWide input, JSONObject dims) throws ParseException {
                String birthday = dims.getString("BIRTHDAY");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                long ts = sdf.parse(birthday).getTime();
                long curTs = System.currentTimeMillis();
                long age = (curTs-ts)/(365*24*60*60*1000L);
                input.setUser_age((int) age);

                input.setUser_gender(dims.getString("GENDER"));

            }
        }, 60, TimeUnit.SECONDS);


        //4.2 关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(wideDSAddUser, new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {

            @Override
            public String getKey(OrderWide input) {
                return input.getProvince_id().toString();
            }

            @Override
            public void join(OrderWide input, JSONObject dims) {
                input.setProvince_name(dims.getString("NAME"));
                input.setProvince_area_code(dims.getString("AREA_CODE"));
                input.setProvince_3166_2_code(dims.getString("ISO_3166_2"));
                input.setProvince_iso_code(dims.getString("ISO_CODE"));
            }
        }, 60, TimeUnit.SECONDS);

        //4.3 关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                        orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                        orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSku_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //4.4 关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
                orderWideWithSkuDS, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //4.5 关联TM维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //4.6 关联Category维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithTmDS, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);

        orderWideWithCategory3DS.print("orderWideWithCategory3DS>>>>>>>>>>>");

        //TODO 5.将数据写入Kafka
        orderWideWithCategory3DS.map(JSON::toJSONString)
                .addSink(MyKafkaUtil.getProducer(orderWideSinkTopic));


        env.execute("OrderWideApp");

    }
}
