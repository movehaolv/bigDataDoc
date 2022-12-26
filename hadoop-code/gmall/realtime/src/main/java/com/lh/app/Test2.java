package com.lh.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.lh.app.function.CustomerDeserialization;
import com.lh.bean.OrderWide;
import com.lh.bean.PaymentWide;
import com.lh.bean.ProductStats;
import com.lh.common.GmallConstant;
import com.lh.utils.DateTimeUtil;
import com.lh.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 20:42 2022/11/6
 */

// Q1， 通过流如何实现批处理的group效果

public class Test2 {

    public static void main(String[] args) throws Exception {
//
///opt/module/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --topic dwd_page_log --time -1 --broker-list node01:9092 --partitions 0    4567
//                /opt/module/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --topic dwm_order_wide --time -1 --broker-list node01:9092 --partitions 0  1281
//                /opt/module/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --topic dwm_payment_wide --time -1 --broker-list node01:9092 --partitions 0  0
//                /opt/module/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --topic dwd_cart_info --time -1 --broker-list node01:9092 --partitions 0  8535
//                /opt/module/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --topic dwd_favor_info --time -1 --broker-list node01:9092 --partitions 0  500
//                /opt/module/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --topic dwd_order_refund_info --time -1 --broker-list node01:9092 --partitions 0   120
//                /opt/module/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --topic dwd_comment_info --time -1 --broker-list node01:9092 --partitions 0   607
//
//
//
//                /opt/module/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --topic pvDS --time -1 --broker-list node01:9092 --partitions 0
//                /opt/module/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --topic orderDS --time -1 --broker-list node01:9092 --partitions 0
//                /opt/module/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --topic payDS --time -1 --broker-list node01:9092 --partitions 0
//                /opt/module/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --topic cartDS --time -1 --broker-list node01:9092 --partitions 0
//                /opt/module/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --topic favorDS --time -1 --broker-list node01:9092 --partitions 0
//                /opt/module/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --topic refundDS --time -1 --broker-list node01:9092 --partitions 0
//                /opt/module/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --topic commentDS --time -1 --broker-list node01:9092 --partitions 0
//                /opt/module/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --topic unionDS --time -1 --broker-list node01:9092 --partitions 0
//                /opt/module/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --topic dws_vs --time -1 --broker-list node01:9092 --partitions 0
//
//
//
//        kafka-topics.sh --zookeeper node01:2181 --delete --topic pvDS
//        kafka-topics.sh --zookeeper node01:2181 --delete --topic favorDS
//        kafka-topics.sh --zookeeper node01:2181 --delete --topic cartDS
//        kafka-topics.sh --zookeeper node01:2181 --delete --topic orderDS
//        kafka-topics.sh --zookeeper node01:2181 --delete --topic payDS
//        kafka-topics.sh --zookeeper node01:2181 --delete --topic refundDS
//        kafka-topics.sh --zookeeper node01:2181 --delete --topic commentDS
//        kafka-topics.sh --zookeeper node01:2181 --delete --topic unionDS
//        kafka-topics.sh --zookeeper node01:2181 --delete --topic dws_vs





    }
}
