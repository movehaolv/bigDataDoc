package com.lh.utils;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 16:14 2022/8/9
 */


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import java.util.Properties;


public class MyKafkaUtil {
    private static String brokers = "node01:9092,node02:9092,node03:9092";  // "192.168.17.133:9092"
    private static String defaultTopic = "DWD_DEFAULT_TOPIC";

    public static FlinkKafkaProducer<String>  getProducer(String topic){
        return new FlinkKafkaProducer<String>(brokers, topic, new SimpleStringSchema());
    }


    public static <T> FlinkKafkaProducer<T> getProducer(KafkaSerializationSchema<T> serializationSchema){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        return new FlinkKafkaProducer<T>(defaultTopic, serializationSchema, properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }


    public static FlinkKafkaConsumer<String> getConsumer(String topic, String groupId){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
//        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);

    }

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {

        Properties properties = new Properties();
//        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new FlinkKafkaConsumer<String>(topic,
                new SimpleStringSchema(),
                properties);

    }


    //拼接Kafka相关属性到DDL
    public static String getKafkaDDL(String topic, String groupId) {
        return  " 'connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + brokers + "', " +
//                " 'properties.group.id' = '" + groupId + "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'earliest-offset'  ";
    }

}