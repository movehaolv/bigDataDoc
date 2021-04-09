package com.atguigu.kfa;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 13:39 2021/2/22
 */


import org.apache.kafka.clients.producer.*;
import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class CustomProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
//kafka 集群， broker list
        props.put("bootstrap.servers", "had01:9092");
        props.put("acks", "all");
//        重试次数
        props.put("retries", 1);
//        批次大小
        props.put("batch.size", 16384);
//        等待时间
        props.put("linger.ms", 1);
//RecordAccumulator 缓冲区大小
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer <String, String> producer = new KafkaProducer(props);
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>("first", Integer.toString(i), Integer.toString(i)));
        }
        producer.close();
    }
}