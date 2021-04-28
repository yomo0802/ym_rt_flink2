package com.ym.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author ymstart
 * @create 2021-04-26 21:45
 */
public class MyKafkaUtil {

    private static String kafkaServer = "hadoop002:9092,hadoop003:9092,hadoop004:9092";
    public static Properties properties = new Properties();

    //封装kafka消费者  封装操作Kafka的工具类，并提供获取Kafka消费者的方法（读）
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {
        //Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaServer);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),properties);
    }

    //将数据写入到kafka中
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<String>(topic,new SimpleStringSchema(),properties);
    }

}
