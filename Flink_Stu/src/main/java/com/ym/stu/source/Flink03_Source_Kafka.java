package com.ym.stu.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author yomo
 * @create 2022-03-30 12:28
 */
public class Flink03_Source_Kafka {

    public static void main(String[] args) throws Exception {
        // 1.流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.从kafka读取数据
        /**官网读取方法
         * KafkaSource<String> source = KafkaSource.<String>builder()
         *     .setBootstrapServers(brokers)
         *     .setTopics("input-topic")
         *     .setGroupId("my-group")
         *     .setStartingOffsets(OffsetsInitializer.earliest())
         *     .setValueOnlyDeserializer(new SimpleStringSchema())
         *     .build();
         *
         * env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
         */
        //2.kafka相关配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "Flink03_Source_Kafka");
        properties.setProperty("auto.offset.reset", "latest");

        env.addSource(new FlinkKafkaConsumer<String>("sensor",new SimpleStringSchema(),properties)).print("Flink03_Source_Kafka");

        //3.执行
        env.execute();
    }

}
