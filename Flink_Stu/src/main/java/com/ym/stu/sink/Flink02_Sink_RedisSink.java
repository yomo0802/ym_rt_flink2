package com.ym.stu.sink;

import com.alibaba.fastjson.JSON;
import com.ym.stu.bean.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.ArrayList;

/**
 * @author yomo
 * @create 2022-04-06 10:14
 */
public class Flink02_Sink_RedisSink {

    public static void main(String[] args) throws Exception {

        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

        //连接Redis配置
        FlinkJedisPoolConfig redisConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .setPort(6379)
                .setMaxTotal(100)
                .setTimeout(1000 * 10)
                .build();

        // 1.流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //2.
        env.fromCollection(waterSensors)
                .addSink(new RedisSink<>(redisConfig, new RedisMapper<WaterSensor>() {
                /**
                key                 value(hash)
                "sensor"            field           value
                                    sensor_1        {"id":"sensor_1","ts":1607527992000,"vc":20}
                                    ...             ...
               */

                    @Override
            public RedisCommandDescription getCommandDescription() {
                //返回存在Redis中的数据类型,存储时Hash,第二个参数是外面的key
                return new RedisCommandDescription(RedisCommand.HSET,"sensor");
            }

            @Override
            public String getKeyFromData(WaterSensor data) {
                //从数据中获取key ：Hash的key
                return data.getId();
            }

            @Override
            public String getValueFromData(WaterSensor data) {
                //从数据中获取value : Hash的value
                return JSON.toJSONString(data);
            }
        }));
        /**
         * Redis查看是否收到数据
         * 启动redis
         * redis-cli --raw
         */


        //3.执行
        env.execute();

    }

}
