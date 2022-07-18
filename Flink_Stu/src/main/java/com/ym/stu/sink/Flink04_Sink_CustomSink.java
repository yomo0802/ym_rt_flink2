package com.ym.stu.sink;

import com.ym.stu.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;

/**
 * @author yomo
 * @create 2022-04-06 10:46
 */
public class Flink04_Sink_CustomSink {

    public static void main(String[] args) throws Exception {
        /**
         * 	在mysql中创建数据库和表
         * create database test;
         * use test;
         * CREATE TABLE `sensor` (
         *   `id` varchar(20) NOT NULL,
         *   `ts` bigint(20) NOT NULL,
         *   `vc` int(11) NOT NULL,
         *   PRIMARY KEY (`id`,`ts`)
         * ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
         */
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

        // 1.流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //2.
        env.fromCollection(waterSensors)
                .addSink(new RichSinkFunction<WaterSensor>() {

                    private PreparedStatement preparedStatement;
                    private Connection connection;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?useSSL=false", "root", "123456");
                        preparedStatement = connection.prepareStatement("insert into sensor values(?, ?, ?)");
                    }

                    @Override
                    public void close() throws Exception {
                        preparedStatement.close();
                        connection.close();
                    }

                    @Override
                    public void invoke(WaterSensor value, Context context) throws Exception {
                        preparedStatement.setString(1,value.getId());
                        preparedStatement.setLong(2,value.getTs());
                        preparedStatement.setInt(3,value.getVc());
                        preparedStatement.execute();

                    }
                });

        //3.执行
        env.execute();

    }

}
