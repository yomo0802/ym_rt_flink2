package com.ym.stu.transform;

import com.ym.stu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/** sum,
    min,
     max
    minBy,
    maxBy
    作用  KeyedStream的每一个支流做聚合。执行完成后，会将聚合的结果合成一个流返回，所以结果都是DataStream
    返回  KeyedStream -> SingleOutputStreamOperator

 * @author yomo
 * @create 2022-03-30 16:57
 */
public class Flink10_TransForm_Sum_Min_Max {

    public static void main(String[] args) throws Exception {

        // 1.流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.1 sum max min
//        KeyedStream<Integer, String> integerStringKeyedStream = env.fromElements(1, 2, 3, 4, 5).keyBy(ele -> ele % 2 == 0 ? "奇数" : "偶数" );
//        integerStringKeyedStream.sum(0).print("sum");
//        integerStringKeyedStream.max(0).print("max");
//        integerStringKeyedStream.min(0).print("min");

        //2.2   sum
//        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
//        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
//        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
//        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 30));
//        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
//        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));
//        KeyedStream<WaterSensor, String> waterSensorStringKeyedStream = env.fromCollection(waterSensors).keyBy(WaterSensor::getId);
//        waterSensorStringKeyedStream.sum("vc").print("maxBy");
        /**
         * 结果
         * maxBy:3> WaterSensor(id=sensor_1, ts=1607527992000, vc=20)
         * maxBy:3> WaterSensor(id=sensor_1, ts=1607527992000, vc=70)
         * maxBy:3> WaterSensor(id=sensor_1, ts=1607527992000, vc=100)
         * maxBy:1> WaterSensor(id=sensor_2, ts=1607527993000, vc=10)
         * maxBy:1> WaterSensor(id=sensor_2, ts=1607527993000, vc=40)
         *
         * 注意:
         * 	分组聚合后, 理论上只能取分组字段和聚合结果, 但是Flink允许其他的字段也可以取出来, 其他字段默认情况是取的是这个组内第一个元素的字段值
         */


        //2.3   maxBy
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 30));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));
        KeyedStream<WaterSensor, String> waterSensorStringKeyedStream = env.fromCollection(waterSensors).keyBy(WaterSensor::getId);
        waterSensorStringKeyedStream.maxBy("vc",false).print("maxBy");
        /**
         * 结果
         * maxBy:3> WaterSensor(id=sensor_1, ts=1607527992000, vc=20)
         * maxBy:3> WaterSensor(id=sensor_1, ts=1607527994000, vc=50)
         * maxBy:3> WaterSensor(id=sensor_1, ts=1607527994000, vc=50)
         * maxBy:1> WaterSensor(id=sensor_2, ts=1607527993000, vc=10)
         * maxBy:1> WaterSensor(id=sensor_2, ts=1607527995000, vc=30)
         *
         * 注意:
         * 	maxBy和minBy可以指定当出现相同值的时候,其他字段是否取第一个. true表示取第一个, false表示取最后一个.
         *
         */


        //3.执行
        env.execute();

    }
}
