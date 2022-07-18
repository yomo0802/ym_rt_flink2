package com.ym.stu.transform;

import com.ym.stu.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/** Reduce
 *  作用  一个分组数据流的聚合操作，合并当前的元素和上次聚合的结果，产生一个新的值，
 *          返回的流中包含每一次聚合的结果，而不是只返回最后一次聚合的最终结果。
 * 	       为什么还要把中间值也保存下来? 考虑流式数据的特点: 没有终点, 也就没有最终的概念了. 任何一个中间的聚合结果都是值!
 * 	返回   KeyedStream -> SingleOutputStreamOperator
 * @author yomo
 * @create 2022-03-30 17:17
 */
public class Flink11_TransForm_Reduce {

    public static void main(String[] args) throws Exception {

        // 1.流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

        //2.1 匿名内部类写法
        env.fromCollection(waterSensors).keyBy(WaterSensor::getId).reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                System.out.println("reducer function...");
                return new WaterSensor(value1.getId(),value1.getTs(),value1.getVc()+value2.getVc());
            }
        }).print("reduce");

        //2.2Lambda表达式写法
//        env.fromCollection(waterSensors).keyBy(WaterSensor::getId).reduce((value1, value2) -> {
//            System.out.println("reducer function...");
//            return new WaterSensor(value1.getId(),value1.getTs(),value1.getVc()+value2.getVc());
//        }).print("reduce");
        /**
         * 注意:
         * 1.	聚合后结果的类型, 必须和原来流中元素的类型保持一致!
         */

        //3.执行
        env.execute();

    }

}
