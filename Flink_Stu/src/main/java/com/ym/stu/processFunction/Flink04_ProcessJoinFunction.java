package com.ym.stu.processFunction;

import com.ym.stu.bean.WaterSensor;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author yomo
 * @create 2022-04-20 16:55
 */
public class Flink04_ProcessJoinFunction {

    public static void main(String[] args) throws Exception {

        // 1.流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<WaterSensor> s1 = env
                .socketTextStream("hadoop102", 8888)  // 在socket终端只输入毫秒级别的时间戳
                .map(value -> {
                    String[] datas = value.split(",");
                    return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

                });
        SingleOutputStreamOperator<WaterSensor> s2 = env
                .socketTextStream("hadoop102", 9999)  // 在socket终端只输入毫秒级别的时间戳
                .map(value -> {
                    String[] datas = value.split(",");
                    return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

                });

        s1.join(s2)
                .where(WaterSensor::getId)
                .equalTo(WaterSensor::getId)
                // 必须使用窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<WaterSensor, WaterSensor, String>() {
                    @Override
                    public String join(WaterSensor first, WaterSensor second) throws Exception {
                        return "first: " + first + ", second: " + second;
                    }
                })
                .print();

        //3.执行
        env.execute();

    }

}
