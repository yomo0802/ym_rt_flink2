package com.ym.stu.sideoutput;

import com.ym.stu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author yomo
 * @create 2022-04-20 16:13
 */
public class Flink01_SideOutput {

    public static void main(String[] args) throws Exception {

        // 1.流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        System.out.println(env.getConfig());

        //2.
        SingleOutputStreamOperator<WaterSensor> stream = env
                .socketTextStream("hadoop102", 7777)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
                    }
                });

        //创建watermark
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))//最大容忍延迟
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {// 指定时间戳
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000;
                    }
                });
        //将watermark 作用于流上
        SingleOutputStreamOperator<String> result = stream
                .assignTimestampsAndWatermarks(waterSensorWatermarkStrategy)
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(3))
                //设置侧输出流
                .sideOutputLateData(new OutputTag<WaterSensor>("side_1") {
                })
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        String msg = "当前key:" + key + "窗口:[" + context.window().getStart() / 1000 + "," + context.window().getEnd() / 1000 + "] 一共有:" + elements.spliterator().estimateSize()
                                + "条数据";
                        out.collect(context.window().toString());
                        out.collect(msg);
                    }
                });
        //输出主流
        result.print("主流");
        //输出侧输出流
        result.getSideOutput(new OutputTag<WaterSensor>("side_1"){}).print("侧输出流");

        //3.执行
        env.execute();

    }

}
