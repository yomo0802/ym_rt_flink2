package com.ym.stu.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**	AggregateFunction(增量聚合函数)
 * @author yomo
 * @create 2022-04-06 15:32
 */
public class Flink08_AggregateFunction {

    public static void main(String[] args) throws Exception {

        // 1.流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.
        env.socketTextStream("hadoop102", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        Arrays.stream(value.split("\\W+")).forEach(word -> out.collect(Tuple2.of(word, 1L)));
                    }
                })
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<Tuple2<String, Long>, Long, Long>() {
                    // 创建累加器: 初始化中间值
                    @Override
                    public Long createAccumulator() {
                        System.out.println("createAccumulator");
                        return 0L;
                    }

                    //累加操作
                    @Override
                    public Long add(Tuple2<String, Long> value, Long accumulator) {
                        System.out.println("add");
                        return value.f1+accumulator;
                    }

                    //获取结果
                    @Override
                    public Long getResult(Long accumulator) {
                        System.out.println("getResult");
                        return accumulator;
                    }

                    //累加器的合并,只有会话窗口会调用
                    @Override
                    public Long merge(Long a, Long b) {
                        System.out.println("meger");
                        return a+b;
                    }
                })
                .print();


        //3.执行
        env.execute();

    }

}
