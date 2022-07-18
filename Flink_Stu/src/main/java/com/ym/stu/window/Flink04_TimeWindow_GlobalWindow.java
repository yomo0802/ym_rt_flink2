package com.ym.stu.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**时间窗口
 * 全局窗口(Global Windows)
 * 	全局窗口分配器会分配相同key的所有元素进入同一个 Global window. 这种窗口机制只有指定自定义的触发器时才有用.
 * 	否则, 不会做任务计算, 因为这种窗口没有能够处理聚集在一起元素的结束点.
 * @author yomo
 * @create 2022-04-06 15:11
 */
public class Flink04_TimeWindow_GlobalWindow {

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
                .window(GlobalWindows.create())
                .sum(1)
                .print();

        //3.执行
        env.execute();

    }

}
