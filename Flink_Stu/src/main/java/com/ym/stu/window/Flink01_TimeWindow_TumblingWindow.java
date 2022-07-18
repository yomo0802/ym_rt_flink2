package com.ym.stu.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


import java.util.Arrays;

/**时间窗口
 *
 * 滚动窗口(Tumbling Windows)
 * 	滚动窗口有固定的大小, 窗口与窗口之间不会重叠也没有缝隙.比如,如果指定一个长度为5分钟的滚动窗口, 当前窗口开始计算, 每5分钟启动一个新的窗口.
 * 	滚动窗口能将数据流切分成不重叠的窗口，每一个事件只能属于一个窗口。
 *
 * @author yomo
 * @create 2022-04-06 14:49
 */
public class Flink01_TimeWindow_TumblingWindow {

    public static void main(String[] args) throws Exception {

        // 1.流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream("hadoop102",7777)
                .flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        /**
                         * 正则表达式
                         * \W+：匹配一个或多个非字母进行切割，匹配到的非字母不缓存；
                         * (\W+)：匹配一个或多个非字母进行切割，匹配到的非字母全部缓存；
                         * (\W)+：匹配一个或多个非字母进行切割，匹配到的非字母缓存最后一位；
                         * [\W]+：匹配一个或多个非字母进行切割，匹配到的非字母不缓存；(跟\W+一样)
                         * [\W+]：匹配一个非字母进行切割，匹配到的非字母不缓存(加号只是相当于一个符号，但与\W含义重叠，无效)。
                         * 原文链接：https://blog.csdn.net/weixin_44356698/article/details/108082449
                         */
                        Arrays.stream(value.split("\\w+"))
                                .forEach(word->out.collect(Tuple2.of(word,1L)));
                    }
                })
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(8)))
                .sum(1)
                .print();

        //3.执行
        env.execute();

    }

}
