package com.ym.stu.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Map
 * 作用   将数据流中的数据进行转换, 形成新的数据流，消费一个元素并产出一个元素
 * 返回   DataStream -> DataStream
 * @author yomo
 * @create 2022-03-30 13:10
 */
public class Flink03_TransForm_Map_RichMapFunction {
    public static void main(String[] args) throws Exception {

        // 1.流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.
        env.fromElements(1,2,3,4,5).map(new MyRichMapFunction()).setParallelism(2).print("Flink03_TransForm_Map_RichMapFunction");

        //3.执行
        env.execute();

    }

    private static class MyRichMapFunction extends RichMapFunction<Integer,Integer>{
        // 默认生命周期方法, 初始化方法, 在每个并行度上只会被调用一次
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open 方法执行一次");
        }
        // 默认生命周期方法, 最后一个方法, 做一些清理工作, 在每个并行度上只调用一次
        @Override
        public void close() throws Exception {
            System.out.println("close 方法执行一次");
        }

        @Override
        public Integer map(Integer value) throws Exception {
            System.out.println("map 一个元素执行一次");
            return value * 2;
        }
    }
}
