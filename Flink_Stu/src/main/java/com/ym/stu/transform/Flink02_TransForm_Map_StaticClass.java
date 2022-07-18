package com.ym.stu.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yomo
 * @create 2022-03-30 13:06
 */
public class Flink02_TransForm_Map_StaticClass {

    public static void main(String[] args) throws Exception {

        // 1.流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.静态内部类
        env.fromElements(1,2,3,4,5).map(new MyMapFunction()).print();

        //3.执行
        env.execute();

    }

    private static class MyMapFunction implements MapFunction<Integer,Integer> {
        @Override
        public Integer map(Integer value) throws Exception {
            return value * 2;
        }
    }
}
