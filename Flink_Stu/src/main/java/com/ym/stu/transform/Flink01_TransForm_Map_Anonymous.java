package com.ym.stu.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yomo
 * @create 2022-03-30 12:58
 */
public class Flink01_TransForm_Map_Anonymous {

    public static void main(String[] args) throws Exception {

        // 1.流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.1 方式一:匿名内部类对象
        env.fromElements(1,2,3,4,5).map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value*2;
            }
        }).print("Flink01_TransForm_Map_Anonymous");

        //2.2 方式二 :Lambda表达式
        //env.fromElements(1,2,3,4,5).map(ele -> ele *2).print();


        //3.执行
        env.execute();

    }
}
