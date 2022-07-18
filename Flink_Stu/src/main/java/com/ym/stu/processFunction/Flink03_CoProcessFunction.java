package com.ym.stu.processFunction;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yomo
 * @create 2022-04-20 16:52
 */
public class Flink03_CoProcessFunction {

    public static void main(String[] args) throws Exception {

        // 1.流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //3.执行
        env.execute();

    }

}
