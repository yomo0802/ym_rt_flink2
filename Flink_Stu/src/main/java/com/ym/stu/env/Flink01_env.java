package com.ym.stu.env;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yomo
 * @create 2022-03-30 12:09
 */
public class Flink01_env {
    public static void main(String[] args) throws Exception {

        // 批处理环境
        ExecutionEnvironment benv = ExecutionEnvironment.getExecutionEnvironment();
        // 流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //执行
        env.execute();
    }

}
