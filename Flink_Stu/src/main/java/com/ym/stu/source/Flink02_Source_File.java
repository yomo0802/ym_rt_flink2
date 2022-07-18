package com.ym.stu.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yomo
 * @create 2022-03-30 12:20
 */
public class Flink02_Source_File {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.从文件获取数据
        env.readTextFile("Flink_Stu/input").print();
        //3.执行
        env.execute();

    }

}
