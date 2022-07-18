package com.ym.stu.transform;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Shuffle
 *  作用  把流中的元素随机打乱. 对同一个组数据, 每次只需得到的结果都不同.
 *  返回  DataStream -> DataStream
 * @author yomo
 * @create 2022-03-30 13:45
 */
public class Flink07_TransForm_Shuffle {

    public static void main(String[] args) throws Exception {

        // 1.流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.
        env.fromElements(10, 3, 5, 9, 20, 8).shuffle().print();


        //3.执行
        env.execute();

    }

}
