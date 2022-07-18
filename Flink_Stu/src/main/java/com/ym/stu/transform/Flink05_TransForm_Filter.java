package com.ym.stu.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Filter
 *  作用  根据指定的规则将满足条件（true）的数据保留，不满足条件(false)的数据丢弃
 *  返回  DataStream -> DataStream
 * @author yomo
 * @create 2022-03-30 13:31
 */
public class Flink05_TransForm_Filter {

    public static void main(String[] args) throws Exception {

        // 1.流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.1 匿名内部类写法
//        env.fromElements(10,2,5,6,7).filter(new FilterFunction<Integer>() {
//            @Override
//            public boolean filter(Integer value) throws Exception {
//                return value % 2 == 0;
//            }
//        }).print("Flink05_TransForm_Filter");

        //2.2 Lambda表达式写法
        env.fromElements(10,2,5,6,7).filter(value -> value % 2 == 0).print("Flink05_TransForm_Filter");

        //3.执行
        env.execute();

    }

}
