package com.ym.stu.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/** FlatMap
 *  作用  消费一个元素并产生零个或多个元素
 *  返回   DataStream -> DataStream
 * @author yomo
 * @create 2022-03-30 13:21
 */
public class Flink04_TransForm_FlatMap {

    public static void main(String[] args) throws Exception {

        // 1.流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.1 匿名内部类写法
        env.fromElements(1,2,3,4,5).flatMap(new FlatMapFunction<Integer, Integer>() {
            @Override
            public void flatMap(Integer value, Collector<Integer> out) throws Exception {
                out.collect(value * 2);
                out.collect(value * 3);
            }
        }).print("Flink04_TransForm_FlatMap");

        //2.2 Lambda表达式写法
//        env.fromElements(1,2,3,4,5).flatMap((Integer value, Collector<Integer> out) -> {
//            out.collect(value * 2);
//            out.collect(value * 3);
//        }).returns(Types.INT).print();
        /** returns(Types.INT)
         * 说明: 在使用Lambda表达式表达式的时候, 由于泛型擦除的存在,
         * 在运行的时候无法获取泛型的具体类型, 全部当做Object来处理,
         * 及其低效, 所以Flink要求当参数中有泛型的时候, 必须明确指定泛型的类型.
         */
        //3.执行
        env.execute();

    }

}
