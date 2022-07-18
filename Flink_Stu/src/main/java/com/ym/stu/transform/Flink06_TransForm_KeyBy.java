package com.ym.stu.transform;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** KeyBy
 *  作用  把流中的数据分到不同的分区中.具有相同key的元素会分到同一个分区中.一个分区中可以有多重不同的key.
 * 	      在内部是使用的hash分区来实现的.
 * 	参数  Key选择器函数: interface KeySelector<IN, KEY>
 * 	     注意: 什么值不可以作为KeySelector的Key:
 * 	 没有覆写hashCode方法的POJO, 而是依赖Object的hashCode. 因为这样分组没有任何的意义: 每个元素都会得到一个独立无二的组.
 *       实际情况是:可以运行, 但是分的组没有意义.
 * 	任何类型的数组
 * 	返回  DataStream → KeyedStream
 * @author yomo
 * @create 2022-03-30 13:37
 */
public class Flink06_TransForm_KeyBy {

    public static void main(String[] args) throws Exception {

        // 1.流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.1 匿名内部类写法
        env.fromElements(10, 3, 5, 9, 20, 8).keyBy(new KeySelector<Integer, String>() {
            @Override
            public String getKey(Integer value) throws Exception {
                return value % 2 == 0 ? "偶数":"奇数";
            }
        }).print("Flink06_TransForm_KeyBy");

        //2.2 Lambda表达式写法
        //env.fromElements(10, 3, 5, 9, 20, 8).keyBy(value -> value % 2 == 0 ? "偶数":"奇数").print("Flink06_TransForm_KeyBy");

        //3.执行
        env.execute();

    }

}
