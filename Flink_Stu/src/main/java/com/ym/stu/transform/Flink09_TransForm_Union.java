package com.ym.stu.transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Union
 *  作用  对两个或者两个以上的DataStream进行union操作，产生一个包含所有DataStream元素的新DataStream
 * @author yomo
 * @create 2022-03-30 16:52
 */
public class Flink09_TransForm_Union {

    public static void main(String[] args) throws Exception {

        // 1.流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Integer> stream2 = env.fromElements(10, 20, 30, 40, 50);
        DataStreamSource<Integer> stream3 = env.fromElements(100, 200, 300, 400, 500);
        //2.
        stream1.union(stream2).union(stream3).print();
        /**
         * connect与 union 区别：
         * 1.	union之前两个流的类型必须是一样，connect可以不一样
         * 2.	connect只能操作两个流，union可以操作多个。
         */

        //3.执行
        env.execute();

    }

}
