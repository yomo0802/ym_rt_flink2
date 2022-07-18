package com.ym.stu.transform;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Connect
 *  作用  在某些情况下，我们需要将两个不同来源的数据流进行连接，实现数据匹配，
 *        比如订单支付和第三方交易信息，这两个信息的数据就来自于不同数据源，
 *        连接后，将订单支付和第三方交易信息进行对账，此时，才能算真正的支付完成。
 * 	      Flink中的connect算子可以连接两个保持他们类型的数据流，两个数据流被connect之后，
 * 	      只是被放在了一个同一个流中，内部依然保持各自的数据和形式不发生任何变化，两个流相互独立。
 *
 *  返回  DataStream[A], DataStream[B] -> ConnectedStreams[A,B]
 * @author yomo
 * @create 2022-03-30 13:48
 */
public class Flink08_TransForm_Connect {

    public static void main(String[] args) throws Exception {

        // 1.流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<String> stringDataStreamSource = env.fromElements("a", "b", "c" );
        //2.将两个流连接在一起
        ConnectedStreams<Integer, String> connect = integerDataStreamSource.connect(stringDataStreamSource);
        connect.getFirstInput().print("first");
        connect.getSecondInput().print("second");
        /**
         * 注意:
         * 1.	两个流中存储的数据类型可以不同
         * 2.	只是机械的合并在一起, 内部仍然是分离的2个流
         * 3.	只能2个流进行connect, 不能有第3个参与
         */

        //3.执行
        env.execute();

    }

}
