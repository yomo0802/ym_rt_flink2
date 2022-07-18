package com.ym.stu.state.operator_state;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**广播状态
 * @author yomo
 * @create 2022-05-09 10:11
 */
public class Flink02_State_Operator_Broadcast_State {

    public static void main(String[] args) throws Exception {

        // 1.流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //
        DataStreamSource<String> dataStream = env.socketTextStream("hadoop102", 7777);
        DataStreamSource<String> controlStream  = env.socketTextStream("hadoop102", 9999);
        //状态
        MapStateDescriptor<String, String> stateDescriptor = new MapStateDescriptor<>("state", String.class, String.class);
        //广播流
        BroadcastStream<String> broadcastStream = controlStream.broadcast(stateDescriptor);
        dataStream
                .connect(broadcastStream)
                .process(new BroadcastProcessFunction<String, String, String>() {
            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                // 从广播状态中取值, 不同的值做不同的业务
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(stateDescriptor);
                if ("1".equals(broadcastState.get("switch"))) {
                    out.collect("切换到1号配置");
                } else if ("0".equals(broadcastState.get("switch"))) {
                    out.collect("切换到0号配置");
                } else {
                    out.collect("切换到其他配置");
                }
            }

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(stateDescriptor);
                //将获取的数据存放到广播状态
                broadcastState.put("switch",value);
            }
        });

        //3.执行
        env.execute();

    }

}
