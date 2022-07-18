package com.ym.stu.state.keyed_state;

import com.ym.stu.bean.WaterSensor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**计算每个传感器的水位和
 * @author yomo
 * @create 2022-05-09 10:54
 */
public class Flink03_State_Keyed_ReducingState {

    public static void main(String[] args) throws Exception {

        // 1.流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env.socketTextStream("hadoop102",7777)
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0],Long.valueOf(split[1]),Integer.valueOf(split[2]));
                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, Integer>() {

                    private ReducingState<Integer> sumVcState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sumVcState = this.getRuntimeContext()
                                .getReducingState(new ReducingStateDescriptor<Integer>("sumVcState",Integer::sum,Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<Integer> out) throws Exception {
                        sumVcState.add(value.getVc());
                        out.collect(sumVcState.get());
                    }
                })
                .print();

        //3.执行
        env.execute();

    }

}
