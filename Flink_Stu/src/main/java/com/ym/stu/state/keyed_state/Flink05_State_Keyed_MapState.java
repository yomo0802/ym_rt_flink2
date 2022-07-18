package com.ym.stu.state.keyed_state;

import com.ym.stu.bean.WaterSensor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**去重: 去掉重复的水位值. 思路: 把水位值作为MapState的key来实现去重, value随意
 * @author yomo
 * @create 2022-05-09 11:13
 */
public class Flink05_State_Keyed_MapState {

    public static void main(String[] args) throws Exception {

        // 1.流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.
        env.socketTextStream("hadoop102", 7777)
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    private MapState<Integer, String> mapState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        mapState = this
                                .getRuntimeContext()
                                .getMapState(new MapStateDescriptor<Integer, String>("mapState", Integer.class, String.class));
                    }
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                        if (!mapState.contains(value.getVc())) {
                            out.collect(value);
                            mapState.put(value.getVc(), "随意");
                        }
                    }
                })
                .print();

        //3.执行
        env.execute();

    }

}
