package com.ym.stu.processFunction;

import com.ym.stu.bean.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author yomo
 * @create 2022-04-20 16:46
 */
public class Flink02_KeyedProcessFunction {

    public static void main(String[] args) throws Exception {

        // 1.流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] datas = line.split(",");
                    return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                })
                .keyBy(ws -> ws.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        System.out.println(ctx.getCurrentKey());
                        out.collect(value.toString());
                    }
                }).print();

        //3.执行
        env.execute();

    }

}
