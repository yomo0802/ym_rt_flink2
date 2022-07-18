package com.ym.stu.timer;

import com.ym.stu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**需求：监控水位传感器的水位值，如果水位值在五分钟之内(processing time)连续上升，则报警。
 * @author yomo
 * @create 2022-04-20 17:37
 */
public class Flink03_Practice {

    public static void main(String[] args) throws Exception {

        // 1.流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<WaterSensor> stream = env
                .socketTextStream("hadoop102", 9999)  // 在socket终端只输入秒级别的时间戳
                .map(value -> {
                    String[] datas = value.split(",");
                    return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                });

        WatermarkStrategy<WaterSensor> wms = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTs() * 1000);

        stream
                .assignTimestampsAndWatermarks(wms)
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    int lastVc = 0;
                    long timeTs = Long.MIN_VALUE;

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        if (value.getVc() > lastVc) {
                            if (timeTs == Long.MIN_VALUE) {
                                System.out.println("注册...");
                                timeTs = ctx.timestamp() + 5000L;
                                ctx.timerService().registerEventTimeTimer(timeTs);
                            }
                        } else {
                            ctx.timerService().deleteEventTimeTimer(timeTs);
                            timeTs = Long.MIN_VALUE;
                        }
                        lastVc = value.getVc();
                        System.out.println(lastVc);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + "报警！！！");
                        timeTs = Long.MIN_VALUE;
                    }
                })
                .print();

        //3.执行
        env.execute();

    }

}
