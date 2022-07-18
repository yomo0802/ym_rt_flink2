package com.ym.stu.watermark;

import com.ym.stu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**自定义WatermarkStrategy
 * 间歇性
 * @author yomo
 * @create 2022-04-20 16:03
 */
public class Flink03_Watermark_Punctuated {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<WaterSensor> stream = env
                .socketTextStream("hadoop102", 7777)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
                    }
                });
        //自定义创建WatermarkStrategy
        WatermarkStrategy<WaterSensor> myWaterSensorWatermarkStrategy = new WatermarkStrategy<WaterSensor>() {
            @Override
            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                System.out.println("createWatermarkGenerator...");
                return new MyPunctuated(3);
            }
        }.withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                System.out.println("recordTimestamp:" + recordTimestamp);
                return element.getTs() * 1000;
            }
        });

    }

    private static class MyPunctuated implements WatermarkGenerator<WaterSensor> {

        private long maxTs = Long.MIN_VALUE;
        //允许的最大延迟时间 ms
        private final long maxDelay;

        public MyPunctuated(long maxDelay) {
            this.maxDelay = maxDelay * 1000;
            this.maxTs = Long.MIN_VALUE + this.maxDelay +1;
        }

        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            System.out.println("onEvent..." + eventTimestamp);
            //有了新的元素找到最大的时间戳
            maxTs = Math.max(maxTs, eventTimestamp);
            output.emitWatermark(new Watermark(maxTs - maxDelay - 1));

        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 不需要实现
        }
    }
}
