package com.ym.stu.watermark;

import com.ym.stu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**自定义WatermarkStrategy
 * 周期型
 * @author yomo
 * @create 2022-04-19 14:44
 */
public class Flink02_Watermark_Period {

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
                return new MyPeriod(3);
            }
        }.withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                System.out.println("recordTimestamp:" + recordTimestamp);
                return element.getTs() * 1000;
            }
        });


    }

    private static class MyPeriod implements WatermarkGenerator<WaterSensor> {

        private long maxTs = Long.MIN_VALUE;
        //允许的最大延迟时间 ms
        private final long maxDelay;

        public MyPeriod(long maxDelay) {
            this.maxDelay = maxDelay * 1000;
            this.maxTs = Long.MIN_VALUE + this.maxDelay +1;
        }
        // 每收到一个元素, 执行一次. 用来生产WaterMark中的时间戳
        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            System.out.println("onEvent" + eventTimestamp);
            //有了新元素 找到最大的时间戳
            maxTs = Math.max(maxTs,eventTimestamp);
            System.out.println(maxTs);
        }
        // 周期性的把WaterMark发射出去, 默认周期是200ms
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            //            System.out.println("onPeriodicEmit...");
            // 周期性的发射水印: 相当于Flink把自己的时钟调慢了一个最大延迟
            output.emitWatermark(new Watermark(maxTs - maxDelay -1));
        }
    }
}
