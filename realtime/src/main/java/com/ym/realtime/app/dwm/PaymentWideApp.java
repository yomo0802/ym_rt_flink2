package com.ym.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.ym.realtime.bean.OrderWide;
import com.ym.realtime.bean.PaymentInfo;
import com.ym.realtime.bean.PaymentWide;
import com.ym.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * @author yomo
 * @create 2021-04-30 14:28
 */
public class PaymentWideApp {

    public static void main(String[] args) throws Exception {

        //TODO 0.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
         /*
        //设置CK相关配置
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend("hdfs://hadoop:8020/gmall/flink/checkpoint/OrderWideApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        */

        //TODO 1.接收数据流
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";

        //TODO 2.读取kafka数据
        DataStreamSource<String> paymentInfoSource = env.addSource(MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId));
        DataStreamSource<String> orderWideSource = env.addSource(MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId));

        //TODO 3.对数据进行结构转换 将每行数据转换为javaBean 并提取时间戳生成WaterMark
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentInfoSource.map(jsonStr -> JSON.parseObject(jsonStr, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @Override
                            public long extractTimestamp(PaymentInfo paymentInfo, long l) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                                try {
                                    return sdf.parse(paymentInfo.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    throw new RuntimeException("时间格式错误");
                                }
                            }
                        }));

        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideSource.map(jsonStr -> JSON.parseObject(jsonStr, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide orderWide, long l) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                                try {
                                    return sdf.parse(orderWide.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    throw new RuntimeException("时间格式错误");
                                }
                            }
                        }));

        //TODO 4.按OrderId分组
        KeyedStream<PaymentInfo, Long> paymentInfoLongKeyedStream = paymentInfoDS.keyBy(PaymentInfo::getOrder_id);
        KeyedStream<OrderWide, Long> orderWideLongKeyedStream = orderWideDS.keyBy(OrderWide::getOrder_id);

        //TODO 5.双流join
        SingleOutputStreamOperator<PaymentWide> paymentWide = paymentInfoLongKeyedStream.intervalJoin(orderWideLongKeyedStream)
                .between(Time.minutes(-15), Time.seconds(0))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context context, Collector<PaymentWide> collector) throws Exception {
                        collector.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });
        //测试
        //paymentWide.print("paymentWide>>>>");

        //TODO 6.将数据写入kafka  dwm_payment_wide
        paymentWide.map(paymentWide1 -> JSON.toJSONString(paymentWide)).addSink(MyKafkaUtil.getKafkaSink(paymentInfoSourceTopic));

        env.execute();

    }
}
