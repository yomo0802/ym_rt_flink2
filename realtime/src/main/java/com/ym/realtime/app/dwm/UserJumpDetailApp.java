package com.ym.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ym.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * 跳出就是用户成功访问了网站的一个页面后就退出，不在继续访问网站的其它页面。而跳出率就是用跳出次数除以访问次数。
 * 关注跳出率，可以看出引流过来的访客是否能很快的被吸引，渠道引流过来的用户之间的质量对比，对于应用优化前后跳出率的对比也能看出优化改进的成果。
 *
 * 思路:
 * 首先要识别哪些是跳出行为，要把这些跳出的访客最后一个访问的页面识别出来。那么要抓住几个特征：
 * 	该页面是用户近期访问的第一个页面
 * 这个可以通过该页面是否有上一个页面（last_page_id）来判断，如果这个表示为空，就说明这是这个访客这次访问的第一个页面。
 * 	首次访问之后很长一段时间（自己设定），用户没继续再有其他页面的访问。
 * 这第一个特征的识别很简单，保留last_page_id为空的就可以了。但是第二个访问的判断，其实有点麻烦，首先这不是用一条数据就能得出结论的，
 * 需要组合判断，要用一条存在的数据和不存在的数据进行组合判断。而且要通过一个不存在的数据求得一条存在的数据。
 * 更麻烦的他并不是永远不存在，而是在一定时间范围内不存在。那么如何识别有一定失效的组合行为呢？
 * 最简单的办法就是Flink自带的CEP技术。这个CEP非常适合通过多条数据组合来识别某个事件。
 * 用户跳出事件，本质上就是一个条件事件加一个超时事件的组合。
 *
 * @author yomo
 * @create 2021-04-29 18:20
 */
public class UserJumpDetailApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 1.从kafka的dwd_page_log主题中读取页面日志
        String sourceTopic = "dwd_page_log";
        String groupId = "userJumpDetailApp";
        String sinkTopic = "dwm_user_jump_detail";

        //TODO 从kafka中读取数据
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);


        // TODO 测试1  将上面的kafka 数据源注释掉 为了方便 改为从集合中读取数据
        //  bin/kafka-console-consumer.sh --bootstrap-server hadoop002:9092 --from-beginning --topic dwm_user_jump_detail
        /*DataStream<String> kafkaDS = env
                .fromElements(
                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":1000000001} ",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":1000000000}",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" + "\"home\"},\"ts\":1000000002} ",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                                "\"detail\"},\"ts\":1000000020} "
                );*/

        // TODO 测试2 用端口测试
        //  {"common":{"mid":"101"},"page":{"page_id":"home"},"ts":1000000001}
        //  {"common":{"mid":"102"},"page":{"page_id":"home"},"ts":1000000001}
        //  {"common":{"mid":"102"},"page":{"page_id":"home","last_page_id":"aa"},"ts":1000000020}
        //  {"common":{"mid":"102"},"page":{"page_id":"home","last_page_id":"aa"},"ts":1000000030}
        // DataStreamSource<String> kafkaDS = env.socketTextStream("hadoop002", 7777);


        //TODO 将数据转换为json流
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(jsonObj -> JSON.parseObject(jsonObj));

        //TODO 提取数据中的时间戳生成Watermark
        // 老版本,默认使用的处理时间语义,新版本默认时间语义为事件时间 1.12
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 指定事件时间字段
        SingleOutputStreamOperator<JSONObject> jsonObjWithDStream = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps() //有序数据
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts") ;// *1000L 端口测试用;
                    }
                }));

        //TODO 按mid进行分区
        KeyedStream<JSONObject, String> jsonObjectStringKeyedStream = jsonObjWithDStream
                .keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //TODO 配置CEP表达式
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override //条件1：进入的第一个页面
            public boolean filter(JSONObject jsonObject) throws Exception {
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                System.out.println("进入的第一个页面 " + lastPageId);

                if (lastPageId == null || lastPageId.length() == 0) {
                    return true;
                }
                return false;
            }
        }).followedBy("follow").where(new SimpleCondition<JSONObject>() {
            @Override //条件2 在 10秒内必须有第二个页面
            public boolean filter(JSONObject jsonObject) throws Exception {
                String pageId = jsonObject.getJSONObject("page").getString("page_id");
                System.out.println("进入的第二个页面 " + pageId);
                if (pageId != null && pageId.length() > 0) {
                    return true;
                }
                return false;
            }
        }).within(Time.milliseconds(10000));

        //TODO 根据表达式筛选流
        PatternStream<JSONObject> patternedStream = CEP.pattern(jsonObjectStringKeyedStream, pattern);

        //TODO 提取命中的数据
        /**
         * 	设定超时时间标识 timeoutTag
         * 	flatSelect方法中，实现PatternFlatTimeoutFunction中的timeout方法。
         * 	所有out.collect的数据都被打上了超时标记
         * 	本身的flatSelect方法因为不需要未超时的数据所以不接受数据。
         * 	通过SideOutput侧输出流输出超时数据
         */
        OutputTag<String> timeoutTag = new OutputTag<String>("timeout") {};

        SingleOutputStreamOperator<Object> selectDS = patternedStream.flatSelect(
                timeoutTag,
                new PatternFlatTimeoutFunction<JSONObject, String>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> map, long l, Collector<String> collector) throws Exception {
                        //超时数据
                        collector.collect(map.get("start").get(0).toString());
                    }
                }, new PatternFlatSelectFunction<JSONObject, Object>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> map, Collector<Object> collector) throws Exception {
                        //不写,匹配到的数据 不需要
                    }
                }
        );

        //TODO 将数据写入kafka dwm_user_jump_detail主题
        FlinkKafkaProducer<String> kafkaSink = MyKafkaUtil.getKafkaSink(sinkTopic);
        selectDS.getSideOutput(timeoutTag).addSink(kafkaSink);
        selectDS.getSideOutput(timeoutTag).print(">>>>>>>>>>>>>>>>>");

        env.execute();

    }

}
