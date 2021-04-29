package com.ym.realtime.app.dwd;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONArray;
import com.ym.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

/**
 * @author ymstart
 * @create 2021-04-26 21:33
 */
public class LogAppBase {

    //定义用户行为主题信息
    public static final String TOPIC_START = "dwd_start_log";
    public static final String TOPIC_PAGE = "dwd_page_log";
    public static final String Topic_DISPLAY = "dwd_display_log";

    public static void main(String[] args) throws Exception {
        //1.0获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.1设置并行度 一般设置为kafka主题的分区数
        env.setParallelism(1);
        //1.2设置状态后端 测试存在hdfs
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop002:8020//gmall/dwd_log/ck"));
        //1.3开启ck 10秒钟开启一次ck
        //env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(60000L); //超时时间1分钟
        //问题：开启checkpoint 如果有脏数据 任务会一直卡着 查询官网发现 默认重启策略是Integer的最大值 我们手动设置为不重启
        env.setRestartStrategy(RestartStrategies.noRestart());
        //一秒内重启三次
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2,1000L));

        //修改用户名
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //2.读取kafka ods_base_log 主题数据
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource("ods_base_log", "ods_base_log_group");
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //3.将每行数据转化为jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(data -> JSONObject.parseObject(data));

        //4.按mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(data -> data.getJSONObject("common").getString("mid"));

        //5.使用状态做新老用户校验
        SingleOutputStreamOperator<JSONObject> jsonWithFlagDS = keyedStream.map(new NewMidRichMapFunc());

        //打印测试  kafka  zk (hadoop)
        // kafka生产者 bin/kafka-console-producer.sh --broker-list hadoop002:9092 --topic ods_base_log
        //在生产者中输出两次  {"common":{"ar":"110000","ba":"iPhone","ch":"Appstore","is_new":"1","md":"iPhone Xs","mid":"mid_18","os":"iOS 13.3.1","uid":"23","vc":"v2.1.134"},"page":{"during_time":18188,"item":"1","item_type":"sku_ids","last_page_id":"trade","page_id":"payment"},"ts":1618801864000}
        //is_new 会从 1 -> 0
        //jsonWithFlagDS.print();

        //6.分流,使用ProcessFunction将ODS数据分为启动、曝光、页面
        SingleOutputStreamOperator<String> pageDS = jsonWithFlagDS.process(new SplitProcessFunc());

        //7.将三个流分别写入到kafka的不同主题中
        DataStream<String> startDS = pageDS.getSideOutput(new OutputTag<String>("start") {});
        DataStream<String> displayDS = pageDS.getSideOutput(new OutputTag<String>("display") {});

        //打印测试 zk kafka log_mock nginx
        //pageDS.print("page>>>>");
        //startDS.print("start>>>");
        //displayDS.print("display>>>");

        pageDS.addSink(MyKafkaUtil.getKafkaSink(TOPIC_PAGE)); //bin/kafka-console-consumer.sh --bootstrap-server hadoop002:9092 --from-beginning --topic dwd_page_log
        startDS.addSink(MyKafkaUtil.getKafkaSink(TOPIC_START)); //bin/kafka-console-consumer.sh --bootstrap-server hadoop002:9092 --from-beginning --topic dwd_start_log
        displayDS.addSink(MyKafkaUtil.getKafkaSink(Topic_DISPLAY)); //bin/kafka-console-consumer.sh --bootstrap-server hadoop002:9092 --from-beginning --topic dwd_display_log

        //8.执行任务
        env.execute();

    }


    /**
     * 使用状态做新老用户校验
     */
    private static class NewMidRichMapFunc extends RichMapFunction<JSONObject, JSONObject> {

        //声明状态 用于存放当前Mid是否已经访问过
        private ValueState<String> firstVisitDateState;
        private SimpleDateFormat simpleDateFormat;

        @Override
        public void open(Configuration parameters) throws Exception {
            firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("new-mid", String.class));
            simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        }

        @Override
        public JSONObject map(JSONObject jsonObject) throws Exception {

            //取出新用户标记
            String isNew = jsonObject.getJSONObject("common").getString("is_new");

            //如果当前前端传输数据表示新用户,进行校验 可能一些原因导致数据发生变化,需要修改状态 1 表示新用户  0  表示老用户
            if ("1".equals(isNew)) {
                //取出状态数据并取出当前访问时间
                String firstData = firstVisitDateState.value();
                Long ts = jsonObject.getLong("ts");

                //判断状态数据是否为null
                if (firstData != null) {
                    //修复
                    jsonObject.getJSONObject("common").put("is_new", "0");
                } else {
                    //更新状态
                    firstVisitDateState.update(simpleDateFormat.format(ts));
                }
            }

            return jsonObject;
        }
    }

    /**
     * 分流,使用ProcessFunction将ODS数据分为启动、曝光、页面
     */
    private static class SplitProcessFunc extends ProcessFunction<JSONObject, String> {

        @Override
        public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {

            //提取"start"字段
            String startStr = jsonObject.getString("start");

            //判断是否为启动数据
            if (startStr != null && startStr.length() > 0) {
                //将启动日志输出到侧输出流
                context.output(new OutputTag<String>("start") {
                }, jsonObject.toString());
            } else {

                //为页面数据,将数据输出到主流
                collector.collect(jsonObject.toString());

                //不是启动数据,继续判断是否是曝光数据
                JSONArray displays = jsonObject.getJSONArray("displays");
                if (displays != null && displays.size() > 0) {
                    //为曝光数据,遍历写入侧输出流
                    for (int i = 0; i < displays.size(); i++) {
                        //取出单条曝光数据
                        JSONObject displayJson = displays.getJSONObject(i);
                        //添加页面ID
                        displayJson.put("page_id",
                                jsonObject.getJSONObject("page").getString("page_id"));
                        //输出到侧输出流
                        context.output(new OutputTag<String>("display") {
                        }, displayJson.toString());
                    }
                }

            }

        }
    }
}
