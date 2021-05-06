package com.ym.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ym.realtime.bean.VisitorStats;
import com.ym.realtime.utils.ClickHouseUtil;
import com.ym.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

/**  访问主题宽表 DateStream
 * * Mock -> Nginx -> Logger ->
 * * Kafka(ods_base_log) ->
 * * FlinkApp(LogBaseApp) ->
 * * Kafka(dwd_page_log dwd_start_log dwd_display_log) ->
 * * FlinkApp(UvApp UserJumpApp) ->
 * * Kafka(dwm_unique_visit dwm_user_jump_detail) ->
 * * VisitorStatsApp -> ClickHouse
 *
 * @author yomo
 * @create 2021-05-02 13:48
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {

        //TODO 0.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(4);
        /*
        //检查点CK相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop102:8020/gmall/flink/checkpoint/VisitorStatsApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME","atguigu");
        */
        String groupId = "visitor_stats_app";

        //TODO 1.从Kafka的pv、uv、跳转明细主题中获取数据
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";

        //从kafka中读取数据
        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> uniqueVisitSource = MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId);
        FlinkKafkaConsumer<String> userJumpSource = MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId);

        //将数据写入流中
        DataStreamSource<String> pageViewDS = env.addSource(pageViewSource);
        DataStreamSource<String> uniqueVisitDS = env.addSource(uniqueVisitSource);
        DataStreamSource<String> userJumpDS = env.addSource(userJumpSource);

        //测试
        //pageViewDS.print("pv>>>>");
        //uniqueVisitDS.print("uv>>>>");
        //userJumpDS.print("uj>>>>");

        //合并数据流

        // TODO 2 对读取的流进行结果转换 格式化流数据,使其字段统一  (JavaBean)
        //2.1 转换pv流
        SingleOutputStreamOperator<VisitorStats> pageViewStatsDS = pageViewDS.map(json -> {

            JSONObject jsonObject = JSON.parseObject(json);
            // 合并数据流的核心算子是union。但是union算子，要求所有的数据流结构必须一致。所以union前要调整数据结构。
            return new VisitorStats("", "",
                    jsonObject.getJSONObject("common").getString("vc"),
                    jsonObject.getJSONObject("common").getString("ch"),
                    jsonObject.getJSONObject("common").getString("ar"),
                    jsonObject.getJSONObject("common").getString("is_new"),
                    0L, 1L, 0L, 0L,
                    jsonObject.getJSONObject("page").getLong("during_time"),
                    jsonObject.getLong("ts"));

        });

        //2.2转换uv流
        SingleOutputStreamOperator<VisitorStats> uniqueVisitStatsDS = uniqueVisitDS.map(
                json -> {
                    JSONObject jsonObj = JSON.parseObject(json);
                    return new VisitorStats("", "",
                            jsonObj.getJSONObject("common").getString("vc"),
                            jsonObj.getJSONObject("common").getString("ch"),
                            jsonObj.getJSONObject("common").getString("ar"),
                            jsonObj.getJSONObject("common").getString("is_new"),
                            1L, 0L, 0L, 0L, 0L, jsonObj.getLong("ts"));
                });

        //2.3 转换sv流
        SingleOutputStreamOperator<VisitorStats> sessionVisitDS = pageViewDS.process(
                new ProcessFunction<String, VisitorStats>() {
                    @Override
                    public void processElement(String json, Context ctx, Collector<VisitorStats> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(json);
                        // 获取上一条数据
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        if (lastPageId == null || lastPageId.length() <= 0) {
                            //    System.out.println("sc:"+json);
                            VisitorStats visitorStats = new VisitorStats("", "",
                                    jsonObj.getJSONObject("common").getString("vc"),
                                    jsonObj.getJSONObject("common").getString("ch"),
                                    jsonObj.getJSONObject("common").getString("ar"),
                                    jsonObj.getJSONObject("common").getString("is_new"),
                                    0L, 0L, 1L, 0L, 0L,
                                    jsonObj.getLong("ts"));
                            out.collect(visitorStats);
                        }
                    }
                });

        //2.4 转换跳转流
        SingleOutputStreamOperator<VisitorStats> userJumpStatDS = userJumpDS.map(json -> {
            JSONObject jsonObj = JSON.parseObject(json);
            return new VisitorStats("", "",
                    jsonObj.getJSONObject("common").getString("vc"),
                    jsonObj.getJSONObject("common").getString("ch"),
                    jsonObj.getJSONObject("common").getString("ar"),
                    jsonObj.getJSONObject("common").getString("is_new"),
                    0L, 0L, 0L, 1L, 0L, jsonObj.getLong("ts"));
        });

        //TODO 4 将4个流合并起来
        DataStream<VisitorStats> unionDetailDS = uniqueVisitStatsDS.union(pageViewStatsDS, sessionVisitDS, userJumpStatDS);

        //TODO 5 分组聚合计算
        SingleOutputStreamOperator<VisitorStats> visitorStatsSingleOutputStreamOperator = unionDetailDS.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(10L))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats visitorStats, long l) {
                        return visitorStats.getTs();
                    }
                }));
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedDS = visitorStatsSingleOutputStreamOperator.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
                return Tuple4.of(visitorStats.getVc(), visitorStats.getCh(), visitorStats.getAr(), visitorStats.getIs_new());
            }
        });

        // TODO 开窗
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowDS = keyedDS.window(TumblingEventTimeWindows.of(Time.seconds(2)));

        //TODO 聚合操作
        SingleOutputStreamOperator<VisitorStats> result = windowDS.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats t1, VisitorStats t2) throws Exception {
                return new VisitorStats("", "",
                        t1.getVc(),
                        t1.getCh(),
                        t1.getAr(),
                        t1.getIs_new(),
                        t1.getUv_ct() + t2.getUv_ct(),
                        t1.getPv_ct() + t2.getPv_ct(),
                        t1.getSv_ct() + t2.getSv_ct(),
                        t1.getUj_ct() + t2.getUj_ct(),
                        t1.getDur_sum() + t2.getDur_sum(),
                        System.currentTimeMillis());
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {

            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow timeWindow, Iterable<VisitorStats> iterable, Collector<VisitorStats> collector) throws Exception {

                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

                //取出数据
                VisitorStats visitorStats = iterable.iterator().next();

                //取出窗口的开始以及结束时间
                long start = timeWindow.getStart();
                long end = timeWindow.getEnd();
                String stt = sdf.format(start);
                String edt = sdf.format(end);

                //设置时间数据
                visitorStats.setStt(stt);
                visitorStats.setEdt(edt);

                //将数据写出
                collector.collect(visitorStats);

            }
        });
        //测试
        /**ClickHouse 数据准备
         *
         * create table  visitor_stats_2021 (
         *         stt DateTime,
         *         edt DateTime,
         *         vc  String,
         *         ch  String,
         *         ar  String,
         *         is_new String,
         *         uv_ct UInt64,
         *         pv_ct UInt64,
         *         sv_ct UInt64,
         *         uj_ct UInt64,
         *         dur_sum  UInt64,
         *         ts UInt64
         *         ) engine =ReplacingMergeTree(ts)
         *         partition by toYYYYMMDD(stt)
         *         order by  (stt,edt,is_new,vc,ch,ar);
         *
         * 	启动ZK、Kafka、logger.sh、ClickHouse、【HDFS】
         * 	运行BaseLogApp
         * 	运行UniqueVisitApp
         * 	运行UserJumpDetailApp
         * 	运行VisitorStatsApp
         * 	运行rt_applog目录下的jar包
         * 	查看控制台输出
         * 	查看ClickHouse中visitor_stats_2021表数据
         */
        //result.print("result>>>>");

        //写入OLAP数据库  将集合后的数据写入Clickhouse
        /**
         * 为何要写入ClickHouse数据库，
         * ClickHouse数据库作为专门解决大量数据统计分析的数据库，在保证了海量数据存储的能力，
         * 同时又兼顾了响应速度。而且还支持标准SQL，即灵活又易上手。
         */
        result.addSink(ClickHouseUtil.getSink("insert into visitor_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?)"));


        env.execute();


    }
}
