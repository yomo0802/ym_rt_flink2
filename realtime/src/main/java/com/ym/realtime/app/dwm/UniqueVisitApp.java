package com.ym.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ym.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;

/**
 * 日活统计 访问页面 日活+1
 *
 * @author yomo
 * @create 2021-04-29 15:47
 */
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(60000);
        //StateBackend fsStateBackend = new FsStateBackend("hdfs://hadoop102:8020/gmall/flink/checkpoint/UniqueVisitApp");
        //env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        String groupId = "unique_visit_app";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";
        //读取Kafka数据
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);
        //对读取的数据进行格式转换
        SingleOutputStreamOperator<JSONObject> kafkaJsonDS = kafkaDS.map(kafkaJson -> JSON.parseObject(kafkaJson));

        //测试 zk kafka logger.sh LogAPPBase UniqueVisitApp log_mock
        //kafkaJsonDS.print("uv");

        //按mid进行分组
        KeyedStream<JSONObject, String> keyedByMidDS = kafkaJsonDS.keyBy(jsonObbj -> jsonObbj.getJSONObject("common").getString("mid"));
        //过滤掉不是今天第一次访问的用户
        SingleOutputStreamOperator<JSONObject> filterDS = keyedByMidDS.filter(new UvFilterFunction());

        //数据流写入到DWM层Kafka dwm_unique_visit主题中
        SingleOutputStreamOperator<String> jsonDWMDS = filterDS.map(jsonObj -> jsonObj.toJSONString());
        //测试  zk kafka logger.sh log_mock LogAppBase UniqueVisitApp
        // bin/kafka-console-consumer.sh --bootstrap-server hadoop002:9092 --from-beginning --topic dwm_unique_visit
        jsonDWMDS.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));


        env.execute();

    }

    private static class UvFilterFunction extends RichFilterFunction<JSONObject> {

        //声明状态
        private ValueState<String> lastVisitState;
        private SimpleDateFormat simpleDateFormat;

        @Override
        public void open(Configuration parameters) throws Exception {
            simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<>("visit-state", String.class);

            //创建状态TTL配置项
            StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build();

            stringValueStateDescriptor.enableTimeToLive(stateTtlConfig);

            lastVisitState = getRuntimeContext().getState(stringValueStateDescriptor);
        }

        ////首先检查当前页面是否有上页标识，如果有说明该次访问一定不是当日首次
        @Override
        public boolean filter(JSONObject jsonObject) throws Exception {

            //取出上次访问页面
            String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
            //不是首次登录
            if (lastPageId != null && lastPageId.length() > 0) {
                return false;
            }
            Long ts = jsonObject.getLong("ts");
            String logDate = simpleDateFormat.format(ts);
            String lastVisitDate = lastVisitState.value();

            //访问时间不为空 且 访问时间=当前时间 已经访问过
            if (lastVisitDate != null && lastVisitDate.length() > 0 && logDate.equals(lastVisitDate)) {
                System.out.println(lastVisitDate + "已访问 " + "现在时间 " + logDate);
            } else {
                //第一次访问
                System.out.println(lastVisitDate + "未访问" +  "现在时间" + logDate);
                lastVisitState.update(logDate);
                return true;
            }
            return false;
        }

    }
}
