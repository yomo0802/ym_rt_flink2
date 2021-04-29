package com.ym.realtime.app.dwd;

import com.alibaba.fastjson.JSONObject;
import com.ym.realtime.app.funcs.DbSplitProcessFunction;
import com.ym.realtime.app.funcs.DimSink;
import com.ym.realtime.bean.TableProcess;
import com.ym.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @author yomo
 * @create 2021-04-28 13:55
 */
public class DBAppBase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        //读取kafka数据
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource("ods_base_db_m", "ods_base_db_m_groupId");
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //将每行数据转换为json对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(data -> JSONObject.parseObject(data));

        //过滤出data字段数据 以及过滤出data数据不完整的数据
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                //获取data字段
                String data = jsonObject.getString("data");
                return data != null && data.length() > 0;
            }
        });

        //测试打印 zk kafka db_mock
        //filterDS.print();

        //分流 ProcessFunction 维度数据放到HBase 中 事实数据放到kafka Dw层
        //还可以动态的添加表 或字段
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE) {
        };
        SingleOutputStreamOperator<JSONObject> kafkaJsonDS = filterDS.process(new DbSplitProcessFunction(hbaseTag));
        DataStream<JSONObject> hbaseJsonDS = kafkaJsonDS.getSideOutput(hbaseTag);

        //测试 zk kafka db_mock(在mysql中手动添加数据) HBase Phoenix HDFS maxwell
        //hbaseJsonDS.print("HBASE>>>>>");
        //kafkaJsonDS.print("KAFKA>>>>>");

        //取出分流数据 写入HBase还是Kafka
        hbaseJsonDS.addSink(new DimSink());
        FlinkKafkaProducer<JSONObject> kafkaSinkSchema = MyKafkaUtil.getKafkaSinkSchema(new KafkaSerializationSchema<JSONObject>() {

            @Override
            public void open(SerializationSchema.InitializationContext context) throws Exception {
                System.out.println("开始序列化kafka数据！");
            }

            //从每条数据得到该条数据应送往的主题名
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                return new ProducerRecord<byte[], byte[]>(jsonObject.getString("sink_table"), jsonObject.getString("data").getBytes());
            }
        });
        //打印测试
        hbaseJsonDS.print("hbase>>>>");
        kafkaJsonDS.print("kafka>>>>");
        kafkaJsonDS.addSink(kafkaSinkSchema);

        env.execute();
    }
}
