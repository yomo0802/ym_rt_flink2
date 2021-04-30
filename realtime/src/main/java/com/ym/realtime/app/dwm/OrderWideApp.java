package com.ym.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ym.realtime.app.funcs.DimAsyncFunction;
import com.ym.realtime.bean.OrderDetail;
import com.ym.realtime.bean.OrderInfo;
import com.ym.realtime.bean.OrderWide;
import com.ym.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
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
import java.util.concurrent.TimeUnit;

/**
 * 宽表层
 *
 * @author yomo
 * @create 2021-04-29 22:47
 */
public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 1.从Kafka的dwd层接收订单和订单明细数据
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";

        //TODO 2.读取kafka数据
        FlinkKafkaConsumer<String> kafkaSourceOrderInfo = MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> kafkaSourceOrderDetail = MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId);
        DataStreamSource<String> orderInfoDS = env.addSource(kafkaSourceOrderInfo);
        DataStreamSource<String> orderDetailDS = env.addSource(kafkaSourceOrderDetail);

        //TODO 3.将每行数据转换为javaBean,提取时间戳生成WaterMark
        WatermarkStrategy<OrderInfo> orderInfoWatermarkStrategy = WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo orderInfo, long l) {
                        return orderInfo.getCreate_ts();
                    }
                });

        WatermarkStrategy<OrderDetail> orderDetailWatermarkStrategy = WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail orderDetail, long l) {
                        return orderDetail.getCreate_ts();
                    }
                });
        //订单流
        KeyedStream<OrderInfo, Long> orderInfoLongKeyedStream = orderInfoDS.map(jsonStr -> {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            //将JSON字符串转换为javaBean
            OrderInfo orderInfo = JSON.parseObject(jsonStr, OrderInfo.class);
            //取出创建时间字段
            String create_time = orderInfo.getCreate_time();
            //按空格分割
            String[] createTimeArr = create_time.split(" ");

            orderInfo.setCreate_date(createTimeArr[0]); //yyyy-MM-dd
            orderInfo.setCreate_hour(createTimeArr[1]); //HH:mm:ss
            orderInfo.setCreate_ts(sdf.parse(create_time).getTime());
            return orderInfo;
        }).assignTimestampsAndWatermarks(orderInfoWatermarkStrategy).keyBy(OrderInfo::getId);

        //订单详情流
        KeyedStream<OrderDetail, Long> orderDetailLongKeyedStream = orderDetailDS.map(jsonStr -> {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            OrderDetail orderDetail = JSON.parseObject(jsonStr, OrderDetail.class);
            orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
            return orderDetail;
        }).assignTimestampsAndWatermarks(orderDetailWatermarkStrategy).keyBy(OrderDetail::getOrder_id);

        //TODO 4.双流join intervalJoin
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoLongKeyedStream.intervalJoin(orderDetailLongKeyedStream)
                .between(Time.seconds(-5), Time.seconds(5)) //生产环境,为了不丢数据,设置时间为最大网络延迟
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context context, Collector<OrderWide> collector) throws Exception {
                        collector.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

        //测试打印  db_mock zk kafka hdfs hbase Maxwell DBAppBase OrderWideApp
        // 业务数据生成->Maxwell同步->Kafka的ods_base_db_m主题->BaseDBApp分流写回kafka->dwd_order_info和dwd_order_detail->OrderWideApp从kafka的dwd层读数据，打印输出
        //orderWideDS.print("orderWide>>>>>");

        //TODO 5.维表关联
        //TODO 5.1用户维度关联
        SingleOutputStreamOperator<OrderWide> orderWideUserDS = AsyncDataStream.unorderedWait(orderWideDS, new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {

            @Override
            public String getKey(OrderWide orderWide) {
                return orderWide.getUser_id().toString();
            }

            @Override
            public void join(OrderWide orderWide, JSONObject dimInfo) throws Exception {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

                //取出用户维度中的生日
                String birthday = dimInfo.getString("BIRTHDAY");
                long currentTS = System.currentTimeMillis();
                long ts = sdf.parse(birthday).getTime();

                //将生日字段处理成年纪
                Long ageLong = (currentTS - ts) / 1000L / 60 / 60 / 24 / 365;
                orderWide.setUser_age(ageLong.intValue());

                //取出用户维度中的性别
                String gender = dimInfo.getString("GENDER");
                orderWide.setUser_gender(gender);
            }
        }, 60, TimeUnit.SECONDS);

        //TODO 测试 db_mock maxwell ZK、kafka 、HDFS、HBase、Redis(bin/redis-server ./redis.conf bin/redis-cli -h hadoop002)
        // db_mock -> mysql(binlog) -> Maxwell->、Kafka(ods_base_db_m)-> DBAppBase(Hbase)  -> kafka(dwd_order_detail)->OrderWideApp
        /*初始化用户维度数据到Hbase（通过Maxwell的Bootstrap）
        bin/maxwell-bootstrap --user maxwell  --password 123456 --host hadoop002  --database gmall2021 --table user_info --client_id maxwell_1
        */
        //orderWideUserDS.print("dim join user >>>");

        // TODO 5.2 关联省市维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideUserDS, new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {

            @Override
            public String getKey(OrderWide orderWide) {
                return String.valueOf(orderWide.getProvince_id());
            }

            @Override
            public void join(OrderWide orderWide, JSONObject dimInfo) throws Exception {
                orderWide.setProvince_name(dimInfo.getString("NAME"));
                orderWide.setProvince_3166_2_code(dimInfo.getString("ISO_3166_2"));
                orderWide.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
                orderWide.setProvince_area_code(dimInfo.getString("AREA_CODE"));
            }
        }, 60, TimeUnit.SECONDS);
        //测试同上
        /*初始化省份维度数据到Hbase（通过Maxwell的Bootstrap）
        bin/maxwell-bootstrap --user maxwell  --password 123456 --host hadoop002  --database gmall2021 --table base_province --client_id maxwell_1
        */

        // TODO 5.3关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSKUDS = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                        orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                        orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSku_id());
                    }
                }, 60, TimeUnit.SECONDS);
        //测试同上
        /*初始化SKU维度数据到Hbase（通过Maxwell的Bootstrap）
        bin/maxwell-bootstrap --user maxwell  --password 123456 --host hadoop002  --database gmall2021 --table sku_info --client_id maxwell_1
        */

        // TODO 5.4关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
                orderWideWithSKUDS, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }
                }, 60, TimeUnit.SECONDS);
        //测试同上
        /*初始化SPU维度数据到Hbase（通过Maxwell的Bootstrap）
        bin/maxwell-bootstrap --user maxwell  --password 123456 --host hadoop002  --database gmall2021 --table spu_info --client_id maxwell_1
        */

        // TODO 5.5关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);
        //测试同上
        /*初始化品类维度数据到Hbase（通过Maxwell的Bootstrap）
        bin/maxwell-bootstrap --user maxwell  --password 123456 --host hadoop002  --database gmall2021 --table base_Category3 --client_id maxwell_1
        */

        // TODO 5.6关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithCategory3DS, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);
        //测试同上
	    /*初始化品牌维度数据到Hbase（通过Maxwell的Bootstrap）
        bin/maxwell-bootstrap --user maxwell  --password 000000 --host hadoop102  --database gmall2021 --table base_trademark --client_id maxwell_1*/

	    // TODO 6 将订单数据和订单明细join后 以及维表关联的宽表写到 kafka的 dwm层
        orderWideWithTmDS.map(orderWide -> JSON.toJSONString(orderWide)).addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic)); //dwm_order_wide


        env.execute();


    }
}
