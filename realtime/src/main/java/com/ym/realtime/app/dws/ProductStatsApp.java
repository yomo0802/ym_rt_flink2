package com.ym.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ym.realtime.app.funcs.DimAsyncFunction;
import com.ym.realtime.bean.OrderWide;
import com.ym.realtime.bean.PaymentWide;
import com.ym.realtime.bean.ProductStats;
import com.ym.realtime.common.GmallConstant;
import com.ym.realtime.utils.ClickHouseUtil;
import com.ym.realtime.utils.DateTimeUtil;
import com.ym.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/** 商品主题宽表 DateStream
 * Desc: 形成以商品为准的统计  曝光 点击  购物车  下单 支付  退单  评论数 宽表
 * <p>
 * mockLog -> Nginx -> Logger -> Kafka(ods_base_log) -> FlinkApp(BaseLogApp) -> Kafka(dwd_page_log)
 * mockDB -> MySQL -> Maxwell -> Kafka(ods_base_db_m) -> FlinkApp(BaseDBApp) -> Kafka(HBase) ->
 * FlinkApp(OrderWideApp,PaymentWideApp,Redis,Phoenix) -> Kafka(dwm_order_wide,dwm_payment_wide)
 *
 * @author yomo
 * @create 2021-05-05 13:24
 */
public class ProductStatsApp {
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
                "hdfs://hadoop002:8020/gmall/flink/checkpoint/ProductStatsApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME","atguigu");
        */

        //TODO 1.从Kafka中获取数据流
        String groupId = "product_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);

        FlinkKafkaConsumer<String> favorInfoSourceSource = MyKafkaUtil.getKafkaSource(favorInfoSourceTopic, groupId);
        DataStreamSource<String> favorInfoDStream = env.addSource(favorInfoSourceSource);

        FlinkKafkaConsumer<String> cartInfoSource = MyKafkaUtil.getKafkaSource(cartInfoSourceTopic, groupId);
        DataStreamSource<String> cartInfoDStream = env.addSource(cartInfoSource);

        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId);
        DataStreamSource<String> orderWideDStream = env.addSource(orderWideSource);

        FlinkKafkaConsumer<String> paymentWideSource = MyKafkaUtil.getKafkaSource(paymentWideSourceTopic, groupId);
        DataStreamSource<String> paymentWideDStream = env.addSource(paymentWideSource);

        FlinkKafkaConsumer<String> refundInfoSource = MyKafkaUtil.getKafkaSource(refundInfoSourceTopic, groupId);
        DataStreamSource<String> refundInfoDStream = env.addSource(refundInfoSource);

        FlinkKafkaConsumer<String> commentInfoSource = MyKafkaUtil.getKafkaSource(commentInfoSourceTopic, groupId);
        DataStreamSource<String> commentInfoDStream = env.addSource(commentInfoSource);

        //TODO 2 将7个流转换为统一数据格式
        //2.1 处理点击和曝光数据
        SingleOutputStreamOperator<ProductStats> clickAndDisplayDS = pageViewDStream.process(new ProcessFunction<String, ProductStats>() {
            @Override
            public void processElement(String pageLog, Context ctx, Collector<ProductStats> out) throws Exception {
                //转换为JSON对象
                JSONObject jsonObject = JSON.parseObject(pageLog);

                //获取pageId
                String page_id = jsonObject.getJSONObject("page").getString("page_id");
                //获取时间戳字段
                Long ts = jsonObject.getLong("ts");

                //判断good_detail页面,则为点击数据
                if ("good_detail".equals(page_id)) {
                    ProductStats productStats = ProductStats
                            .builder()
                            .sku_id(jsonObject.getJSONObject("page").getLong("item"))
                            .click_ct(1L)
                            .ts(ts)
                            .build();
                    out.collect(productStats);
                }

                //取出曝光数据
                JSONArray displays = jsonObject.getJSONArray("displays");
                if (displays != null && displays.size() > 0) {

                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject displayObj = displays.getJSONObject(i);

                        if ("sku_id".equals(displayObj.getString("item_type"))) {
                            ProductStats productStats = ProductStats.builder()
                                    .sku_id(displayObj.getLong("item"))
                                    .display_ct(1L)
                                    .ts(ts)
                                    .build();
                            out.collect(productStats);
                        }
                    }
                }
            }
        });

        //2.2 处理收藏数据
        SingleOutputStreamOperator<ProductStats> favorStatsDS = favorInfoDStream.map(json -> {
            JSONObject favorInfo = JSON.parseObject(json);
            Long ts = DateTimeUtil.toTs(favorInfo.getString("create_time"));
            return ProductStats.builder()
                    .sku_id(favorInfo.getLong("sku_id"))
                    .favor_ct(1L)
                    .ts(ts)
                    .build();
        });

        //2.3 处理加购数据
        SingleOutputStreamOperator<ProductStats> cartStatsDS = cartInfoDStream.map(
                json -> {
                    JSONObject carInfo = JSON.parseObject(json);
                    Long ts = DateTimeUtil.toTs(carInfo.getString("create_time"));

                    return ProductStats.builder()
                            .sku_id(carInfo.getLong("sku_id"))
                            .cart_ct(1L)
                            .ts(ts)
                            .build();
                });



        //2.4 处理下单数据
        SingleOutputStreamOperator<ProductStats> orderDS = orderWideDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                //转换为OrderWide对象
                OrderWide orderWide = JSON.parseObject(value, OrderWide.class);

                //获取时间戳字段
                Long ts = DateTimeUtil.toTs(orderWide.getCreate_time());

                return ProductStats.builder()
                        .sku_id(orderWide.getSku_id())
                        .order_amount(orderWide.getTotal_amount())
                        .order_sku_num(orderWide.getSku_num())
                        .orderIdSet(new HashSet<>(Collections.singleton(orderWide.getOrder_id())))
                        .ts(ts)
                        .build();
            }
        });

        //2.5 处理支付数据
        SingleOutputStreamOperator<ProductStats> paymentStatsDS = paymentWideDStream.map(
                json -> {
                    PaymentWide paymentWide = JSON.parseObject(json, PaymentWide.class);

                    Long ts = DateTimeUtil.toTs(paymentWide.getPayment_create_time());

                    return ProductStats.builder()
                            .sku_id(paymentWide.getSku_id())
                            .payment_amount(paymentWide.getSplit_total_amount())
                            .paidOrderIdSet(new HashSet<>(Collections.singleton(paymentWide.getOrder_id())))
                            .ts(ts).build();
                });

        //2.6 处理退单数据
        SingleOutputStreamOperator<ProductStats> refundStatsDS = refundInfoDStream.map(
                json -> {
                    JSONObject refundJsonObj = JSON.parseObject(json);
                    Long ts = DateTimeUtil.toTs(refundJsonObj.getString("create_time"));
                    return ProductStats.builder()
                            .sku_id(refundJsonObj.getLong("sku_id"))
                            .refund_amount(refundJsonObj.getBigDecimal("refund_amount"))
                            .refundOrderIdSet(new HashSet<>(Collections.singleton(refundJsonObj.getLong("order_id"))))
                            .ts(ts)
                            .build();
                });

        //2.7 处理评价数据
        SingleOutputStreamOperator<ProductStats> appraiseDS = commentInfoDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {

                //将数据转换为JSON对象
                JSONObject jsonObject = JSON.parseObject(value);

                Long ts = DateTimeUtil.toTs(jsonObject.getString("create_time"));

                //处理好评数
                Long goodCommentCt = 0L;
                if (GmallConstant.APPRAISE_GOOD.equals(jsonObject.getString("appraise"))) {
                    goodCommentCt = 1L;
                }

                return ProductStats.builder()
                        .sku_id(jsonObject.getLong("sku_id"))
                        .comment_ct(1L)
                        .good_comment_ct(goodCommentCt)
                        .ts(ts)
                        .build();
            }
        });

        //3. Union
        DataStream<ProductStats> unionDS = clickAndDisplayDS.union(favorStatsDS, cartStatsDS, orderDS, paymentStatsDS, refundStatsDS, appraiseDS);

        //4.设置Watermark
        SingleOutputStreamOperator<ProductStats> productStatsWithWaterMarkDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(10L)).withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
            @Override
            public long extractTimestamp(ProductStats element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        //5.分组、开窗、聚合
        SingleOutputStreamOperator<ProductStats> reduceDS = productStatsWithWaterMarkDS.keyBy(ProductStats::getSku_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(2)))
                .reduce(new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats t1, ProductStats t2) throws Exception {
                        t1.setDisplay_ct(t1.getDisplay_ct() + t2.getDisplay_ct());
                        t1.setClick_ct(t1.getClick_ct() + t2.getClick_ct());
                        t1.setFavor_ct(t1.getFavor_ct() + t2.getFavor_ct());
                        t1.setOrder_sku_num(t1.getOrder_sku_num() + t2.getOrder_sku_num());
                        t1.setOrder_amount(t1.getOrder_amount().add(t2.getOrder_amount()));

                        t1.getOrderIdSet().addAll(t2.getOrderIdSet());
                        t1.setOrder_ct((long) t1.getOrderIdSet().size());

                        t1.setPayment_amount(t1.getPayment_amount().add(t2.getPayment_amount()));
                        t1.getPaidOrderIdSet().addAll(t2.getPaidOrderIdSet());

                        t1.getRefundOrderIdSet().addAll(t2.getRefundOrderIdSet());

                        t1.setRefund_order_ct((long) t1.getRefundOrderIdSet().size());
                        t1.setRefund_amount(t1.getRefund_amount().add(t2.getRefund_amount()));
                        t1.setPaid_order_ct((long) t1.getPaidOrderIdSet().size());

                        t1.setComment_ct(t1.getComment_ct() + t2.getComment_ct());
                        t1.setGood_comment_ct(t1.getGood_comment_ct() + t2.getGood_comment_ct());

                        return t1;
                    }
                }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void apply(Long aLong, TimeWindow window, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                        long start = window.getStart();
                        long end = window.getEnd();

                        String stt = sdf.format(start);
                        String edt = sdf.format(end);

                        //取出聚合以后的数据
                        ProductStats productStats = input.iterator().next();

                        //设置窗口时间
                        productStats.setStt(stt);
                        productStats.setEdt(edt);

                        out.collect(productStats);

                    }
                });

        //6.关联维度信息
        //6.1 关联SKU信息
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(reduceDS, new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
            @Override
            public String getKey(ProductStats productStats) {
                return productStats.getSku_id().toString();
            }

            @Override
            public void join(ProductStats productStats, JSONObject dimInfo) throws Exception {

                //获取维度中的信息
                String sku_name = dimInfo.getString("SKU_NAME");
                BigDecimal price = dimInfo.getBigDecimal("PRICE");
                Long spu_id = dimInfo.getLong("SPU_ID");
                Long tm_id = dimInfo.getLong("TM_ID");
                Long category3_id = dimInfo.getLong("CATEGORY3_ID");

                //关联SKU维度信息
                productStats.setSku_name(sku_name);
                productStats.setSku_price(price);
                productStats.setSpu_id(spu_id);
                productStats.setTm_id(tm_id);
                productStats.setCategory3_id(category3_id);

            }
        }, 300, TimeUnit.SECONDS);

        //6.2 补充SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS =
                AsyncDataStream.unorderedWait(productStatsWithSkuDS,
                        new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws Exception {
                                productStats.setSpu_name(jsonObject.getString("SPU_NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getSpu_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //6.3 补充品类维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3DS =
                AsyncDataStream.unorderedWait(productStatsWithSpuDS,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws Exception {
                                productStats.setCategory3_name(jsonObject.getString("NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getCategory3_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //6.4 补充品牌维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDS =
                AsyncDataStream.unorderedWait(productStatsWithCategory3DS,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws Exception {
                                productStats.setTm_name(jsonObject.getString("TM_NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getTm_id());
                            }
                        }, 60, TimeUnit.SECONDS);


        //打印测试
        productStatsWithTmDS.print();

        /**
         * 在ClickHouse中创建商品主题宽表
         *
         * create table product_stats_200821 (
         *    stt DateTime,
         *    edt DateTime,
         *    sku_id  UInt64,
         *    sku_name String,
         *    sku_price Decimal64(2),
         *    spu_id UInt64,
         *    spu_name String ,
         *    tm_id UInt64,
         *    tm_name String,
         *    category3_id UInt64,
         *    category3_name String ,
         *    display_ct UInt64,
         *    click_ct UInt64,
         *    favor_ct UInt64,
         *    cart_ct UInt64,
         *    order_sku_num UInt64,
         *    order_amount Decimal64(2),
         *    order_ct UInt64 ,
         *    payment_amount Decimal64(2),
         *    paid_order_ct UInt64,
         *    refund_order_ct UInt64,
         *    refund_amount Decimal64(2),
         *    comment_ct UInt64,
         *    good_comment_ct UInt64 ,
         *    ts UInt64
         * )engine =ReplacingMergeTree( ts)
         *         partition by  toYYYYMMDD(stt)
         *         order by   (stt,edt,sku_id );
         *
         *
         *
         * 	启动ZK、Kafka、logger.sh、ClickHouse、Redis、HDFS、Hbase、Maxwell
         * 	运行BaseLogApp
         * 	运行BaseDBApp
         * 	运行OrderWideApp
         * 	运行PaymentWideApp
         * 	运行ProductsStatsApp
         * 	运行rt_applog目录下的jar包
         * 	运行rt_dblog目录下的jar包
         * 	查看控制台输出
         * 	查看ClickHouse中products_stats_2021表数据
         */

        //8.写入ClickHouse
        productStatsWithTmDS.addSink(ClickHouseUtil.getSink("insert into product_stats_200821 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        //9.执行任务
        env.execute();


    }
}
