package com.ym.realtime.app.dws;

import com.ym.realtime.bean.ProvinceStats;
import com.ym.realtime.utils.ClickHouseUtil;
import com.ym.realtime.utils.MyKafkaUtil;
import org.apache.calcite.schema.StreamableTable;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 	定义Table流环境
 * 	把数据源定义为动态表
 * 	通过SQL查询出结果表
 * 	把结果表转换为数据流
 * 	把数据流写入目标数据库
 *
 * @author yomo
 * @create 2021-05-06 7:52
 */
public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境(流、表)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1 设置状态后端
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop002:8020/gmall/dwd_log/ck"));
        //1.2 开启CK
        //env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(60000L);

        // TODO 2.获取表的执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // TODO 3.读取kafka的数据创建动态表
        String sourceTopic = "dwm_order_wide";
        String groupId = "province_stats_app";

        tableEnv.executeSql("create table order_wide (" +
                "province_id bigint," +
                "province_name string," +
                "province_area_code string," +
                "province_iso_code string," +
                "province_3166_2_code string," +
                "order_id string," +
                "split_total_amount double," +
                "create_time string," +
                "rowtime as to_timestamp(create_time,'yyyy-MM-dd HH:mm:ss')," +
                "waterrmark for rowtime as rowtime )"+
                " with (" + MyKafkaUtil.getKafkaDDL(sourceTopic,groupId));
        //测试
        //tableEnv.executeSql("select * from order_wide").print();

        //TODO 4.分组 开窗 聚合
        Table reduceTable = tableEnv.sqlQuery("select" +
                "    DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '2' SECOND),'yyyy-MM-dd HH:mm:ss') as stt," +
                "    DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '2' SECOND),'yyyy-MM-dd HH:mm:ss') as edt," +
                "    province_id," +
                "    province_name," +
                "    province_area_code," +
                "    province_iso_code," +
                "    province_3166_2_code," +
                "    sum(split_total_amount) order_amount," +
                "    count(*) order_count," +
                "    UNIX_TIMESTAMP()*1000 ts " +
                "from ORDER_WIDE " +
                "group by province_id,province_name,province_area_code,province_iso_code,province_3166_2_code,TUMBLE(rowtime, INTERVAL '2' SECOND)");

        // TODO 5.将动态表转换为追加流
        DataStream<ProvinceStats> rowDataStream = tableEnv.toAppendStream(reduceTable, ProvinceStats.class);
        rowDataStream.print();

        /**
         * 在ClickHouse中创建地区主题宽表
         * create table province_stats_200821 (
         *    stt DateTime,
         *    edt DateTime,
         *    province_id  UInt64,
         *    province_name String,
         *    area_code String,
         *    iso_code String,
         *    iso_3166_2 String,
         *    order_amount Decimal64(2),
         *    order_count UInt64,
         *    ts UInt64
         * )engine =ReplacingMergeTree(ts)
         *         partition by  toYYYYMMDD(stt)
         *         order by   (stt,edt,province_id );
         *
         *
         *  测试
         * 	启动ZK、Kafka、ClickHouse、Redis、HDFS、Hbase、Maxwell
         * 	运行BaseDBApp
         * 	运行OrderWideApp
         * 	运行ProvinceStatsSqlApp
         * 	运行rt_dblog目录下的jar包
         * 	查看控制台输出
         * 	查看ClickHouse中products_stats_2021表数据
         */

        // TODO 6.写入clcikhouse
        rowDataStream.addSink(ClickHouseUtil.getSink("insert into province_stats_2021 values(?,?,?,?,?,?,?,?,?,?)"));


        env.execute();

    }
}
