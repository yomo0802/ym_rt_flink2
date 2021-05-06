package com.ym.realtime.app.dws;

import com.ym.realtime.app.funcs.KeyWordUDTF;
import com.ym.realtime.common.GmallConstant;
import com.ym.realtime.utils.ClickHouseUtil;
import com.ym.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 关键词主题
 * * MockLog -> Nginx -> Logger -> Kafka(ods_base_log) -> FlinkApp(BaseLogApp)
 * * -> Kafka(dwd_page_log) -> KeyWordStatsApp -> ClickHouse
 *
 * @author yomo
 * @create 2021-05-06 8:48
 */
public class KeywordStatsApp {

    public static void main(String[] args) throws Exception {
        //TODO 0.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(4);
        /*
        //CK相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop102:8020/gmall/flink/checkpoint/ProvinceStatsSqlApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME","atguigu");
        */
        //TODO 1.定义Table流环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        //TODO 2.读取Kafka主题数据创建动态表  dwd_page_log
        String sourceTopic = "dwd_page_log";
        String groupId = "keyword_stats_app";
        tableEnv.executeSql("CREATE TABLE page_view " +
                "(common MAP<STRING,STRING>, " +
                "page MAP<STRING,STRING>," +
                "ts BIGINT, " +
                "rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')) ," +
                "WATERMARK FOR  rowtime  AS  rowtime - INTERVAL '2' SECOND) " +
                "WITH (" + MyKafkaUtil.getKafkaDDL(sourceTopic, groupId)
        );

        //TODO 3.过滤数据,只需要搜索数据,搜索的关键词不能为空
        Table filterTable = tableEnv.sqlQuery("" +
                "select  " +
                "   page['item'] fullWord ," +
                "   rowtime " +
                "from " +
                "   page_view " +
                "where page['item_type'] = 'keyword' and page['item'] is not null");

        //TODO 4.使用UDTF函数进行切词处理 函数注册
        tableEnv.createTemporarySystemFunction("ik_analyze", KeyWordUDTF.class);
        Table analyzeWordTable = tableEnv.sqlQuery("select " +
                "   keyword " +
                "   rowtime " +
                "from " + filterTable +
                ",lateral table(ik_analyze(funllWord)) as (keyword)");

        //TODO 5.分组 开窗 聚合
        Table resultTable = tableEnv.sqlQuery("select " +
                "   keyword, " +
                "   count(*) ct, '" + GmallConstant.KEYWORD_SEARCH + "' source," +
                "   DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '2' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                "   DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '2' SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
                "   UNIX_TIMESTAMP()*1000 ts from " + analyzeWordTable +
                " GROUP BY " + "TUMBLE(rowtime, INTERVAL '2' SECOND ),keyword");

        //TODO 6.将动态表转换为追加流
        DataStream<KeywordStatsApp> keywordStatsAppDataStream = tableEnv.toAppendStream(resultTable, KeywordStatsApp.class);
        keywordStatsAppDataStream.print();


        /**
         * 在ClickHouse中创建关键词统计表
         * create table keyword_stats_200821 (
         *     stt DateTime,
         *     edt DateTime,
         *     keyword String ,
         *     source String ,
         *     ct UInt64 ,
         *     ts UInt64
         * )engine =ReplacingMergeTree( ts)
         *         partition by  toYYYYMMDD(stt)
         *         order by  ( stt,edt,keyword,source );
         *
         *
         * 测试
         *
         * 	启动ZK、Kafka、logger.sh、ClickHouse
         * 	运行BaseLogApp
         * 	运行KeywordStatsApp
         * 	运行rt_applog目录下的jar包
         * 	查看控制台输出
         * 	查看ClickHouse中keyword_stats_2021表数据
         */

        //TODO 7.将数据写入clickhouse
        keywordStatsAppDataStream.addSink(ClickHouseUtil.getSink("insert into keyword_stats_2021(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?,)"));


        env.execute();

    }

}
