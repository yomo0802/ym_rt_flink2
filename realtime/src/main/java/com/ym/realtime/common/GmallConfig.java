package com.ym.realtime.common;

/**
 * @author yomo
 * @create 2021-04-28 14:45
 */
public class GmallConfig {

    //Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL2021_REALTIME";

    //Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    //Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop002,hadoop003,hadoop004:2181";

    //ClickHouse驱动
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

    //ClickHouse连接地址
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop002:8123/default";
}
