package com.ym.realtime.bean;

import lombok.Data;

/** JAVABean mysql中表字段
 * @author yomo
 * @create 2021-04-28 14:17
 */
@Data
public class TableProcess {

    //动态分流Sink常量
    public static final String SINK_TYPE_HBASE = "hbase";
    public static final String SINK_TYPE_KAFKA = "kafka";
    public static final String SINK_TYPE_CK= "clickhouse";

    //来源表
    String sourceTable;
    //操作类型 insert update delete
    String operateType;
    //输出类型 hbase kafka
    String sinkType;
    //输出表
    String sinkTable;
    //输出字段
    String sinkColumn;
    //主键字段
    String sinkPk;
    //建表扩展
    String sinkExtend;

}
