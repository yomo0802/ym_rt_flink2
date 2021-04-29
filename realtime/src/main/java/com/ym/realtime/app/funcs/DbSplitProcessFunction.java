package com.ym.realtime.app.funcs;

import com.alibaba.fastjson.JSONObject;
import com.ym.realtime.bean.TableProcess;
import com.ym.realtime.common.GmallConfig;
import com.ym.realtime.utils.MySQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * @author yomo
 * @create 2021-04-28 14:31
 */
public class DbSplitProcessFunction extends ProcessFunction<JSONObject, JSONObject> {

    //定义属性
    private OutputTag<JSONObject> outputTag;

    //构造方法
    public DbSplitProcessFunction(OutputTag<JSONObject> outputTag) {
        this.outputTag = outputTag;
    }

    //定义配置信息的Map
    private HashMap<String, TableProcess> tableProcessHashMap;

    //定义Set用于记录当前Phoenix中已存在的表
    private HashSet<String> existTables;

    //定义Phoenix连接
    private Connection connection = null;

    //生命周期方法
    @Override
    public void open(Configuration parameters) throws Exception {

        //初始化配置信息
        tableProcessHashMap = new HashMap<>();

        //初始化Phoenix中已存在的表
        existTables = new HashSet<>();

        //初始化Phoenix连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection.prepareStatement(GmallConfig.PHOENIX_SERVER);

        //周期读取配置信息
        refreshMeta();

        //开启定时任务调度,周期性执行读取配置信息方法
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                refreshMeta();
            }
        }, 1000L, 5000L);


    }

    /**
     * 周期性读取配置信息方法
     * 1. 读取MySQL中的配置信息
     * 2. 将查询结果封装为Map,以便后续每条数据获取
     * 3. 检查Phoenix中是否存在该表,如果不存在,则在Phoenix中建表
     */
    private void refreshMeta() {

        System.out.println("开始读取MySQL配置信息");

        //1. 读取MySQL中的配置信息
        List<TableProcess> tableProcesses = MySQLUtil.queryList("select * from table_process", TableProcess.class, true);

        //2. 将查询结果封装为Map,以便后续每条数据获取
        for (TableProcess tableProcess : tableProcesses) {

            //获取表源
            String sourceTable = tableProcess.getSourceTable();

            //获取操作类型
            String operateType = tableProcess.getOperateType();
            //联合主键
            String key = sourceTable + ":" + operateType;

            tableProcessHashMap.put(key, tableProcess);

            //3. 检查Phoenix中是否存在该表,如果不存在,则在Phoenix中建表
            if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                //校验Hbase中是否存在该表 能添加进去说明表不存在
                boolean notExist = existTables.add(tableProcess.getSinkTable());

                if (notExist) {
                    checkTable(tableProcess.getSinkTable(),
                            tableProcess.getSinkColumn(),
                            tableProcess.getSinkPk(),
                            tableProcess.getSinkExtend());
                }
            }
        }
        //校验
        if (tableProcessHashMap == null || tableProcessHashMap.size() == 0) {
            throw new RuntimeException("读取MySql配置信息失败");
        }

    }

    /**
     * Phoenix建表
     *
     * @param sinkTable  表名
     * @param sinkColumn 字段
     * @param sinkPk     主键
     * @param sinkExtend 扩展字段
     *                   create table if not exist 库名.表名(id varchar primary key , name varchar,sex varchar) ....
     */
    private void checkTable(String sinkTable, String sinkColumn, String sinkPk, String sinkExtend) {

        if (sinkPk == null) {
            sinkPk = "id";
        }

        if (sinkExtend == null) {
            sinkExtend = null;
        }

        //封装建表sql
        StringBuilder createSQL = new StringBuilder("create table if not exist ").append(GmallConfig.HBASE_SCHEMA).append(".").append(sinkTable).append("(");
        //遍历添加字段信息
        String[] fields = sinkColumn.split(",");
        for (int i = 0; i < fields.length; i++) {
            //取出字段
            String field = fields[i];

            //判断字段是否为主键
            if (sinkPk.equals(field)) {
                createSQL.append(field).append(" varchar primary key ");
            } else {
                createSQL.append(field).append(" varchar ");
            }

            //如果当前字段不是最后一个字段,则追加,
            if (i < fields.length - 1) {
                createSQL.append(",");
            }
        }
        createSQL.append(")");
        createSQL.append(sinkExtend);

        System.out.println(createSQL);

        //执行建表sql
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(createSQL.toString());
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("创建Phoenix表 " + sinkTable + " 失败");
        }

    }

    //核心处理方法，根据MySQL配置表的信息为每条数据打标签，走kafka还是hbase
    @Override
    public void processElement(JSONObject jsonObject, Context context, Collector<JSONObject> collector) throws Exception {

        //取出数据中的表名和操作类型
        String table = jsonObject.getString("table");
        String type = jsonObject.getString("type");

        //使用MaxWell是 数据操作类型为"bootstrap-insert" 为方便后期处理 -> insert
        if ("bootstrap-insert".equals(type)) {
            type = "insert";
            jsonObject.put("type", type);
        }

        //拼接key
        String key = table + ":" + type;

        //获取对应的tableProcess数据
        TableProcess tableProcess = tableProcessHashMap.get(key);

        //判断当前配置信息是否存在
        if (tableProcess != null) {
            //向数据中追加sink_table信息
            jsonObject.put("sink_table", tableProcess.getSinkTable());

            //根据配置信息中提供的字段做数据过滤
            filterColumn(jsonObject.getJSONObject("data"), tableProcess.getSinkColumn());

            //判断写入kafka 还是HBase
            if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                collector.collect(jsonObject);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                context.output(outputTag, jsonObject);
            }
        } else {
            System.out.println(" NO KEY" + key + " IN MYSQL");
        }

    }

    //校验字段，过滤掉多余的字段
    private void filterColumn(JSONObject data, String sinkColumn) {

        //保留的数据字段
        String[] fields = sinkColumn.split(",");
        List<String> fieldList = Arrays.asList(fields);

        Set<Map.Entry<String, Object>> entries = data.entrySet();
        //entries.removeIf(next ->!fieldList.contains(next.getKey()));
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();

        while (iterator.hasNext()) {
            Map.Entry<String, Object> next = iterator.next();
            if (!fieldList.contains(next.getKey())) {
                iterator.remove();
            }
        }

    }
}
