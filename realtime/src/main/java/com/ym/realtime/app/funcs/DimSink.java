package com.ym.realtime.app.funcs;

import com.alibaba.fastjson.JSONObject;
import com.ym.realtime.common.GmallConfig;
import com.ym.realtime.utils.DimUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.*;
import java.util.Collection;
import java.util.Set;

/**
 * @author yomo
 * @create 2021-04-29 13:27
 */
public class DimSink extends RichSinkFunction<JSONObject> {

    private Connection connection = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化Phoenix连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //将数据写入Phoenix upsert into t(id,name,sex) values (...)
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;

        try {

            JSONObject data = value.getJSONObject("data");
            Set<String> keys = data.keySet();
            Collection<Object> values = data.values();

            //获取表名
            String tableName = value.getString("sink_table");

            //创建插入数据的sql
            String upsertSQL = genUpsertSql(tableName, keys, values);
            System.out.println(upsertSQL);
            //编译sql
            preparedStatement = connection.prepareStatement(upsertSQL);
            //执行sql
            preparedStatement.executeUpdate();
            //提交
            connection.commit();

            //判断如果是更新操作,则删除redis中的数据保证数据的一致性
            String type = value.getString("type");
            if ("update".equals(type)) {
                DimUtil.deleteCached(tableName, data.getString("id"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new Exception("向Phoenix插入数据失败");
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }

    }

    //upsert into t(id,name,sex) values (' ',' ',' ')
    private String genUpsertSql(String tableName, Set<String> keys, Collection<Object> values) {
        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + tableName + "(" + StringUtils.join(keys, ",") + ")" +
                " values('" + StringUtils.join(values, "','") + "')";
    }
}
