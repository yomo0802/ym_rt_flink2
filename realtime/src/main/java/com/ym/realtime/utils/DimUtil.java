package com.ym.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 *
 *  <p>
 *  Redis:
 *  1.存什么数据？         维度数据   JsonStr
 *  2.用什么类型？         String  Set  Hash
 *  3.RedisKey的设计？     String：tableName+id  Set:tableName  Hash:tableName
 *  t:19:zhangsan
 *  <p>
 *  集合方式排除,原因在于我们需要对每条独立的维度数据设置过期时间
 *
 * @author yomo
 * @create 2021-04-29 13:45
 */
public class DimUtil {

    //根据key让redis中缓存失效
    public static void deleteCached(String tableName, String id) {
        String key = tableName.toUpperCase() + ":" + id;

        Jedis jedis = null;
        try {
            jedis = RedisUtil.getJedis();
            //通过key清除缓存
            jedis.del(key);
            jedis.close();
        } catch (Exception e) {
            System.out.println("缓存异常");
            e.printStackTrace();
        }
    }

    public static JSONObject getDimInfo(String tableName, Tuple2<String, String>... columnValues) {

        if (columnValues.length <= 0) {
            throw new RuntimeException("查询维度数据时,请至少设置一个查询条件");
        }

        //创建 Phoenix Where 子句
        StringBuilder whereSQL = new StringBuilder(" where ");

        //创建Redis key
        StringBuilder redisKey = new StringBuilder(tableName).append(":");

        //遍历查询条件并赋值where sql
        for (int i = 0; i < columnValues.length; i++) {
            //获取单个查询条件
            Tuple2<String, String> columnValue = columnValues[i];

            String column = columnValue.f0;
            String value = columnValue.f1;
            //where column = 'value'
            whereSQL.append(column).append("='").append(value).append("'");

            redisKey.append(value);

            //判断如果不是最后一个条件,则添加 and
            if (i < columnValues.length - 1) {
                //where column1 = 'value1' and column2 = 'value2'
                whereSQL.append(" and ");
                redisKey.append(":");
            }
        }
        //获取Redis连接
        Jedis jedis = RedisUtil.getJedis();
        String dimJsonStr = jedis.get(redisKey.toString());

        //判断是否从redis中查询到数据
        if (dimJsonStr != null && dimJsonStr.length() > 0) {
            jedis.close();
            return JSON.parseObject(dimJsonStr);
        }

        //拼接sql
        String querySql = "select * from " + tableName + whereSQL.toString();
        System.out.println(querySql);

        //查询Phoenix中的维度数据
        List<JSONObject> queryList = PhoenixUtil.queryList(querySql, JSONObject.class);
        JSONObject dimJsonObj = queryList.get(0);

        //将数据写入Redis
        jedis.set(redisKey.toString(), dimJsonObj.toString());
        jedis.expire(redisKey.toString(), 24 * 60 * 60); //数据存放1天
        jedis.close();

        return dimJsonObj;

    }

    public static JSONObject getDimInfo(String tableName, String value) {
        return getDimInfo(tableName, new Tuple2<>("id", value));
    }


    public static void main(String[] args) {
        System.out.println(getDimInfo("DIM_BASE_TRADEMARK", "19"));
    }
}
