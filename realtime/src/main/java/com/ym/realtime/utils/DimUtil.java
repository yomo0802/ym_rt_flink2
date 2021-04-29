package com.ym.realtime.utils;

import redis.clients.jedis.Jedis;

/**
 * @author yomo
 * @create 2021-04-29 13:45
 */
public class DimUtil {

    //根据key让redis中缓存失效
    public static void deleteCached(String tableName,String id) {
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
}
