package com.ym.realtime.app.funcs;

import com.alibaba.fastjson.JSONObject;

/**
 * @author yomo
 * @create 2021-04-29 23:45
 */
public interface DimJoinFunction<T>  {

    //获取数据中的所要关联维度的主键
    String getKey(T input);

    //关联事实数据和维度数据
    void join(T input , JSONObject dimInfo) throws Exception;

}
