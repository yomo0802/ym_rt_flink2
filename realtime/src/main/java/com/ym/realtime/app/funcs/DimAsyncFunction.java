package com.ym.realtime.app.funcs;

import com.alibaba.fastjson.JSONObject;
import com.ym.realtime.utils.DimUtil;
import com.ym.realtime.bean.OrderWide;
import com.ym.realtime.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author yomo
 * @create 2021-04-29 23:44
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {

    //声明线程池对象
    private ThreadPoolExecutor threadPoolExecutor;

    //声明属性
    private String tableName;

    //构造方法
    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化线程池
        threadPoolExecutor = ThreadPoolUtil.getInstance();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) {
        threadPoolExecutor.submit(new Runnable() {
            @Override
            public void run() {

                //获取查询条件 从流对象中获取主键
                String key = getKey(input);

                //1.查询维度信息 根据主键获取维度对象数据
                JSONObject dimInfo = DimUtil.getDimInfo(tableName, key);

                //2.关联到事实数据上
                if (dimInfo != null && dimInfo.size() > 0) {
                    try {
                        //维度数据和流数据关联
                        join(input, dimInfo);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                //3.继续向下游传输
                resultFuture.complete(Collections.singleton(input));

            }
        });
    }
}
