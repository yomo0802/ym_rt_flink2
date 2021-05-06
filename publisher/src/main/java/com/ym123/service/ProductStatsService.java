package com.ym123.service;

import java.math.BigDecimal;

/** 查询统计数据层
 * 商品统计接口
 * @author yomo
 * @create 2021-05-06 9:40
 */
public interface ProductStatsService {

    //获取某一天的总交易额
    public BigDecimal getGmv(int date);

}
