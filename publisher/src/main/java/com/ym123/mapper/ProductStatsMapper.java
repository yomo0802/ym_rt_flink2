package com.ym123.mapper;

import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;

/** 数据映射层 编辑SQL查询商品同统计表
 * 商品统计Mapper
 * @author yomo
 * @create 2021-05-06 9:36
 */
public interface ProductStatsMapper {

    //获取商品交易额
    @Select("select sum(order_amount) order_amount from product_stats_2021 where toYYYYYMMDD(stt)=#{date}")
    public BigDecimal getGmv(int date);

}
