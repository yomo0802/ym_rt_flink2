package com.ym123.service.impl;

import com.ym123.mapper.ProductStatsMapper;
import com.ym123.service.ProductStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

/** 商品统计接口实现类
 * @author yomo
 * @create 2021-05-06 9:42
 */
@Service
public class ProductStatsServiceImpl implements ProductStatsService {

    @Autowired
    ProductStatsMapper productStatsMapper;

    @Override
    public BigDecimal getGmv(int date) {
        return productStatsMapper.getGmv(date);
    }
}
