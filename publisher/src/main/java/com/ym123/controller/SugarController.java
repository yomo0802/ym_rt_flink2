package com.ym123.controller;

import com.ym123.mapper.ProductStatsMapper;
import com.ym123.service.ProductStatsService;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.Date;

/**
 * 查询接口以及返回参数处理
 *
 * 接收用户请求，并做出相应。根据sugar不同的组件，返回不同的格式
 *
 * @author yomo
 * @create 2021-05-06 9:44
 */

@RestController
@RequestMapping("/api/sugar")
public class SugarController {

    @Autowired
    ProductStatsService productStatsService;

    @RequestMapping("/gmv")
    public String getGmv(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = now();
        }

        BigDecimal gmv = productStatsService.getGmv(date);
        String json = "{    \"status\":0,   \"data\": "+ gmv + "}";
        return json;
    }

    private int now() {

        String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return Integer.valueOf(yyyyMMdd);
    }

}
