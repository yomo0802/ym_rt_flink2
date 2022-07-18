package com.ym.project.app;

import com.ym.project.bean.AdsClickLog;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.api.common.typeinfo.Types.*;

/**各省份页面广告点击量实时统计
 *
 * 电商网站的市场营销商业指标中，除了自身的APP推广，还会考虑到页面上的广告投放（包括自己经营的产品和其它网站的广告）。
 * 所以广告相关的统计分析，也是市场营销的重要指标。
 * 对于广告的统计，最简单也最重要的就是页面广告的点击量，网站往往需要根据广告点击量来制定定价策略和调整推广方式，
 * 而且也可以借此收集用户的偏好信息。更加具体的应用是，我们可以根据用户的地理位置进行划分，
 * 从而总结出不同省份用户对不同广告的偏好，这样更有助于广告的精准投放。
 *
 * @author yomo
 * @create 2022-04-06 13:47
 */
public class Flink06_Project_Ads_Click {

    public static void main(String[] args) throws Exception {

        // 1.流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //2.
        env.readTextFile("Flink_Stu/input/AdClickLog.csv")
                .map(line -> {
                    String[] split = line.split(",");
                    return new AdsClickLog(
                            Long.valueOf(split[0]),
                            Long.valueOf(split[1]),
                            split[2],
                            split[3],
                            Long.valueOf(split[4]));
                })
                .map(log -> Tuple2.of(Tuple2.of(log.getProvince(),log.getAdId()),1L))
                .returns(TUPLE(TUPLE(STRING,LONG),LONG))
                .keyBy(new KeySelector<Tuple2<Tuple2<String, Long>, Long>, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> getKey(Tuple2<Tuple2<String, Long>, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                .sum(1)
                .print();

        //3.执行
        env.execute();

    }

}
