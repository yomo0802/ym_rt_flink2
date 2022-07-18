package com.ym.project.app;

import com.ym.project.bean.UserBehavior;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**日志数据
 *
 * 	衡量网站流量一个最简单的指标，就是网站的页面浏览量（Page View，PV）。用户每次打开一个页面便记录1次PV，多次打开同一页面则浏览量累计。
 * 	一般来说，PV与来访者的数量成正比，但是PV并不直接决定页面的真实来访者数量，如同一个来访者通过不断的刷新页面，也可以制造出非常高的PV。
 *
 * 网站总浏览量（PV）的统计
 * @author yomo
 * @create 2022-04-06 11:03
 */
public class Flink01_Project_PV_WordCount {

    public static void main(String[] args) throws Exception {

        // 1.流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //2.
        env.readTextFile("Flink_Stu/input/UserBehavior.csv")
                .map(line ->{
            //对数据进行切割,然后封装到POJO中
                    String[] split = line.split(",");
                    return  new UserBehavior(
                            Long.valueOf(split[0]),
                            Long.valueOf(split[1]),
                            Integer.valueOf(split[2]),
                            split[3],
                            Long.valueOf(split[4]));
                })
                //过滤出pv行为
                .filter(behavior -> "pv".equals(behavior.getBehavior()))
                .map(behavior -> Tuple2.of("pv",1L))
                //使用Tuple类型,方便后面求和
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                //按照key分组
                .keyBy(value -> value.f0)
                //求和
                .sum(1)
                .print();

        //3.执行
        env.execute();

    }

}
