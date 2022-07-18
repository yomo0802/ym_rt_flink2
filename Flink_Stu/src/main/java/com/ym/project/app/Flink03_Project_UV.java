package com.ym.project.app;

import com.ym.project.bean.UserBehavior;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**日志数据
 *
 * 我们统计的是所有用户对页面的所有浏览行为，也就是说，同一用户的浏览行为会被重复统计。而在实际应用中，
 * 我们往往还会关注，到底有多少不同的用户访问了网站，所以另外一个统计流量的重要指标是网站的独立访客数（Unique Visitor，UV）
 *
 * 网站独立访客数（UV）的统计
 *
 * @author yomo
 * @create 2022-04-06 12:49
 */
public class Flink03_Project_UV {

    public static void main(String[] args) throws Exception {

        // 1.流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //2.
        env.readTextFile("Flink_Stu/input/UserBehavior.csv")
                .flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] split = line.split(",");
                    UserBehavior userBehavior = new UserBehavior(
                            Long.valueOf(split[0]),
                            Long.valueOf(split[1]),
                            Integer.valueOf(split[2]),
                            split[3],
                            Long.valueOf(split[4]));
                    if ("pv".equals(userBehavior.getBehavior())) {
                        out.collect(Tuple2.of("uv",userBehavior.getUserId()));
                    }
                }).returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy(t -> t.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, Integer>() {
                    HashSet<Long> userIDs = new HashSet<Long>();
                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Integer> out) throws Exception {
                        userIDs.add(value.f1);
                        out.collect(userIDs.size());
                    }
                })
                .print();
        //3.执行
        env.execute();

    }

}
