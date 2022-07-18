package com.ym.project.app;

import com.ym.project.bean.MarketingUserBehavior;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**APP市场推广统计 - 分渠道
 *
 * 随着智能手机的普及，在如今的电商网站中已经有越来越多的用户来自移动端，相比起传统浏览器的登录方式，
 * 手机APP成为了更多用户访问电商网站的首选。对于电商企业来说，一般会通过各种不同的渠道对自己的APP进行市场推广，
 * 而这些渠道的统计数据（比如，不同网站上广告链接的点击量、APP下载量）就成了市场营销的重要商业指标。
 *
 * @author yomo
 * @create 2022-04-06 13:14
 */
public class Flink04_Project_AppAnalysis_By_Chanel {

    public static void main(String[] args) throws Exception {

        // 1.流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //2.
        env.addSource(new AppMarketingUserBehavior())
                .map(behavior -> Tuple2.of(behavior.getChannel() + "_" + behavior.getBehavior(),1L))
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy(t -> t.f0)
                .sum(1)
                .print();

        //3.执行
        env.execute();

    }

    private static class AppMarketingUserBehavior extends RichSourceFunction<MarketingUserBehavior> {

        boolean canRun = true;
        Random random = new Random();

        List<String> channels = Arrays.asList("huawwei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
        List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {

            while (canRun) {
                MarketingUserBehavior marketingUserBehavior = new MarketingUserBehavior(
                        (long) random.nextInt(1000000),
                        behaviors.get(random.nextInt(behaviors.size())),
                        channels.get(random.nextInt(channels.size())),
                        System.currentTimeMillis()
                );
                ctx.collect(marketingUserBehavior);
                Thread.sleep(2000);
            }
        }

        @Override
        public void cancel() {
            canRun = false;
        }
    }
}
