package com.ym.project.app;

import com.ym.project.bean.OrderEvent;
import com.ym.project.bean.TxEvent;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**订单支付实时监控
 *
 * 	在电商网站中，订单的支付作为直接与营销收入挂钩的一环，在业务流程中非常重要。
 * 	对于订单而言，为了正确控制业务流程，也为了增加用户的支付意愿，网站一般会设置一个支付失效时间，超过一段时间不支付的订单就会被取消。
 * 	另外，对于订单的支付，我们还应保证用户支付的正确性，这可以通过第三方支付平台的交易数据来做一个实时对账。
 *  需求: 来自两条流的订单交易匹配
 *  对于订单支付事件，用户支付完成其实并不算完，我们还得确认平台账户上是否到账了。而往往这会来自不同的日志信息，
 *  以我们要同时读入两条流的数据来做合并处理。
 *
 * @author yomo
 * @create 2022-04-06 14:08
 */
public class Flink07_Project_Order {

    public static void main(String[] args) throws Exception {

        // 1.流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(2);

        //2.1 读取order流
        SingleOutputStreamOperator<OrderEvent> orderEventDS = env.readTextFile("Flink_Stu/input/OrderLog.csv")
                .map(line -> {
                    String[] split = line.split(",");
                    return new OrderEvent(
                            Long.valueOf(split[0]),
                            split[1],
                            split[2],
                            Long.valueOf(split[3]));
                });

        //2.2 读取交易流
        SingleOutputStreamOperator<TxEvent> txDS = env.readTextFile("Flink_Stu/input/ReceiptLog.csv")
                .map(line -> {
                    String[] split = line.split(",");
                    return new TxEvent(
                            split[0],
                            split[1],
                            Long.valueOf(split[2]));
                });
        //3. 将两个流join
        ConnectedStreams<OrderEvent, TxEvent> connect = orderEventDS.connect(txDS);

        //4.因为不同的数据流到达的先后顺序不一致，所以需要匹配对账信息.  输出表示对账成功与否
        connect.keyBy("txId","txId")
                .process(new CoProcessFunction<OrderEvent, TxEvent, String>() {

                    //存txId -> OrderEvent
                    Map<String,OrderEvent> orderEventMap = new HashMap<>();
                    //存储txId -> txEvent
                    Map<String,TxEvent> txEventMap = new HashMap<>();

                    @Override
                    public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                        //获取交易信息
                        if (txEventMap.containsKey(value.getTxId())) {
                            out.collect("订单:"+ value +" 对账成功");
                            txEventMap.remove(value.getTxId());
                        } else {
                            orderEventMap.put(value.getTxId(),value);
                        }
                    }

                    @Override
                    public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
                        //获取订单信息
                        if (orderEventMap.containsKey(value.getTxId())) {
                            OrderEvent orderEvent = orderEventMap.get(value.getTxId());
                            out.collect("订单:" + value +" 对账成功");
                            orderEventMap.remove(value.getTxId());
                        } else {
                            txEventMap.put(value.getTxId(),value);
                        }
                    }
                }).print();

        //5.执行
        env.execute();

    }

}
