package com.ym.stu.source;

import com.ym.stu.bean.WaterSensor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author yomo
 * @create 2022-03-30 12:43
 */
public class Flink04_Source_Custom {

    public static void main(String[] args) throws Exception {

        //1.流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.自定义source
        /**测试数据
         * sensor_1,1607527992000,20
         * sensor_1,1607527993000,40
         * sensor_1,1607527994000,50
         */
        env.addSource(new MySource("hadoop102", 7777)).print("Flink04_Source_Custom" );

        //3.执行
        env.execute();
    }

    private static class MySource implements SourceFunction<WaterSensor> {

        private String host;
        private int port;
        private volatile boolean isRunning = true;
        private Socket socket;

        public MySource(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            //实现从一个socket读取数据
            socket = new Socket(host, port);
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            String line = null;
            while (isRunning && (line = reader.readLine()) != null) {
                String[] split = line.split("," );
                ctx.collect(new WaterSensor(split[0],Long.valueOf(split[1]),Integer.valueOf(split[2])));
            }
        }

        /**
         * 大多数的source在run方法内部都会有一个while循环,
         * 当调用这个方法的时候, 应该可以让run方法中的while循环结束
         */
        @Override
        public void cancel() {
            isRunning = false;
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
