/**
 * Project Name:MyFlinkProject
 * File Name:KafkaMessageStreaming
 * Package Name:com.kafkatoflink.consumer
 * Date:2019-5-24 23:29
 * Copyright (c) 2019, YBL All Rights Reserved.
 */
package com.kafkatoflink.consumer;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;
import org.apache.flink.util.Collector;

/**
 * ClassName: KafkaMessageStreaming <br/>
 * Function: ${TODO} ADD FUNCTION. <br/>
 * Reason: ${TODO} ADD REASON(可选). <br/>
 * date: 2019-5-24 23:29 <br/>
 * Description：
 *
 * @author chenm <20chenming08@163.com>
 * @version V1.0
 * @since JDK 1.8
 */
public class KafkaMessageStreaming {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 非常关键，一定要设置启动检查点！！
        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "flink:9092");
        props.setProperty("group.id", "flink-group");

        //    args[0] = "test-0921";  //传入的是kafka中的topic
        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<>("test-0921", new SimpleStringSchema(), props);
        consumer.assignTimestampsAndWatermarks(new MessageWaterEmitter());

        DataStream<Tuple2<String, Long>> keyedStream = env
                .addSource(consumer)
                .flatMap(new MessageSplitter())
                .keyBy(0)
                .timeWindow(Time.seconds(2))
                .apply(new WindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<Tuple2<String, Long>> out) throws Exception {
                        long sum = 0L;
                        int count = 0;
                        for (Tuple2<String, Long> record: input) {
                            sum += record.f1;
                            count++;
                        }
                        Tuple2<String, Long> result = input.iterator().next();
                        result.f1 = sum / count;
                        out.collect(result);
                    }
                });

        //将结果打印出来
        keyedStream.print();
        //    将结果保存到文件中
        //    args[1] = "E:\\FlinkTest\\KafkaFlinkTest";//传入的是结果保存的路径
        keyedStream.writeAsText("E:\\FlinkTest\\KafkaFlinkTest");
        env.execute("Kafka-Flink Test");
    }
}
