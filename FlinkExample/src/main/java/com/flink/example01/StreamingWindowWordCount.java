/**
 * Project Name:MyFlinkProject
 * File Name:StreamingWindowWordCount
 * Package Name:com.flink.example01
 * Date:2018/9/21 0:32
 * Copyright (c) 2018, HSJRY All Rights Reserved.
 */
package com.flink.example01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * ClassName: StreamingWindowWordCount <br/>
 * Function: 流式单词统计. <br/>
 * Reason: 流式单词统计. <br/>
 * date: 2018/9/21 0:32 <br/>
 * Description：
 *
 * @author chenm <20chenming08@163.com>
 * @version V1.0.0
 * @since JDK 1.8
 */
public class StreamingWindowWordCount {
    public static void main(String[] args) throws Exception{
        //定义socket的端口号
        int port = 9000;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.out.println("没有指定port参数,使用默认值9000");
        }

        //获取运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //连接socket 获取输入数据
        DataStreamSource<String> text = env.socketTextStream("flink", port, "\n");

        //计算数据
        DataStream<WordWithCount> windowCount = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            // 打平操作，把每行单词转换为<word,count>类型的数据
            @Override
            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                String[] splits = value.split("\\s");
                for (String word : splits){
                    out.collect(new WordWithCount(word, 1L));
                }
            }
        }).keyBy("word") // 针对相同的word数据进行分组
        .timeWindow(Time.seconds(2), Time.seconds(1)) // 指定计算数据的窗口大小和活动的窗口大小
        .reduce(new ReduceFunction<WordWithCount>() {
            @Override
            public WordWithCount reduce(WordWithCount a, WordWithCount b) throws Exception {
                return new WordWithCount(a.word, a.count + b.count);
            }
        });

        // 把数据打印到控制台
        windowCount.print().setParallelism(1);// 使用一个平行度

        // 注意：因为flink是懒加载的，所以必须调用execute方法，上面的方法才会执行
        env.execute("streaming word count");
    }

    /**
     * 为了存储单次和单次出现的次数
     */
    public static class WordWithCount{
        public String word;
        public long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
