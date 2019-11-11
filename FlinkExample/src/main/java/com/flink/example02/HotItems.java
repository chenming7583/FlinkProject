/**
 * Project Name:MyFlinkProject
 * File Name:HotItems
 * Package Name:com.flink.example02
 * Date:2019-1-23 0:56
 * Copyright (c) 2019, HSJRY All Rights Reserved.
 */
package com.flink.example02;

import com.flink.example02.pojo.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.File;
import java.net.URL;

/**
 * ClassName: HotItems <br/>
 * Function: ${TODO} ADD FUNCTION. <br/>
 * Reason: ${TODO} ADD REASON(可选). <br/>
 * date: 2019-1-23 0:56 <br/>
 * Description：
 *
 * @author 20chenming08@163.com
 * @version ${enclosing_type}${tags}
 * @since JDK 1.8
 */
public class HotItems {
    public static void main(String[] args) throws Exception{
        // 创建execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 按照eventTime处理
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 为了打印到控制台的结果不乱序，我们配置全局的并发为1，改变并发对结果正确性没有影响
        env.setParallelism(1);

        // 读取文件UserBehavior.csv数据
        URL fileUrl = HotItems.class.getClassLoader().getResource("UserBehavior.csv");
        Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));

        // 抽取UserBehavior的PojoTypeInfo
        PojoTypeInfo<UserBehavior> pojoType = (PojoTypeInfo<UserBehavior>)TypeExtractor.createTypeInfo(UserBehavior.class);
        // 定义字段
        String[] fieldOrder = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};

        // 创建 PojoCsvInputFormat
        PojoCsvInputFormat<UserBehavior> csvInput = new PojoCsvInputFormat<>(filePath, pojoType, fieldOrder);

        env.createInput(csvInput, pojoType).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                return userBehavior.timestamp * 1000;
            }
        }).filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior userBehavior) throws Exception {
                return userBehavior.behavior.equals("pv");
            }
        }).keyBy("itemId")
        .timeWindow(Time.minutes(60), Time.minutes(5))
        .aggregate(new CountAgg(), new WindowResultFunction())
        .keyBy("windowEnd")
        .process(new TopNHotItems(3))
        .print();

        env.execute("Hot Items Job");
    }
}
