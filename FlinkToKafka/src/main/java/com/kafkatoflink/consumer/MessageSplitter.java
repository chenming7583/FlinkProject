/**
 * Project Name:MyFlinkProject
 * File Name:MessageSplitter
 * Package Name:com.kafkatoflink.consumer
 * Date:2019-5-25 0:19
 * Copyright (c) 2019, YBL All Rights Reserved.
 */
package com.kafkatoflink.consumer;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * ClassName: MessageSplitter <br/>
 * Function: ${TODO} ADD FUNCTION. <br/>
 * Reason: ${TODO} ADD REASON(可选). <br/>
 * date: 2019-5-25 0:19 <br/>
 * Description：
 *
 * @author chenm <chenming@ybl-group.com>
 * @version V1.0
 * @since JDK 1.8
 */
public class MessageSplitter implements FlatMapFunction<String, Tuple2<String, Long>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
        if (value != null && value.contains(",")) {
            String[] parts = value.split(",");
            out.collect(new Tuple2<>(parts[1], Long.parseLong(parts[2])));
        }
    }
}
