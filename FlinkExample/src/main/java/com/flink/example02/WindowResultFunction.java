/**
 * Project Name:MyFlinkProject
 * File Name:WindowResultFunction
 * Package Name:com.flink.example02
 * Date:2019-1-23 20:31
 * Copyright (c) 2019, HSJRY All Rights Reserved.
 */
package com.flink.example02;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * ClassName: WindowResultFunction <br/>
 * Function: ${TODO} ADD FUNCTION. <br/>
 * Reason: ${TODO} ADD REASON(可选). <br/>
 * date: 2019-1-23 20:31 <br/>
 * Description：
 *
 * @author chenm25100 <chenm25100@hsjry.com>
 * @version ${enclosing_type}${tags}
 * @since JDK 1.8
 */
public class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {

    @Override
    public void apply(Tuple key, // 窗口的主键，即itemId
                      TimeWindow window, // 窗口
                      Iterable<Long> aggregateResult, // 聚合函数的结果，即count值
                      Collector<ItemViewCount> collector // 输出类型为 ItemViewCount
    ) throws Exception {
        Long itemId = ((Tuple1<Long>)key).f0;
        Long count = aggregateResult.iterator().next();
        collector.collect(ItemViewCount.of(itemId, window.getEnd(), count));
    }
}
