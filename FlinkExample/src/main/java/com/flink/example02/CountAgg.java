/**
 * Project Name:MyFlinkProject
 * File Name:CountAgg
 * Package Name:com.flink.example02
 * Date:2019-1-23 20:23
 * Copyright (c) 2019, HSJRY All Rights Reserved.
 */
package com.flink.example02;

import com.flink.example02.pojo.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * ClassName: CountAgg <br/>
 * Function: ${TODO} ADD FUNCTION. <br/>
 * Reason: ${TODO} ADD REASON(可选). <br/>
 * date: 2019-1-23 20:23 <br/>
 * Description：count 统计的聚合函数实现，没出现一条记录加一
 *
 * @author 20chenming08@163.com
 * @version ${enclosing_type}${tags}
 * @since JDK 1.8
 */
public class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(UserBehavior userBehavior, Long acc) {
        return acc + 1;
    }

    @Override
    public Long getResult(Long acc) {
        return acc;
    }

    @Override
    public Long merge(Long acc1, Long acc2) {
        return acc1 + acc2;
    }
}
