/**
 * Project Name:MyFlinkProject
 * File Name:MessageWaterEmitter
 * Package Name:com.kafkatoflink.consumer
 * Date:2019-5-25 0:18
 * Copyright (c) 2019, YBL All Rights Reserved.
 */
package com.kafkatoflink.consumer;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * ClassName: MessageWaterEmitter <br/>
 * Function: ${TODO} ADD FUNCTION. <br/>
 * Reason: ${TODO} ADD REASON(可选). <br/>
 * date: 2019-5-25 0:18 <br/>
 * Description：
 *
 * @author chenm <chenming@ybl-group.com>
 * @version V1.0
 * @since JDK 1.8
 */
public class MessageWaterEmitter implements AssignerWithPunctuatedWatermarks<String> {

    @Override
    public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
        if (lastElement != null && lastElement.contains(",")) {
            String[] parts = lastElement.split(",");
            return new Watermark(Long.parseLong(parts[0]));
        }
        return null;
    }

    @Override
    public long extractTimestamp(String element, long previousElementTimestamp) {
        if (element != null && element.contains(",")) {
            String[] parts = element.split(",");
            return Long.parseLong(parts[0]);
        }
        return 0L;
    }
}
