/**
 * Project Name:MyFlinkProject
 * File Name:MemoryUsageExtrator
 * Package Name:com.kafkatoflink.producer
 * Date:2019-5-25 1:51
 * Copyright (c) 2019, YBL All Rights Reserved.
 */
package com.kafkaflink.producer;

import com.sun.management.OperatingSystemMXBean;

import java.lang.management.ManagementFactory;

/**
 * ClassName: MemoryUsageExtrator <br/>
 * Function: ${TODO} ADD FUNCTION. <br/>
 * Reason: ${TODO} ADD REASON(可选). <br/>
 * date: 2019-5-25 1:51 <br/>
 * Description：
 *
 * @author chenm <20chenming08@163.com>
 * @version V1.0
 * @since JDK 1.8
 */
public class MemoryUsageExtrator {
    private static OperatingSystemMXBean mxBean =
            (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    /**
     * Get current free memory size in bytes
     * @return  free RAM size
     */
    public static long currentFreeMemorySizeInBytes() {
        return mxBean.getFreePhysicalMemorySize();
    }
}
