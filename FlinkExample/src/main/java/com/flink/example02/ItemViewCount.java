/**
 * Project Name:MyFlinkProject
 * File Name:ItemViewCount
 * Package Name:com.flink.example02
 * Date:2019-1-23 20:33
 * Copyright (c) 2019, HSJRY All Rights Reserved.
 */
package com.flink.example02;

/**
 * ClassName: ItemViewCount <br/>
 * Function: ${TODO} ADD FUNCTION. <br/>
 * Reason: ${TODO} ADD REASON(可选). <br/>
 * date: 2019-1-23 20:33 <br/>
 * Description：
 *
 * @author 20chenming08@163.com
 * @version ${enclosing_type}${tags}
 * @since JDK 1.8
 */
public class ItemViewCount {
    public long itemId; // 商品id
    public long windowEnd; // 窗口结束时间戳
    public long viewCount; // 商品点击量

    public static ItemViewCount of(long itemId, long windowEnd, long viewCount){
        ItemViewCount itemViewCount = new ItemViewCount();
        itemViewCount.itemId = itemId;
        itemViewCount.windowEnd = windowEnd;
        itemViewCount.viewCount = viewCount;
        return itemViewCount;
    }
}
