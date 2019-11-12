/**
 * Project Name:MyFlinkProject
 * File Name:RedisSource
 * Package Name:com.flink.data.clean
 * Date:2019/11/11 22:02
 * Copyright (c) 2019, XUNZHAOTECH All Rights Reserved.
 */
package com.flink.data.clean;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.HashMap;
import java.util.Map;

/**
 * ClassName: RedisSource <br/>
 * Function: redis data source. <br/>
 *
 * @author chenm
 * @version V1.0.0
 * @date: 2019/11/11 22:02 <br/>
 * @since JDK 1.8
 */
public class RedisSource implements SourceFunction<HashMap<String, String>>{
    private Logger logger = LoggerFactory.getLogger(RedisSource.class);

    private Jedis jedis = null;

    private Boolean isRunning = Boolean.TRUE;

    /**
     * 休眠时间
     * 1min
     */
    private final Long SLEEP_MILLION = 60000L;

    @Override
    public void run(SourceContext<HashMap<String, String>> sourceContext) throws Exception {
        this.jedis = new Jedis("flink", 6379);

        /**
         *  Redis 中保存的国家和大区的关系
         *  Redis中进行数据的初始化，数据格式：
         *       Hash      大区      国家
         *       hset areas;   AREA_US    US
         *       hset areas;   AREA_CT    TW,HK
         *       hset areas    AREA_AR   PK,SA,KW
         *       hset areas    AREA_IN    IN
         */
        HashMap<String, String> keyValueMap = new HashMap<>();

        while (isRunning){
            try {
                /**
                 * 清除历史数据
                 */
                keyValueMap.clear();
                // 获取区域数据
                Map<String, String> areas = jedis.hgetAll("areas");
                // 转换国家对应大区
                for (Map.Entry<String, String> entry : areas.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    String[] splits = value.split(",");
                    for (String split : splits) {
                        keyValueMap.put(split, key);
                    }
                }

                // 将数据放入数据源
                if (keyValueMap.size() > 0){
                    sourceContext.collect(keyValueMap);
                }else {
                    logger.warn("从Redis中获取到的数据为空！");
                }

                Thread.sleep(SLEEP_MILLION);
            } catch (JedisConnectionException e) {
                logger.error("redis连接异常：", e);
                // 捕获redis连接异常并重建连接
                this.jedis = new Jedis("flink", 6379);
            } catch (Exception e){
                logger.error("Source数据源异常", e);
                throw e;
            }
        }
    }

    /**
     * 任务停止 设置false
     */
    @Override
    public void cancel() {
        isRunning = Boolean.FALSE;

        // 关闭redis连接
        if (this.jedis != null){
            this.jedis.close();
        }
    }
}
