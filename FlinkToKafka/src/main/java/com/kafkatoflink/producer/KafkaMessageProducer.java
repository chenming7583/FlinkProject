/**
 * Project Name:MyFlinkProject
 * File Name:KafkaMessageProducer
 * Package Name:com.kafkatoflink.producer
 * Date:2019-5-25 1:49
 * Copyright (c) 2019, YBL All Rights Reserved.
 */
package com.kafkatoflink.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * ClassName: KafkaMessageProducer <br/>
 * Function: ${TODO} ADD FUNCTION. <br/>
 * Reason: ${TODO} ADD REASON(可选). <br/>
 * date: 2019-5-25 1:49 <br/>
 * Description：
 *
 * @author chenm <chenming@ybl-group.com>
 * @version V1.0
 * @since JDK 1.8
 */
public class KafkaMessageProducer {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.20.130:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        int totalMessageCount = 100;
        for (int i = 0; i < totalMessageCount; i++) {
            String value = String.format("%d,%s,%d", System.currentTimeMillis(), "machine-1", currentMemSize());
            producer.send(new ProducerRecord<>("test-0921", value), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        System.out.println("Failed to send message with exception " + exception);
                    }
                }
            });
            Thread.sleep(100L);
        }
        producer.close();
    }

    private static long currentMemSize() {
        return MemoryUsageExtrator.currentFreeMemorySizeInBytes();
    }
}
