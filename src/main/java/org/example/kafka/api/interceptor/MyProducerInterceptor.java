package org.example.kafka.api.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;

import java.util.Date;
import java.util.Map;
import java.util.UUID;

public class MyProducerInterceptor implements ProducerInterceptor<String, String> {

    private volatile long successCount = 0;
    private volatile long failCount = 0;

    /**
     * 发送消息之前的拦截方法
     * @param record
     * @return
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
//        System.out.println("------MyProducerInterceptor onSend------");
        System.out.println(record.value());
        Headers headers = record.headers();
        headers.add("timestamp", UUID.randomUUID().toString().getBytes());
        ProducerRecord newRecord = new ProducerRecord<>(
                record.topic(),
                record.partition(),
                record.timestamp(),
                record.key(),
                record.value(),
                headers
        );
        return newRecord;
    }

    /**
     * 发送消息之后（ack）的拦截方法
     * @param metadata
     * @param exception
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
//        System.out.println("------MyProducerInterceptor onAcknowledgement------");
        if (null == exception) {
            successCount++;
        }else {
            failCount++;
        }
    }

    @Override
    public void close() {
        System.out.println(String.format("close kafka producer message successful ratio: %s %%",(int) (successCount / (successCount + failCount) * 100)));
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
