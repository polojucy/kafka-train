package org.example.kafka.api.interceptor;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public class MyConsumerInterceptor implements ConsumerInterceptor<String, String> {

    /**
     * 消费数据之前的拦截方法
     * @param records
     * @return
     */
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        records.records("interceptor-topic").forEach(record -> {
            byte[] bytes = record.headers().headers("timestamp").iterator().next().value();
            String time = new String(bytes);
            System.out.println("header: "+ time);
        });
        return records;
    }

    /**
     * 消费数据之后（commit）的拦截方法
     * @param offsets
     */
    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        System.out.println("on commit");
    }

    @Override
    public void close() {
        System.out.println("close");
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
