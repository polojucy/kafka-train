package org.example.kafka.api.interceptor;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class InterceptorConsumer {

    private static String BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
    private static String GROUP_ID_CONFIG = "interceptor-group";
    private static String TOPIC_NAME = "interceptor-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_CONFIG);
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000);

        properties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,MyConsumerInterceptor.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000L));
                records.partitions().forEach(topicPartition -> records.records(topicPartition).forEach(record -> {
                    System.err.println(String.format("consumer kafka message = > " +
                                    "[key : %s, value : %s, offset: %d]",
                            record.key(),
                            record.value(),
                            record.offset()));
                }));

            }
        } finally {
            consumer.close();
        }
    }
}
