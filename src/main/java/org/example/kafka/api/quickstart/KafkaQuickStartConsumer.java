package org.example.kafka.api.quickstart;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

public class KafkaQuickStartConsumer {

    private static String BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
    private static String GROUP_ID_CONFIG = "quickstart-group";
    private static String TOPIC_NAME = "quickstart-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //	非常重要的属性配置：与我们消费者订阅组有关系
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_CONFIG);
        //	常规属性：会话连接超时时间
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        //	消费者提交offset: 自动提交 & 手工提交，默认是自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000);
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
