package org.example.kafka.api.quickstart;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaQuickStartProducer {

    private static String BOOTSTRAP_SERVERS_CONFIG = "my-kafka:9092";
    private static String CLIENT_ID_CONFIG = "quickstart-client";
    private static Long id = 1000L;
    private static String[] names = {"jack", "rose" ,"jens"};
    private static String TOPIC_NAME = "quickstart-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID_CONFIG);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        try {
            while (true) {
                producer.send(generateUser());
                // 同步send
//                Future<RecordMetadata> future = producer.send(generateUser());
//                RecordMetadata metadata = future.get();
//                System.out.println(String.format("topic: %s,offset: %s,partition: %s,timestamp: %s",
//                        metadata.topic(),
//                        metadata.offset(),
//                        metadata.partition(),
//                        metadata.timestamp()));
                // 异步send
//                producer.send(generateUser(), (metadata, e) -> {
//                    System.out.println(String.format("topic: %s,offset: %s,partition: %s,timestamp: %s",
//                        metadata.topic(),
//                        metadata.offset(),
//                        metadata.partition(),
//                        metadata.timestamp()));
//                });
                Thread.sleep(1000L);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    /**
     * 随机生成user对象
     * @return
     */
    private static ProducerRecord<String, String> generateUser() {
        User user = new User();
        user.setId(id++);
        user.setName(names[new Random().nextInt(names.length)]);
        return new ProducerRecord<>(TOPIC_NAME, JSON.toJSONString(user));
    }
}
