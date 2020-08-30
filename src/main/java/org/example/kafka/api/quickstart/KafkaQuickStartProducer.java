package org.example.kafka.api.quickstart;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class KafkaQuickStartProducer {

    private static String BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
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
                Thread.sleep(1000L);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    private static ProducerRecord<String, String> generateUser() {
        User user = new User();
        user.setId(id++);
        user.setName(names[new Random().nextInt(names.length)]);
        return new ProducerRecord<>(TOPIC_NAME, JSON.toJSONString(user));
    }
}
