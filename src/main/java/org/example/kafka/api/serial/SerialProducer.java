package org.example.kafka.api.serial;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.kafka.api.User;

import java.util.Properties;
import java.util.Random;

public class SerialProducer {

    private static String BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
    private static String CLIENT_ID_CONFIG = "serial-producer";
    private static String[] names = {"jack", "rose" ,"jens"};
    private static String TOPIC_NAME = "serial-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID_CONFIG);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserSerializer.class.getName());
        KafkaProducer<String, User> producer = new KafkaProducer<>(properties);

        try {
            for (int i = 0;i < 10;i++) {
                producer.send(generateUser(i));
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
    private static ProducerRecord<String, User> generateUser(int i) {
        User user = new User();
        user.setId("00"+i);
        user.setName(names[new Random().nextInt(names.length)]);
        return new ProducerRecord<>(TOPIC_NAME, user);
    }
}
