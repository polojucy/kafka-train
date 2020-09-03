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
    private static String CLIENT_ID_CONFIG = "quickstart-producer";
    private static Long id = 1000L;
    private static String[] names = {"jack", "rose" ,"jens"};
    private static String TOPIC_NAME = "quickstart-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID_CONFIG);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // kafka 异常重试机制 retries
//        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        // 可重试异常 如：网络原因导致消息发送失败
        // NetworkException
        // LeaderNotAvailableException
        // 不可重试异常 即：本身就是一条异常数据 如：消息体过大
        // RecordTooLargeException

        // acks 可配置的值有：0，x，-1/all
        // 0 表示生产者发送消息后不需要等待消费端响应
        // x 表示生产者发送消息后，至少需要Broker端接收到x个副本 x为一个正整数,kafka默认为 acks=1
        // -1 / all 表示生产者发送消息后，需要等待ISR中所有副本都成功接收到消息
//        properties.put(ProducerConfig.ACKS_CONFIG, 1);

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
