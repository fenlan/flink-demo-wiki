package com.fenlan.flink.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Description:
 *
 * @author xiaozhuang.gj@alibaba-inc.com
 * date 2020-04-19 22:50
 */
public class ConsumerExample {

    private  String topicName;
    private static String kafkaClusterIP = "hadoop-master:9092";

    public ConsumerExample(String topic){
        topicName = topic;
    }

    public  void consume(){
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaClusterIP);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicName));

        //consume record
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }

    public static void main(String[] args) {
        new ConsumerExample("flight").consume();
    }
}
