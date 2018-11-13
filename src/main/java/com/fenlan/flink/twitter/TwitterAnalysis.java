package com.fenlan.flink.twitter;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

public class TwitterAnalysis {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka-server1:9092");
        properties.setProperty("group.id", "flink-twitter");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkKafkaConsumer010<String> consumer =
                new FlinkKafkaConsumer010<>("twitterstream", new SimpleStringSchema(), properties);
        consumer.setStartFromLatest();
        DataStream<String> streamSource = env.addSource(consumer);
        streamSource.print().setParallelism(4);
        env.execute();
    }
}
