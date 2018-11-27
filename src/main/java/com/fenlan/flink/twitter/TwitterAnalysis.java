package com.fenlan.flink.twitter;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import redis.clients.jedis.Jedis;

import java.util.Properties;

public class TwitterAnalysis {

    private static Log log = LogFactory.getLog(TwitterAnalysis.class);
    private static String redisHost = "kafka-server1";
    private static int redisPort = 6379;
    private static Jedis jedis = new Jedis(redisHost, redisPort);

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka-server1:9092");
        properties.setProperty("group.id", "flink-twitter");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        FlinkKafkaConsumer010<String> consumer =
                new FlinkKafkaConsumer010<>("twitterstream", new SimpleStringSchema(), properties);
        consumer.setStartFromLatest();

        DataStream<JsonObject> streamSource = env.addSource(consumer).setParallelism(3)
                .map(TwitterAnalysis::parse).setParallelism(3);
        DataStream<Tuple2<String, Long>> countryCount = streamSource
                .map(TwitterAnalysis::countryTuple).setParallelism(1)
                .keyBy(tuple -> tuple.f0)
                .timeWindow(Time.seconds(5))
                .reduce((t1, t2) -> new Tuple2<>(t1.f0, t1.f1+t2.f1)).setParallelism(1);
        DataStream<Tuple2<String, Long>> langCount = streamSource
                .map(TwitterAnalysis::langTuple).setParallelism(1)
                .keyBy(tuple -> tuple.f0)
                .timeWindow(Time.seconds(5))
                .reduce((t1, t2) -> new Tuple2<>(t1.f0, t1.f1+t2.f1)).setParallelism(1);

        countryCount.print().setParallelism(1);
        langCount.print().setParallelism(1);
        env.execute();
    }

    private static JsonObject parse(String json) {
        JsonObject object = new JsonParser().parse(json).getAsJsonObject();
        jedis.incrBy("twitter-count", 1);
        return object;
    }

    private static Tuple2<String, Long> countryTuple(JsonObject object) {
        try {
            String country = object.get("place")
                    .getAsJsonObject().get("country").toString()
                    .replace("\"", "");
            jedis.hincrBy("twitter-country", country, 1);
            return new Tuple2<>(country, 1L);
        } catch (Exception e) {
            return new Tuple2<>("undefine", 1L);
        }
    }

    private static Tuple2<String, Long> langTuple(JsonObject object) {
        String lang = object.get("lang").toString().replace("\"", "");
        jedis.hincrBy("twitter-lang", lang, 1);
        return new Tuple2<>(lang, 1L);
    }
}
