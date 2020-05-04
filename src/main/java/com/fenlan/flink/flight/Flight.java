package com.fenlan.flink.flight;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.sequencefile.SequenceFileWriterFactory;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Properties;

/**
 * Description:
 *
 * @author xiaozhuang.gj@alibaba-inc.com
 * date 2020-04-19 21:59
 */
public class Flight {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop-master:9092");
        properties.setProperty("zookeeper.connect", "hadoop-master:2181");
        properties.setProperty("group.id", "flink-flight");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.offset.reset", "earliest");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<>("flight", new SimpleStringSchema(), properties);
        consumer.setStartFromLatest();

        DataStream<JSONArray> streamSource = env.addSource(consumer).setParallelism(1)
                .map(Flight::parse).setParallelism(1);

        DataStream<Tuple2<LongWritable, Text>> rows = streamSource.map(a -> {
            ArrayList<String> list = new ArrayList<>(a.size());
            for (Object item : a) {
                list.add(item.toString());
            }
            return String.join(",", list);
        }).flatMap(new FlatMapFunction<String, Tuple2<LongWritable, Text>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<LongWritable, Text>> out)
                    throws Exception {
                out.collect(new Tuple2<>(new LongWritable(0L), new Text(value)));
            }
        })
        .setParallelism(1);

        Configuration hadoopConf = HadoopUtils.getHadoopConfiguration(GlobalConfiguration.loadConfiguration());
        final StreamingFileSink<Tuple2<LongWritable, Text>> sink = StreamingFileSink
                .forBulkFormat(
                        new Path("hdfs://hadoop-master:9000/data/flight/flink-flight"),
                        new SequenceFileWriterFactory<>(hadoopConf, LongWritable.class, Text.class))
                .build();
        rows.addSink(sink);

        env.execute();
    }

    private static JSONArray parse(String json) {
        JSONArray array = new JSONArray();
        json = json.replaceAll("'", "\"");
        json = json.replaceAll("None", "");
        json = json.replaceAll("False", "false");
        json = json.replaceAll("True", "true");
        try {
            array = JSON.parseArray(json);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return array;
    }
}
