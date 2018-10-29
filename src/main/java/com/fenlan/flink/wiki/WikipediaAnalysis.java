package com.fenlan.flink.wiki;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;


public class WikipediaAnalysis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());

        DataStream<Tuple2<String, Long>> result = edits.map(new MapFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(WikipediaEditEvent event) throws Exception {
                return new Tuple2<>(event.getUser(), (long)event.getByteDiff());
            }
        })
                .keyBy(stringLongTuple2 -> stringLongTuple2.f0)
                .timeWindow(Time.seconds(5))
                .reduce((t1, t2) -> new Tuple2<>(t1.f0, t1.f1+t2.f1));
        result.print().setParallelism(1);
        see.execute();
    }
}
