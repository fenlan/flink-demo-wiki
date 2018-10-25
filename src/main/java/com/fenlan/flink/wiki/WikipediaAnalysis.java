package com.fenlan.flink.wiki;

import org.apache.flink.api.common.functions.FoldFunction;
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

        KeyedStream<WikipediaEditEvent, String> keyedEdits = edits.keyBy(WikipediaEditEvent::getUser);

        DataStream<Tuple2<String, Long>> result = keyedEdits
                .timeWindow(Time.seconds(5))
                .fold(new Tuple2<>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> fold(Tuple2<String, Long> stringLongTuple2, WikipediaEditEvent o) {
                        stringLongTuple2.f0 = o.getUser();
                        stringLongTuple2.f1 += o.getByteDiff();
                        return stringLongTuple2;
                    }
                });
        result.print();
        see.execute();
    }
}
