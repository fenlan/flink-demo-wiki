package com.fenlan.flink.sougou;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class SouGouAnalysis {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = env.readTextFile("/home/fenlan/Downloads/SogouQ.reduced", "GBK");

        DataSet<Log> logSet = text
                .filter(s -> s.split("\t").length == 5 ? true : false)
                .map(s -> {
                    String[] attr = s.split("\t");
                    return new Log(attr[0], attr[1], attr[2],
                            Short.parseShort(attr[3].split(" ")[0]),
                            Short.parseShort(attr[3].split(" ")[1]), attr[4]);
        });
        DataSet<Tuple2<String, Long>> map = logSet
                .map(new MapFunction<Log, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Log log) throws Exception {
                        return new Tuple2<>(log.time.substring(0, 2), 1L);
                    }
                })
                .groupBy(tuple -> tuple.f0)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> t1, Tuple2<String, Long> t2) throws Exception {
                        return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
                    }
                });
        map.print();
    }

    public static class Log {
        private String time;
        private String id;
        private String key;
        private short rank;
        private short hit;
        private String url;

        public Log() {}

        public Log(String time, String id, String key, short rank, short hit, String url) {
            this.time = time;
            this.id = id;
            this.key = key;
            this.rank = rank;
            this.hit = hit;
            this.url = url;
        }

        @Override
        public String toString() {
            return "Log{" +
                    "time='" + time + '\'' +
                    ", id=" + id +
                    ", key='" + key + '\'' +
                    ", rank=" + rank +
                    ", hit=" + hit +
                    ", url='" + url + '\'' +
                    '}';
        }
    }
}
