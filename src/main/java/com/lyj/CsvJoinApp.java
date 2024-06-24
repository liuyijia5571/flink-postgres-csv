package com.lyj;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import static com.lyj.util.TableUtil.deleteFolder;

public class CsvJoinApp {
    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("args.length <1");
            System.exit(0);
            return;
        }
        // 设置执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String flieName = args[0];
        // 读取 CSV 文件并创建 DataStream
        String addFilePath = "input/add/" + flieName;
        DataStream<Tuple2<String, String>> addCsvDataStream = env.readTextFile(addFilePath).map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String u) throws Exception {
                Tuple2<String, String> tuple2 = new Tuple2<>();
                String[] split = u.split("\t");
                StringBuffer sb = new StringBuffer();
                sb.append(split[1]).append("\t").append(split[2]).append("\t").append(split[3]);
                tuple2.f0 = sb.toString();
                tuple2.f1 = u;
                return tuple2;
            }
        });

        // 读取 CSV 文件并创建 DataStream
        String csvFilePath = "input/add/file.txt";
        DataStream<Tuple2<String, String>> csvDataStream = env.readTextFile(csvFilePath).map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String u) throws Exception {
                Tuple2<String, String> tuple2 = new Tuple2<>();
                String[] split = u.split("\t");
                StringBuffer sb = new StringBuffer();
                sb.append(split[1]).append("\t").append(split[2]).append("\t").append(split[3]);
                tuple2.f0 = sb.toString();
                tuple2.f1 = u;
                return tuple2;
            }
        });

        // 差集
        SingleOutputStreamOperator<String> differenceStream = addCsvDataStream.keyBy(new KeySelector<Tuple2<String, String>, String>() {
            @Override
            public String getKey(Tuple2<String, String> value) {
                return value.f0;
            }
        }).connect(csvDataStream.keyBy(new KeySelector<Tuple2<String, String>, String>() {
            @Override
            public String getKey(Tuple2<String, String> value) {
                return value.f0;
            }
        })).process(new CoProcessFunction<Tuple2<String, String>, Tuple2<String, String>,  String>() {
            private final Set<String> rightSide = new HashSet<>();

            @Override
            public void processElement1(Tuple2<String, String> value, Context ctx, Collector<String> out) throws Exception {
                if (!rightSide.contains(value.f0)) {
                    out.collect(value.f1);
                }
            }

            @Override
            public void processElement2(Tuple2<String, String> value, Context ctx, Collector< String> out) throws Exception {
                rightSide.add(value.f0);
            }
        });

        String folderPath = "output/file/" + flieName + "differenceStream.txt";
        File folder = new File(folderPath);
        if (folder.exists()) {
            deleteFolder(folder);
            System.out.println("Folder deleted successfully.");
        } else {
            System.out.println("Folder does not exist.");
        }

        differenceStream.writeAsText(folderPath);

        env.execute("flink csvJionApp job");

    }
}
