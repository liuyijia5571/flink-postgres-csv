package com.example;

import com.example.util.MultiFileSourceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class MultiFileSourceToPostgreSQL {

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("args.length  < 1");
            return;
        }
        String folderPath = args[0];
        List<String> filesPath = new ArrayList();
        File folder = new File(folderPath);
        if (folder.exists()) {
            File[] files = folder.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (!file.isDirectory()) {
                        String flieName = file.getName();
                        String csvFilePath = folderPath + File.separator + flieName;
                        filesPath.add(csvFilePath);
                    }
                }
            }
        }
        if (filesPath.size() == 0){
            System.err.println("files.size() = 0");
            return;
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);

        // 自定义文件源，传入多个文件路径

        DataStream<Tuple3<String, String,String>> dataStream = env.addSource(new MultiFileSourceFunction(filesPath));

        // 处理逻辑
        dataStream.map(new RichMapFunction<Tuple3<String, String,String>, String>() {
            @Override
            public String map(Tuple3<String, String,String> value) {
                return value.f2.toUpperCase();
            }
        }).print();

        env.execute("Flink Multi-File Source Example");
    }
}

