package com.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.io.File;

import static com.example.FlinkPostgresCsvApp.deleteFolder;

public class splitWord {

    public static void main(String[] args) throws Exception {
        // 设置执行环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String folderPath = "output/address";

        File folder = new File(folderPath);
        if (folder.exists()) {
            deleteFolder(folder);
            System.out.println("Folder deleted successfully.");
        } else {
            System.out.println("Folder does not exist.");
        }
        // 读取文本文件
        String filePath = "input/address.txt";
        String outFile = "output/address/splitAddress.txt";
        env.readTextFile(filePath)
                .map(new AddCommaEvery14Chars())
                 .writeAsText(outFile);
        env.execute("flink split job");

    }
    // 定义一个MapFunction来处理每行数据
    public static final class AddCommaEvery14Chars implements MapFunction<String, String> {
        @Override
        public String map(String value) {
            StringBuilder result = new StringBuilder();
            int length = value.length();
            for (int i = 0; i < length; i += 14) {
                if (i + 14 < length) {
                    result.append(value, i, i + 14).append(",");
                } else {
                    result.append(value.substring(i));
                }
            }
            return result.toString();
        }
    }
}

