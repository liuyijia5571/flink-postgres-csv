package com.example;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static com.example.util.TableUtil.deleteFolder;


public class SplitWord {

    private static final Logger logger = LoggerFactory.getLogger(SplitWord.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            logger.error("args length < 2");
            return;
        }

        // 设置执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String filePath = args[0];
        String folderPath = args[1];

        File folder = new File(folderPath);
        if (folder.exists()) {
            deleteFolder(folder);
            logger.info("Folder deleted successfully.");
        } else {
            logger.info("Folder does not exist.");
        }
        // 读取文本文件
        String outFile = folderPath + File.separatorChar + "splitAddress.txt";

        env.readTextFile(filePath).map(new RichMapFunction<String, Object>() {
                    @Override
                    public Object map(String value) {
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
                })
                .writeAsText(outFile);

        logger.info("Flink split job started");

        env.execute("flink split job");

        logger.info("Flink split job finished");

    }

}

