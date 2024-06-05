package com.example;

import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;

import java.io.File;
import java.util.List;
import java.util.Map;

import static com.example.util.TableUtil.*;

public class CsvToPostgreSQL {

    private static final String DB_URL = "jdbc:postgresql://192.168.166.168:5432/postgres";

    private static final JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .withUrl(DB_URL)
            .withDriverName("org.postgresql.Driver")
            .withUsername(DB_USER)
            .withPassword(DB_PASSWORD)
            .build();

    public static void main(String[] args) throws Exception {
        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // CSV 文件路径
        String folderPath = "input";
        File folder = new File(folderPath);
        if (folder.exists()) {
            File[] files = folder.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (!file.isDirectory()) {
                        String flieName = file.getName();
                        String csvFilePath = folderPath + "/" + flieName;

                        String[] tableNameArr = flieName.split("_");
                        if (tableNameArr.length > 1) {
                            String schemaName = tableNameArr[0];
                            String tableName = tableNameArr[1].replace(".csv", "").toLowerCase();
                            Map<String, List<String>> columns = getColumns(DB_URL, schemaName, tableName);
                            List<String> colClasses = columns.get("COL_CLASS");
                            List<String> colNames = columns.get("COL_NAMES");

                            String insertSql = getInsertSql(colNames, schemaName, tableName);

                            System.out.println(insertSql);
                            // 读取 CSV 文件并创建 DataStream
                            DataStreamSource<String> csvDataStream = env.readTextFile(csvFilePath);
                            // 将数据写入 PostgreSQL 数据库
                            csvDataStream.addSink(JdbcSink.sink(insertSql, (ps, t) -> {
                                        // 对每个数据元素进行写入操作
                                        String[] datas = t.split(",");
                                        for (int i = 0; i < colNames.size(); i++) {
                                            String colName = colNames.get(i);
                                            String colClass = colClasses.get(i);
                                            setPsData(i + 1, colName, colClass, datas[i], ps);

                                        }
                                    }, jdbcExecutionOptions, connectionOptions
                            ));
                        }
                    }
                }
            }
        }
        // 执行流处理
        env.execute("Flink CSV to PostgreSQL ");
    }
}