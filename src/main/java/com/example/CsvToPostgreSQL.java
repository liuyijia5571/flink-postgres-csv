package com.example;

import com.example.util.ConfigLoader;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;

import static com.example.util.TableUtil.getColumns;
import static com.example.util.TableUtil.getConnectionOptions;
import static com.example.util.TableUtil.getInsertSql;
import static com.example.util.TableUtil.jdbcExecutionOptions;
import static com.example.util.TableUtil.setPsData;


public class CsvToPostgreSQL {

    private static final Logger logger = LoggerFactory.getLogger(CsvToPostgreSQL.class);

    public static void main(String[] args) throws Exception {
        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing();
//        env.setParallelism(1);

        if (args.length < 2) {
            System.err.println("args.length  < 2");
            return;
        }

        // CSV 文件路径
        String folderPath = args[0];
        logger.info("txt path is ", folderPath);

        // 通过命令行参来选择配置文件
        String activeProfile = args[1];

        boolean isTruncate = false;
        if (args.length >= 2) {
            if ("true".equalsIgnoreCase(args[2])) {
                isTruncate = true;
            }
        }
        logger.info("truncate is ", isTruncate);

        ConfigLoader.loadConfiguration(activeProfile);

        File folder = new File(folderPath);
        if (folder.exists()) {
            File[] files = folder.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (!file.isDirectory()) {
                        String flieName = file.getName();
                        String csvFilePath = folderPath + File.separator + flieName;

                        String[] tableNameArr = flieName.split("_");
                        if (tableNameArr.length > 1) {
                            String schemaName = tableNameArr[0].toLowerCase();
                            String tableName = tableNameArr[1].split("\\.")[0].toLowerCase();

                            Map<String, List<String>> columns;
                            columns = getColumns(schemaName, tableName, isTruncate);
                            List<String> colClasses = columns.get("COL_CLASS");
                            List<String> colNames = columns.get("COL_NAMES");
                            if (colNames.isEmpty())
                                continue;
                            String insertSql = getInsertSql(colNames, schemaName, tableName);

                            logger.info(insertSql);
                            // 读取 CSV 文件并创建 DataStream
                            DataStreamSource<String> csvDataStream = env.readTextFile(csvFilePath);
                            // 将数据写入 PostgreSQL 数据库
                            csvDataStream.addSink(JdbcSink.sink(insertSql, (ps, t) -> {
                                        // 对每个数据元素进行写入操作
                                        String[] datas = t.split("\\|");
                                        for (int i = 0; i < colNames.size(); i++) {
                                            String colName = colNames.get(i);
                                            String colClass = colClasses.get(i);
                                            setPsData(i + 1, colName, colClass, datas[i], ps, flieName);

                                        }
                                    }, jdbcExecutionOptions, getConnectionOptions()
                            ));
                        }
                    }
                }
            }
        }
        logger.info("Flink CsvToPostgreSQL job started");
        // 执行流处理
        env.execute("Flink CsvToPostgreSQL ");

        logger.info("Flink CsvToPostgreSQL job finished");
    }
}