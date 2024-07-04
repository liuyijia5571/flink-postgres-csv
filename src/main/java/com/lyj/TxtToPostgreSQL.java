package com.lyj;

import com.lyj.util.ConfigLoader;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;

import static com.lyj.util.ConfigLoader.DB_PROFILE;
import static com.lyj.util.TableUtil.getColumns;
import static com.lyj.util.TableUtil.getConnectionOptions;
import static com.lyj.util.TableUtil.getInsertSql;
import static com.lyj.util.TableUtil.jdbcExecutionOptions;
import static com.lyj.util.TableUtil.setPsData;


public class TxtToPostgreSQL {

    private static final Logger logger = LoggerFactory.getLogger(TxtToPostgreSQL.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // 通过命令行参来选择配置文件
        String activeProfile = params.get(DB_PROFILE);

        // CSV 文件路径
        String folderPath = params.get("txt_path");

        boolean checkParamsResult = checkParams(activeProfile, folderPath);
        if (!checkParamsResult) {
            logger.error("params demo : " +
                    "--db_profile dev43  \n" +
                    "--txt_path C:\\青果\\Data_Result\\sql\\data  \n" +
                    "--is_truncate true  ");
            return;
        }

        //是否清空表
        String isTruncateStr = params.get("is_truncate", "false");

        boolean isTruncate = false;
        if ("true".equalsIgnoreCase(isTruncateStr))
            isTruncate = true;
        logger.info("truncate is {}", isTruncate);

        ConfigLoader.loadConfiguration(activeProfile);
        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.getConfig().setGlobalJobParameters(params);


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

                            logger.info("insertSql is {}", insertSql);
                            // 读取 CSV 文件并创建 DataStream
                            DataStreamSource<String> csvDataStream = env.readTextFile(csvFilePath);
                            // 将数据写入 PostgreSQL 数据库
                            csvDataStream.addSink(JdbcSink.sink(insertSql, (ps, t) -> {
                                        // 对每个数据元素进行写入操作
                                        String[] datas = t.split("\\|");
                                        for (int i = 0; i < colNames.size(); i++) {
                                            String colName = colNames.get(i);
                                            String colClass = colClasses.get(i);
                                            if (i < datas.length) {
                                                setPsData(i + 1, colName, colClass, datas[i], ps, tableName);
                                            } else {
                                                if ("partition_flag".equalsIgnoreCase(colName)) {
                                                    setPsData(i + 1, colName, colClass, tableName, ps, tableName);
                                                } else {
                                                    setPsData(i + 1, colName, colClass, "", ps, tableName);
                                                }
                                            }
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
        env.execute("Flink CsvToPostgreSQL " + System.currentTimeMillis());

        logger.info("Flink CsvToPostgreSQL job finished");
    }

    private static boolean checkParams(String activeProfile, String folderPath) {
        if (activeProfile == null) {
            logger.error("db_profile is null!");
            return false;
        }

        if (folderPath == null) {
            logger.error("csv_path is null!");
            return false;
        }
        File resultFile = new File(folderPath);

        if (!resultFile.isDirectory()) {
            logger.error("csv_path is not directory");
            return false;
        }
        return true;
    }
}