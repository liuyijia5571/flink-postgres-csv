package com.lyj;

import com.lyj.util.ConfigLoader;
import org.apache.commons.lang3.StringUtils;
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
import static com.lyj.util.TableUtil.CHARSET_NAME_31J;
import static com.lyj.util.TableUtil.NOW_DATE;
import static com.lyj.util.TableUtil.getColumns;
import static com.lyj.util.TableUtil.getConnectionOptions;
import static com.lyj.util.TableUtil.getInsertSql;
import static com.lyj.util.TableUtil.jdbcExecutionOptions;
import static com.lyj.util.TableUtil.setPsData;

/**
 * tsv 导入数据库
 * 参数
 * --db_profile
 * dev82
 * --txt_path
 * input/tsv/
 * --is_truncate
 * true
 */
public class TsvToPostgreSQL {

    private static final Logger logger = LoggerFactory.getLogger(TsvToPostgreSQL.class);

    public static void main(String[] args) throws Exception {
        // 通过命令行参来选择配置文件

        final ParameterTool params = ParameterTool.fromArgs(args);

        String activeProfile = params.get(DB_PROFILE);

        // CSV 文件路径
        String folderPath = params.get("txt_path");

        boolean checkParamsResult = checkParams(activeProfile, folderPath);
        if (!checkParamsResult) {
            logger.error("params demo : " + "--db_profile dev43  \n" + "--txt_path C:\\青果\\Data_Result\\sql\\data  \n" + "--is_truncate true  ");
            return;
        }

        //是否清空表
        boolean isTruncate = params.getBoolean("is_truncate", false);

        logger.info("truncate is {}", isTruncate);

        ConfigLoader.loadConfiguration(activeProfile);

        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        File folder = new File(folderPath);
        StringBuffer sb = new StringBuffer();
        if (folder.exists()) {
            File[] files = folder.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (!file.isDirectory()) {
                        String fileName = file.getName();
                        String csvFilePath = folderPath + File.separator + fileName;

                        String[] tableNameArr = fileName.split("_");
                        if (tableNameArr.length > 1) {
                            String schemaName = tableNameArr[0].toLowerCase();
                            String tableName = tableNameArr[1].split("\\.")[0].toLowerCase();

                            Map<String, List<String>> columns = getColumns(schemaName, tableName, isTruncate);
                            List<String> colClasses = columns.get("COL_CLASS");
                            List<String> colNames = columns.get("COL_NAMES");
                            if (colNames.isEmpty()) continue;
                            String insertSql = getInsertSql(colNames, schemaName, tableName);

                            sb.append("SELECT ").append(colNames.stream().reduce((s1, s2) -> s1 + "," + s2).orElse(null)).append(" from ").append(schemaName).append(".").append(tableName).append(" order by seq_no ;\n");

                            logger.info("insertSql is {}", insertSql);
                            // 读取 CSV 文件并创建 DataStream
                            DataStreamSource<String> csvDataStream = env.readTextFile(csvFilePath, CHARSET_NAME_31J);
                            // 将数据写入 PostgreSQL 数据库
                            csvDataStream.addSink(JdbcSink.sink(insertSql, (ps, t) -> {
                                // 对每个数据元素进行写入操作
                                String[] datas = t.split("\t");
                                for (int i = 0; i < colNames.size(); i++) {
                                    String colName = colNames.get(i);
                                    String colClass = colClasses.get(i);
                                    if (colName.equalsIgnoreCase("insert_pro_id") || colName.equalsIgnoreCase("upd_user_id") || colName.equalsIgnoreCase("upd_job_id") || colName.equalsIgnoreCase("upd_pro_id") || colName.equalsIgnoreCase("insert_user_id")) {
                                        if (datas.length > i) {
                                            setPsData(i + 1, colName, colClass, datas[i], ps, fileName);
                                        } else {
                                            setPsData(i + 1, colName, colClass, "", ps, fileName);
                                        }

                                    } else if (colName.equalsIgnoreCase("partition_flag")) {
                                        if (datas.length > i) {
                                            if (StringUtils.isNotBlank(datas[i]))
                                                setPsData(i + 1, colName, colClass, datas[i], ps, fileName);
                                            else
                                                setPsData(i + 1, colName, colClass, tableName.toUpperCase(), ps, fileName);
                                        } else {
                                            setPsData(i + 1, colName, colClass, tableName.toUpperCase(), ps, fileName);
                                        }
                                    } else if (colName.equalsIgnoreCase("upd_sys_date") || colName.equalsIgnoreCase("insert_sys_date")) {
                                        if (datas.length > i) {
                                            setPsData(i + 1, colName, colClass, datas[i], ps, fileName);
                                        } else {
                                            setPsData(i + 1, colName, colClass, NOW_DATE, ps, fileName);
                                        }
                                    } else if (colName.equalsIgnoreCase("insert_job_id")) {
                                        if (datas.length > i) {
                                            setPsData(i + 1, colName, colClass, datas[i], ps, fileName);
                                        } else {
                                            setPsData(i + 1, colName, colClass, TsvToPostgreSQL.class.getSimpleName(), ps, fileName);
                                        }
                                    } else {
                                        if (i < datas.length) {
                                            setPsData(i + 1, colName, colClass, datas[i], ps, tableName);
                                        } else {
                                            setPsData(i + 1, colName, colClass, "", ps, fileName);
                                        }
                                    }
                                }
                            }, jdbcExecutionOptions, getConnectionOptions()));
                        }
                    }
                }
            }
            // 执行流处理
            logger.info("Flink TxtToPostgreSQL job started");

            env.execute("TxtToPostgreSQL" + System.currentTimeMillis());

            logger.info("Flink TxtToPostgreSQL job finished");
        }
        logger.info("selectSql is {}", sb);

    }

    private static boolean checkParams(String activeProfile, String folderPath) {
        if (activeProfile == null) {
            logger.error("db_profile is null!");
            return false;
        }

        if (folderPath == null) {
            logger.error("txt_path is null!");
            return false;
        }
        File resultFile = new File(folderPath);

        if (!resultFile.isDirectory()) {
            logger.error("txt_path is not directory");
            return false;
        }
        return true;
    }
}