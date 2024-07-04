package com.lyj;

import com.lyj.util.ConfigLoader;
import com.lyj.util.TableUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static com.lyj.util.ConfigLoader.DB_PROFILE;
import static com.lyj.util.TableUtil.getColumns;
import static com.lyj.util.TableUtil.getConnectionOptions;
import static com.lyj.util.TableUtil.getInsertSql;
import static com.lyj.util.TableUtil.jdbcExecutionOptions;

/**
 * data导入postgresql
 * 品名导入数据库
 * 品名マスタ_宮川→ジェミニ→RC作業後_20240622追加.xlsx
 * shellName = レンゴーに紐づく品番 join shellName = レンゴーに紐づかない品番
 */
public class Mashin00ToPostgreSQL {

    private static final Logger logger = LoggerFactory.getLogger(Mashin00ToPostgreSQL.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // 通过命令行参来选择配置文件
        String activeProfile = params.get(DB_PROFILE);

        boolean checkParamsResult = checkParams(activeProfile);

        if (!checkParamsResult) {
            logger.error("params demo : " +
                    "--db_profile dev43  \n"

            );
            return;
        }

        ConfigLoader.loadConfiguration(activeProfile);

        String schemaName = "renmasall";
        String tableName = "mashin00";

        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行数
        env.setParallelism(1);

        Map<String, List<String>> columns = getColumns(schemaName, tableName, true);
        List<String> colNames = columns.get("COL_NAMES");
        List<String> colClasses = columns.get("COL_CLASS");
        String insertSql = getInsertSql(colNames, schemaName, tableName);
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("SELECT ").append(colNames.stream().reduce((s1, s2) -> s1 + "," + s2).orElse(null)).append(" from ").
                append(schemaName).append(".").append(tableName).append(" ;\n");
        logger.info("insertSql is {}", insertSql);
        logger.info("selectSql is {}", stringBuilder);

        // CSV 文件路径
        String folderPath = "input" + File.separator + "mashin00";
        File folder = new File(folderPath);
        if (folder.exists()) {
            File[] files = folder.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (!file.isDirectory()) {

                        String csvFilePath = folderPath + File.separator + file.getName();

                        // 读取 TXT 文件并创建 DataStream
                        DataStreamSource<String> csvDataStream = env
                                .readTextFile(csvFilePath);

                        // 将数据写入 PostgreSQL 数据库
                        csvDataStream.addSink(JdbcSink.sink(
                                insertSql,
                                (ps, t) -> {
                                    // 对每个数据元素进行写入操作
                                    String[] datas = t.split("\t", -1);
                                    for (int i = 0; i < colNames.size(); i++) {
                                        String colName = colNames.get(i);
                                        String colClass = colClasses.get(i);
                                        //处理共同字段
                                        if (i < datas.length) {
                                            setPsData(i + 1, colName, colClass, datas[i], ps, tableName);
                                        } else {
                                            if (colName.equalsIgnoreCase("insert_job_id") ||
                                                    colName.equalsIgnoreCase("insert_pro_id") ||
                                                    colName.equalsIgnoreCase("upd_user_id") ||
                                                    colName.equalsIgnoreCase("upd_job_id") ||
                                                    colName.equalsIgnoreCase("upd_pro_id")
                                            ) {
                                                setPsData(i + 1, colName, colClass, "", ps, tableName);
                                            } else if (colName.equalsIgnoreCase("insert_user_id") ||
                                                    colName.equalsIgnoreCase("partition_flag")
                                            ) {
                                                setPsData(i + 1, colName, colClass, tableName, ps, tableName);
                                            } else if (colName.equalsIgnoreCase("upd_sys_date") ||
                                                    colName.equalsIgnoreCase("insert_sys_date")) {
                                                setPsData(i + 1, colName, colClass, "1990-01-01 00:00:00", ps, tableName);
                                            }
                                        }
                                    }
                                },
                                jdbcExecutionOptions,
                                getConnectionOptions()
                        ));
                    }
                }
            }
        }
        env.execute("Flink Mashin00ToPostgreSQL job");
    }

    private static boolean checkParams(String activeProfile) {
        if (activeProfile == null) {
            logger.error("db_profile is null!");
            return false;
        }
        return true;
    }

    private static void setPsData(int parameterIndex, String colName, String colClass, String
            dataValue, PreparedStatement ps, String tableName) throws SQLException {
        //SKNHN1 前面补0
        if (colName.equalsIgnoreCase("SKNHN1")) {
            StringBuilder result = new StringBuilder();
            int length = dataValue.length();
            for (int i = 0; i < 48 - length; i++) {
                result.append(0);
            }
            result.append(dataValue);
            dataValue = result.toString();
        }
        TableUtil.setPsData(parameterIndex, colName, colClass, dataValue, ps, tableName);
    }
}