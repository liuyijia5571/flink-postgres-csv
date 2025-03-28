package com.lyj;

import com.lyj.util.ConfigLoader;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;

import static com.lyj.util.ConfigLoader.getDatabasePassword;
import static com.lyj.util.ConfigLoader.getDatabaseUrl;
import static com.lyj.util.ConfigLoader.getDatabaseUsername;
import static com.lyj.util.TableUtil.getColumns;
import static com.lyj.util.TableUtil.getInsertSql;
import static com.lyj.util.TableUtil.getRowTypeInfo;
import static com.lyj.util.TableUtil.jdbcExecutionOptions;
import static com.lyj.util.TableUtil.setPsData;

/**
 * postgresql db1表到db2表
 */
public class PostgresDB1toDB2App {

    private static final Logger logger = LoggerFactory.getLogger(PostgresDB1toDB2App.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = org.apache.flink.api.java.utils.ParameterTool.fromArgs(args);

        // 通过命令行参来选择配置文件
        String oidActiveProfile = params.get("oid_db_profile");

        // 通过命令行参来选择配置文件
        String newActiveProfile = params.get("new_db_profile");

        boolean checkParamsResult = checkParams(oidActiveProfile, newActiveProfile);
        if (!checkParamsResult) {
            logger.error("params demo : " +
                    "--oid_db_profile dev43  \n" +
                    "--new_db_profile dev82  \n" +
                    "--is_truncate true  ");
            return;
        }

        //是否清空表
        String isTruncateStr = params.get("is_truncate", "false");

        boolean isTruncate = false;
        if ("true".equalsIgnoreCase(isTruncateStr)) {
            isTruncate = true;
        }
        logger.info("truncate is {}", isTruncate);

        ConfigLoader.loadConfiguration(oidActiveProfile);
        String oldDatabaseUrl = getDatabaseUrl();
        String oldDatabaseUsername = getDatabaseUsername();
        String oldDatabasePassword = getDatabasePassword();

        ConfigLoader.loadConfiguration(newActiveProfile);

        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName("org.postgresql.Driver")
                .withUrl(getDatabaseUrl())
                .withUsername(getDatabaseUsername())
                .withPassword(getDatabasePassword())
                .build();


        String sqlFilePath  = params.get("table_list");;

        File file = new File(sqlFilePath);
        if (!file.exists()) {
            logger.error("sqlFilePath is not exists");
            return;
        }

        if (file.isDirectory()) {
            logger.error("sqlFilePath is directory");
            return;
        }

        logger.info("tableFile patch is {}", sqlFilePath);
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<String> sqlLines = Files.readAllLines(Paths.get(sqlFilePath));

        for (String table : sqlLines) {
            String[] split = table.split("\\.");
            String schemaName = split[0].toLowerCase();
            String tableName = split[1].toLowerCase();

            Map<String, List<String>> columns;
            columns = getColumns(schemaName, tableName, isTruncate);
            List<String> colClasses = columns.get("COL_CLASS");
            List<String> colNames = columns.get("COL_NAMES");
            if (colNames.isEmpty())
                continue;
            StringBuilder sbSql = new StringBuilder();
            String collStr = colNames.stream().map(u -> "\"" + u + "\"").reduce((s1, s2) -> s1 + "," + s2).orElse(null);
            sbSql.append("SELECT ").append(collStr).append(" FROM ").append(table);
            logger.info("selectSql is {}", sbSql);
            RowTypeInfo rowTypeInfo = getRowTypeInfo(columns);
            // 创建一个数据流从源数据库读取数据
            DataStream<Row> sourceStream = env.createInput(JdbcInputFormat.buildJdbcInputFormat()
                    .setDrivername("org.postgresql.Driver")
                    .setDBUrl(oldDatabaseUrl)
                    .setUsername(oldDatabaseUsername)
                    .setPassword(oldDatabasePassword)
                    .setQuery(sbSql.toString())
                    .setRowTypeInfo(rowTypeInfo)
                    .finish());

            String insertSql = getInsertSql(colNames, schemaName, tableName);

            logger.info("insertSql is {}", insertSql);
            sourceStream.addSink(JdbcSink.sink(
                    insertSql,
                    (PreparedStatement ps, Row row) -> {
                        // 对每个数据元素进行写入操作
                        for (int i = 0; i < colNames.size(); i++) {
                            String colName = colNames.get(i);
                            String colClass = colClasses.get(i);
                            setPsData(i + 1, colName, colClass, row.getField(i), ps, tableName);
                        }
                    },
                    jdbcExecutionOptions,
                    connectionOptions
            ));
        }


        // 执行任务
        env.execute(PostgresDB1toDB2App.class.getName() + System.currentTimeMillis());

    }

    private static boolean checkParams(String oidActiveProfile, String newActiveProfile) {

        if (oidActiveProfile == null) {
            logger.error("oid_db_profile is null!");
            return false;
        }

        if (newActiveProfile == null) {
            logger.error("new_db_profile is null!");
            return false;
        }
        return true;
    }


}