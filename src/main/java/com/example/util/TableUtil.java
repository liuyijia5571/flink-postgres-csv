package com.example.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.example.util.ConfigLoader.getDatabasePassword;
import static com.example.util.ConfigLoader.getDatabaseUrl;
import static com.example.util.ConfigLoader.getDatabaseUsername;

public class TableUtil {

    private static final Logger logger = LoggerFactory.getLogger(TableUtil.class);

    public static final String[] possibleFormats = {"yyyy/MM/dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss.SSS"};

    public static final Timestamp timestampDate = Timestamp.valueOf("1990-01-01 00:00:00");

    public static final JdbcExecutionOptions jdbcExecutionOptions = JdbcExecutionOptions.builder().withBatchSize(1000) // 设置批处理大小
            .withBatchIntervalMs(200) // 设置批处理间隔时间
            .withMaxRetries(0) // 设置最大重试次数
            .build();

    public static Map<String, List<String>> getColumns(String schema, String tableName) throws Exception {
        return getColumns(schema, tableName, false);
    }

    public static String getInsertSql(List<String> colNames, String schemaName, String tableName) {
        StringBuffer sb = new StringBuffer();
        String colStr = colNames.stream().reduce((s1, s2) -> s1 + "," + s2).orElse(null);
        sb.append("INSERT INTO ").append(schemaName).append(".").append(tableName).append("(");
        sb.append(colStr);
        sb.append(") VALUES (");
        String valStr = colNames.stream().map(u -> " ? ").reduce((s1, s2) -> s1 + "," + s2).orElse(null);
        sb.append(valStr);
        sb.append(")");
        return sb.toString();
    }

    public static Map<String, List<String>> getColumns(String schema, String tableName, boolean isTruncate) throws Exception {
        Map<String, List<String>> columnsMap = new HashMap();
        List<String> colNames = new ArrayList<>();
        List<String> colClass = new ArrayList<>();
        Connection conn = DriverManager.getConnection(getDatabaseUrl(), getDatabaseUsername(), getDatabasePassword());
        Statement stmt = conn.createStatement();
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT column_name, data_type ");
        sb.append("FROM information_schema.columns ");
        sb.append("WHERE table_schema = '");
        sb.append(schema).append("' AND table_name = '");
        sb.append(tableName).append("'");

        logger.info("execute sql is {}", sb);
        ResultSet rs = stmt.executeQuery(sb.toString());

        while (rs.next()) {
            if (!rs.getString("column_name").equalsIgnoreCase("seq_no")) {
                colNames.add(rs.getString("column_name"));
                colClass.add(rs.getString("data_type"));
            }
        }
        columnsMap.put("COL_NAMES", colNames);
        columnsMap.put("COL_CLASS", colClass);
        rs.close();

        if (isTruncate && !colNames.isEmpty()) {
            sb.setLength(0);
            sb.append("truncate table ").append(schema).append(".").append(tableName);
            logger.info("execute sql is {}", sb);
            stmt.execute(sb.toString());
        }
        if (colNames.isEmpty()) logger.error("database table is not found : schema is {},tableName is {}", schema, tableName);
        stmt.close();
        conn.close();
        return columnsMap;
    }

    public static void setPsData(int parameterIndex, String colName, String colClass, String dataValue, PreparedStatement ps, String tableName) throws SQLException {
        //character,numeric,character varying,timestamp without time zone

        switch (colClass) {
            case "numeric":
                if (StringUtils.isNotBlank(dataValue)) {
                    try {
                        ps.setBigDecimal(parameterIndex, new BigDecimal(dataValue));
                    } catch (Exception e) {
                        logger.error(" numeric error tableName: {} colName: {} dataValue: {} error is {}", tableName, colName, dataValue, e.getMessage());
                        ps.setBigDecimal(parameterIndex, new BigDecimal(0));
                    }

                } else {
                    ps.setBigDecimal(parameterIndex, new BigDecimal(0));
                }
                break;
            case "timestamp without time zone":
                if (dataValue.indexOf("-") != -1) {
                    try {
                        ps.setTimestamp(parameterIndex, Timestamp.valueOf(dataValue));
                    } catch (Exception e) {
                        logger.error("dataValue contains - tableName is {} colName is {} dataValue is {} error is {}", tableName, colName, dataValue, e.getMessage());
                        ps.setTimestamp(parameterIndex, timestampDate);
                    }
                } else if (dataValue.indexOf("/") != -1) {
                    try {
                        String determineDateFormat = determineDateFormat(dataValue, possibleFormats);
                        if (!"未知格式".equals(determineDateFormat)) {
                            SimpleDateFormat sdf = new SimpleDateFormat(determineDateFormat);
                            long time = sdf.parse(dataValue).getTime();
                            Timestamp timestamp = new Timestamp(time);
                            ps.setTimestamp(parameterIndex, timestamp);
                        } else {
                            logger.error("tableName: {} colName: {} dataValue: {} ", tableName, colName, dataValue);
                            ps.setTimestamp(parameterIndex, timestampDate);
                        }
                    } catch (ParseException e) {
                        logger.error("dataValue contains /  tableName: {} colName: {} colName: {} error is {}", tableName, colName, dataValue, e.getMessage());
                        ps.setTimestamp(parameterIndex, timestampDate);
                    }
                } else {
                    try {
                        // 计算天数部分和时间部分
                        double excelDate = Double.parseDouble(dataValue);
                        int days = (int) excelDate;
                        double fraction = excelDate - days;

                        // 基准日期
                        LocalDate baseDate = LocalDate.of(1900, 1, 1).minusDays(2); // Excel dates start on 1900-01-01, but there is a bug considering 1900 as a leap year

                        // 计算日期
                        LocalDate date = baseDate.plusDays(days);

                        // 计算时间
                        long totalSecondsInDay = (long) (fraction * 24 * 60 * 60);
                        LocalTime time = LocalTime.ofSecondOfDay(totalSecondsInDay);

                        // 合并日期和时间
                        LocalDateTime dateTime = LocalDateTime.of(date, time);

                        // 转换为 Timestamp
                        Timestamp timestamp = Timestamp.valueOf(dateTime);
                        ps.setTimestamp(parameterIndex, timestamp);
                    } catch (Exception e) {
                        logger.error("dataValue contains /  tableName: {} colName: {} colName: {} error is {}", tableName, colName, dataValue, e.getMessage());
                        ps.setTimestamp(parameterIndex, timestampDate);
                    }
                }
                break;
            default:
                ps.setString(parameterIndex, dataValue);
                break;
        }
    }

    public static void deleteFolder(File folder) {
        File[] files = folder.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteFolder(file);
                } else {
                    file.delete();
                }
            }
        }
        folder.delete();
    }

    public static boolean executeSql(String sql) throws SQLException {
        Connection conn = DriverManager.getConnection(getDatabaseUrl(), getDatabaseUsername(), getDatabasePassword());
        Statement stmt = conn.createStatement();
        boolean result = stmt.execute(sql);
        stmt.close();
        conn.close();
        return result;
    }

    public static String determineDateFormat(String dateStr, String[] formats) {
        for (String format : formats) {
            if (isValidFormat(dateStr, format)) {
                return format;
            }
        }
        return "未知格式";
    }

    public static boolean isValidFormat(String dateStr, String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        sdf.setLenient(false);
        try {
            // 尝试解析日期字符串
            sdf.parse(dateStr);
            // 解析成功
            return true;
        } catch (ParseException e) {
            // 捕获到异常，解析失败
            return false;
        }
    }

    public static JdbcConnectionOptions getConnectionOptions() {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(getDatabaseUrl()).withDriverName("org.postgresql.Driver").withUsername(getDatabaseUsername()).withPassword(getDatabasePassword()).build();
    }
}
