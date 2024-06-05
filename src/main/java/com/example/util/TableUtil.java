package com.example.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;

import java.io.File;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TableUtil {
    public static final String DB_USER = "postgres";
    public static final String DB_PASSWORD = "postgres";

    public static final JdbcExecutionOptions jdbcExecutionOptions = JdbcExecutionOptions.builder()
            .withBatchSize(1000) // 设置批处理大小
            .withBatchIntervalMs(200) // 设置批处理间隔时间
            .withMaxRetries(5) // 设置最大重试次数
            .build();

    public static Map<String, List<String>> getColumns(String DB_URL, String schema, String tableName) throws Exception {
        return getColumns(DB_URL, schema, tableName, false);
    }

    public static String getInsertSql(List<String> colNames, String schemaName, String tableName) {
        StringBuffer sb = new StringBuffer();
        String collStr = colNames.stream().reduce((s1, s2) -> s1 + "," + s2).orElse(null);
        sb.append("INSERT INTO ").append(schemaName).append(".").append(tableName).append("(");
        sb.append(collStr);
        sb.append(") VALUES (");
        String valStr = colNames.stream().map(u -> " ? ,").collect(Collectors.joining());
        sb.append(valStr.substring(0, valStr.length() - 1));
        sb.append(")");
        return sb.toString();
    }

    public static Map<String, List<String>> getColumns(String DB_URL, String schema, String tableName, boolean isTruncate) throws Exception {
        Map columnsMap = new HashMap();
        List<String> colNames = new ArrayList<>();
        List<String> colClass = new ArrayList<>();
        Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
        Statement stmt = conn.createStatement();
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT column_name, data_type ");
        sb.append("FROM information_schema.columns ");
        sb.append("WHERE table_schema = '");
        sb.append(schema).append("' AND table_name = '");
        sb.append(tableName).append("'");

        System.out.println("执行sql：" + sb);
        ResultSet rs = stmt.executeQuery(sb.toString());

        while (rs.next()) {
            if (!rs.getString("column_name").equals("seq_no")) {
                colNames.add(rs.getString("column_name"));
                colClass.add(rs.getString("data_type"));
            }
        }
        columnsMap.put("COL_NAMES", colNames);
        columnsMap.put("COL_CLASS", colClass);
        if (isTruncate) {
            sb.setLength(0);
            sb.append("truncate table ").append(schema).append(".").append(tableName);
            System.out.println("执行sql：" + sb);
            stmt.execute(sb.toString());
        }
        rs.close();
        stmt.close();
        conn.close();
        return columnsMap;
    }

    public static void setPsData(int parameterIndex, String colName, String colClass, String dataValue, PreparedStatement ps) throws SQLException {
        //character,numeric,character varying,timestamp without time zone
        switch (colClass) {
            case "numeric":
                if (StringUtils.isNotBlank(dataValue)) {
                    ps.setBigDecimal(parameterIndex, new BigDecimal(dataValue));
                } else {
                    ps.setBigDecimal(parameterIndex, new BigDecimal(0));
                }
                break;
            case "timestamp without time zone":
                ps.setTimestamp(parameterIndex, Timestamp.valueOf(dataValue));
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
}
