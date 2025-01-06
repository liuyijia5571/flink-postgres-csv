package com.lyj.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcOutputFormat;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.lyj.util.ConfigLoader.getDatabasePassword;
import static com.lyj.util.ConfigLoader.getDatabaseUrl;
import static com.lyj.util.ConfigLoader.getDatabaseUsername;

public class TableUtil {

    private static final Logger logger = LoggerFactory.getLogger(TableUtil.class);

    public static final String[] possibleFormats = {"yyyy/MM/dd HH:mm:ss.SSS","yyyy/MM/dd HH:mm:ss","yyyy/MM/dd HH:mm"};

    public static final Timestamp timestampDate = Timestamp.valueOf("1990-01-01 00:00:00");

    public static final String CHARSET_NAME_31J = "Windows-31J";

    public static final Integer R05_DATE_SPLIT = 2310;


    public static final String I34 = "I34";

    public static final String M03 = "M03";

    public static final String COL_NAMES = "COL_NAMES";
    public static final String COL_CLASS = "COL_CLASS";
    public static final String COL_LENGTH = "COL_LENGTH";
    public static final String NUMERIC_SCALE = "NUMERIC_SCALE";


    public static final String FILE_NAME = "file_name";

    public static final String NOW_DATE;

    static {
        // 获取当前时间的时间戳
        long currentTimeMillis = System.currentTimeMillis();

        // 创建日期对象
        Date date = new Date(currentTimeMillis);

        // 定义日期格式
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        NOW_DATE = dateFormat.format(date);
    }


    public static final JdbcExecutionOptions jdbcExecutionOptions = JdbcExecutionOptions.builder().withBatchSize(1000) // 设置批处理大小
            .withBatchIntervalMs(200) // 设置批处理间隔时间
            .withMaxRetries(0) // 设置最大重试次数
            .build();

    public static Map<String, List<String>> getColumns(String schema, String tableName) throws Exception {
        return getColumns(schema, tableName, false);
    }

    public static String getInsertSql(List<String> colNames, String schemaName, String tableName) {
        StringBuilder sb = new StringBuilder();
        String colStr = colNames.stream().map(u -> "\"" + u + "\"").reduce((s1, s2) -> s1 + "," + s2).orElse(null);
        sb.append("INSERT INTO ").append(schemaName).append(".").append(tableName).append("(");
        sb.append(colStr);
        sb.append(") VALUES (");
        String valStr = colNames.stream().map(u -> " ? ").reduce((s1, s2) -> s1 + "," + s2).orElse(null);
        sb.append(valStr);
        sb.append(")");
        logger.debug("insert sql is {}", sb);
        return sb.toString();
    }

    public static Map<String, List<String>> getColumns(String schema, String tableName, boolean isTruncate) throws Exception {
        return getColumns(schema, tableName, isTruncate, false);
    }

    public static Map<String, List<String>> getColumns(String schema, String tableName, boolean isTruncate, boolean getSeqNo) throws Exception {
        Map<String, List<String>> columnsMap = new HashMap();
        List<String> colNames = new ArrayList<>();
        List<String> colClass = new ArrayList<>();
        List<String> colLength = new ArrayList<>();
        List<String> numericScaleList = new ArrayList<>();

        Connection conn = DriverManager.getConnection(getDatabaseUrl(), getDatabaseUsername(), getDatabasePassword());
        Statement stmt = conn.createStatement();
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT column_name, data_type , ");
        sb.append(" CASE\n")
                .append("        WHEN CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN CHARACTER_MAXIMUM_LENGTH \n")
                .append("        WHEN DATA_TYPE IN ('decimal', 'numeric') THEN NUMERIC_PRECISION + NUMERIC_SCALE \n")
                .append("        WHEN DATA_TYPE IN ('date', 'timestamp', 'datetime') THEN 10 \n")
                .append("        ELSE NULL \n")
                .append("    END AS col_length, \n")
                .append("case WHEN NUMERIC_SCALE is null THEN 0 ELSE NUMERIC_SCALE END AS NUMERIC_SCALE \n");
        sb.append("FROM information_schema.columns ");
        sb.append("WHERE table_schema ilike '");
        sb.append(schema).append("' AND table_name ilike '");
        sb.append(tableName).append("' order by ordinal_position");


        logger.info("execute sql is {}", sb);
        ResultSet rs = stmt.executeQuery(sb.toString());

        while (rs.next()) {
            if (rs.getString("column_name").equalsIgnoreCase("seq_no") && getSeqNo) {
                colNames.add(rs.getString("column_name"));
                colClass.add(rs.getString("data_type"));
                colLength.add(String.valueOf(rs.getInt("col_length")));
                numericScaleList.add(String.valueOf(rs.getInt("NUMERIC_SCALE")));
            }
            if (!rs.getString("column_name").equalsIgnoreCase("seq_no")) {
                colNames.add(rs.getString("column_name"));
                colClass.add(rs.getString("data_type"));
                colLength.add(String.valueOf(rs.getInt("col_length")));
                numericScaleList.add(String.valueOf(rs.getInt("NUMERIC_SCALE")));
            }
        }
        columnsMap.put(COL_NAMES, colNames);
        columnsMap.put(COL_CLASS, colClass);
        columnsMap.put(COL_LENGTH, colLength);
        columnsMap.put(NUMERIC_SCALE, numericScaleList);
        rs.close();

        if (isTruncate && !colNames.isEmpty()) {
            sb.setLength(0);
            sb.append("truncate table ").append(schema).append(".").append(tableName);
            logger.info("execute sql is {}", sb);
            stmt.execute(sb.toString());
        }
        if (colNames.isEmpty())
            logger.error("database table is not found : schema is {},tableName is {}", schema, tableName);
        stmt.close();
        conn.close();
        return columnsMap;
    }

    public static int getMaxSeq(String schema, String tableName, String seqCol) throws SQLException {
        Connection conn = DriverManager.getConnection(getDatabaseUrl(), getDatabaseUsername(), getDatabasePassword());
        Statement stmt = conn.createStatement();
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT max( \"").append(seqCol).append("\")");
        sb.append(" FROM  ").append(schema).append(".").append(tableName);
        logger.info("execute select sql is {}", sb);
        ResultSet rs = stmt.executeQuery(sb.toString());
        int maxSeq = 0;
        if (rs.next()) {
            maxSeq = rs.getInt(1);
        }
        rs.close();
        conn.close();
        return maxSeq;
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
                if (dataValue.contains("-")) {
                    try {
                        ps.setTimestamp(parameterIndex, Timestamp.valueOf(dataValue));
                    } catch (Exception e) {
                        logger.error("dataValue contains - tableName is {} colName is {} dataValue is {} error is {}", tableName, colName, dataValue, e.getMessage());
                        ps.setTimestamp(parameterIndex, timestampDate);
                    }
                } else if (dataValue.contains("/")) {
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
                        logger.error("dataValue contains   tableName: {} colName: {} colName: {} error is {}", tableName, colName, dataValue, e.getMessage());
                        ps.setTimestamp(parameterIndex, timestampDate);
                    }
                } else {
                    try {
                        // 计算天数部分和时间部分
                        LocalDateTime dateTime = getLocalDateTime(dataValue, tableName);
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
                if (dataValue != null)
                    ps.setString(parameterIndex, dataValue);
                else
                    ps.setString(parameterIndex, dataValue);
                break;
        }
    }

    public static LocalDateTime getLocalDateTime(String dataValue, String fileName) {
        try {
            double excelDate = Double.parseDouble(dataValue);

            int days = (int) excelDate;

            double fraction = excelDate - days;

            LocalDate baseDate = LocalDate.of(1900, 1, 1).minusDays(2); // Excel dates start on 1900-01-01, but there is a bug considering 1900 as a leap year
            // 基准日期

            // 计算日期
            LocalDate date = baseDate.plusDays(days);

            // 计算时间
            long totalSecondsInDay = (long) (fraction * 24 * 60 * 60);
            LocalTime time = LocalTime.ofSecondOfDay(totalSecondsInDay);

            // 合并日期和时间
            LocalDateTime dateTime = LocalDateTime.of(date, time);
            return dateTime;
        } catch (Exception e) {
            logger.error("file is {} ,message is {}", fileName, e.getMessage());
            LocalDateTime dateTime = LocalDateTime.of(1900, 1, 1, 0, 0, 0);
            return dateTime;
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
        logger.debug("execute is {}",sql);
        boolean result = stmt.execute(sql);
        stmt.close();
        conn.close();
        return result;
    }

    public static int executeSelectSql(String sql) throws SQLException {
        Connection conn = DriverManager.getConnection(getDatabaseUrl(), getDatabaseUsername(), getDatabasePassword());
        Statement stmt = conn.createStatement();
        logger.info("executeSelectSql,select sql is {}", sql);
        ResultSet rs = stmt.executeQuery(sql);
        int maxSeq = 0;
        if (rs.next()) {
            maxSeq = rs.getInt(1);
        }
        rs.close();
        conn.close();
        return maxSeq;
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

    public static RowTypeInfo getRowTypeInfo(Map<String, List<String>> columns) {
        List<TypeInformation<?>> typeInformationList = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        List<String> colName = columns.get("COL_NAMES");
        List<String> colClass = columns.get("COL_CLASS");
        for (int i = 0; i < colName.size(); i++) {
            String columnName = colName.get(i);
            fieldNames.add(columnName);

            String columnType = colClass.get(i);
            switch (columnType) {
                case "numeric":
                    typeInformationList.add(BasicTypeInfo.BIG_DEC_TYPE_INFO);
                    break;
                case "timestamp without time zone":
                    typeInformationList.add(BasicTypeInfo.DATE_TYPE_INFO);
                    break;
                // Add more types as needed
                default:
                    typeInformationList.add(BasicTypeInfo.STRING_TYPE_INFO);
                    break;
            }

        }
        TypeInformation<?>[] types = typeInformationList.toArray(new TypeInformation[0]);
        String[] names = fieldNames.toArray(new String[0]);
        return new RowTypeInfo(types, names);
    }

    public static int[] getSqlTypes(Map<String, List<String>> columns) {
        List<String> colClass = columns.get("COL_CLASS");
        int[] sqlTypes = new int[colClass.size()];
        for (int i = 0; i < colClass.size(); i++) {
            String columnType = colClass.get(i);
            switch (columnType) {
                case "integer":
                    sqlTypes[i] = Types.INTEGER;
                    break;
                case "numeric":
                    sqlTypes[i] = Types.NUMERIC;
                    break;
                case "timestamp without time zone":
                    sqlTypes[i] = Types.TIMESTAMP;
                    break;
                default:
                    sqlTypes[i] = Types.VARCHAR;
                    break;
            }

        }

        return sqlTypes;
    }

    public static TypeInformation[] getTypeInformationArr(Map<String, List<String>> columns) {
        List<String> colClass = columns.get("COL_CLASS");
        TypeInformation<?>[] rowTypes = new TypeInformation[colClass.size()];
        for (int i = 0; i < colClass.size(); i++) {
            String columnType = colClass.get(i);
            switch (columnType) {
                case "numeric":
                    rowTypes[i] = BasicTypeInfo.BIG_DEC_TYPE_INFO;
                    break;
                case "timestamp without time zone":
                    rowTypes[i] = BasicTypeInfo.DATE_TYPE_INFO;
                    break;
                default:
                    rowTypes[i] = BasicTypeInfo.STRING_TYPE_INFO;
                    break;
            }
        }
        return rowTypes;
    }

    public static String getGroupName(String groupId) {
        String groupName;
        switch (groupId) {
            case "H":
            case "h":
                groupName = "長野";
                break;
            case "F":
            case "f":
                groupName = "船橋";
                break;
            case "K":
            case "k":
                groupName = "市川";
                break;
            case "N":
            case "n":
                groupName = "中野";
                break;
            case "Z":
            case "z":
                groupName = "須坂";
                break;
            default:
                groupName = "その他";
                break;
        }
        return groupName;
    }

    public static void setFieldValue(Row row, int rowIndex, String colClass, String dataValue, String tableName) throws ParseException {
        setFieldValue(row, rowIndex, colClass, dataValue, "0", tableName);
    }

    public static void setFieldValue(Row row, int rowIndex, String colClass, String dataValue, String numericScale, String tableName) throws
            ParseException {
        switch (colClass) {
            case "integer":
                row.setField(rowIndex, Integer.valueOf(dataValue));
                break;
            case "numeric":
                dataValue = dataValue.trim();
                try {
                    if (StringUtils.isNotBlank(dataValue)) {
                        int scale = Integer.parseInt(numericScale);
                        if (scale > 0) {
                            BigDecimal dataBigDecimal = new BigDecimal(dataValue).divideToIntegralValue(BigDecimal.TEN.pow(scale)).setScale(scale, RoundingMode.HALF_UP);
                            row.setField(rowIndex, dataBigDecimal);
                        } else
                            row.setField(rowIndex, new BigDecimal(dataValue));
                    } else {
                        row.setField(rowIndex, new BigDecimal(0));
                    }
                } catch (Exception e) {
                    logger.error("data is {},tableName is {}，message is {} ", dataValue, tableName, e.getMessage());
                    row.setField(rowIndex, new BigDecimal(0));
                }

                break;
            case "timestamp without time zone":
                dataValue = dataValue.trim();
                if (dataValue.contains("-")) {
                    Timestamp timestamp = Timestamp.valueOf(dataValue);
                    row.setField(rowIndex, timestamp);
                } else if (dataValue.contains("/")) {
                    String determineDateFormat = determineDateFormat(dataValue, possibleFormats);
                    if (!"未知格式".equals(determineDateFormat)) {
                        SimpleDateFormat sdf = new SimpleDateFormat(determineDateFormat);
                        long time = sdf.parse(dataValue).getTime();
                        // 从 Date 对象创建 Timestamp 对象
                        Timestamp timestamp = new Timestamp(time);
                        row.setField(rowIndex, timestamp);
                    }
                }
                break;
            default:
                row.setField(rowIndex, dataValue);
                break;
        }

    }

    public static void insertDB(String schema, List<String> colNames, String
            tableName, Map<String, List<String>> columns, DataSet<Row> insertData) {
        // 将数据写入 PostGreSQL 数据库
        String insertSql = getInsertSql(colNames, schema, tableName);
        int[] sqlTypes = getSqlTypes(columns);
        JdbcOutputFormat finish = JdbcOutputFormat.buildJdbcOutputFormat()
                .setDrivername("org.postgresql.Driver")
                .setDBUrl(getDatabaseUrl())
                .setUsername(getDatabaseUsername())
                .setPassword(getDatabasePassword())
                .setQuery(insertSql)
                .setSqlTypes(sqlTypes)
                .finish();
        insertData.output(finish);
    }

    public static int getMaxSeq(String schema, String tableName) throws SQLException {
        Connection conn = DriverManager.getConnection(getDatabaseUrl(), getDatabaseUsername(), getDatabasePassword());
        Statement stmt = conn.createStatement();
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT COUNT(1)");
        sb.append(" FROM  ").append(schema).append(".").append(tableName);
        logger.info("execute select count sql is {}", sb);
        ResultSet rs = stmt.executeQuery(sb.toString());
        int maxSeq = 0;
        if (rs.next()) {
            maxSeq = rs.getInt(1);
        }
        rs.close();
        conn.close();
        return maxSeq;
    }

    public static boolean deleteDataByFileName(String schema, String tableName, String fileName) {
        Connection conn = null;
        PreparedStatement prepareStatement = null;
        try {
            conn = DriverManager.getConnection(getDatabaseUrl(), getDatabaseUsername(), getDatabasePassword());
            StringBuilder sb = new StringBuilder();
            sb.append("DELETE FROM ").append(schema).append(".").append(tableName);
            sb.append(" WHERE ").append(FILE_NAME).append(" = ?");
            logger.info("delete from sql is {} fileName is {}", sb, fileName);
            prepareStatement = conn.prepareStatement(sb.toString());
            prepareStatement.setString(1, fileName);
            boolean execute = prepareStatement.execute();
            return execute;
        } catch (SQLException e) {
            logger.error("Error executing delete statement", e);
            return false;
        } finally {
            if (prepareStatement != null) {
                try {
                    prepareStatement.close();
                } catch (SQLException e) {
                    logger.error("Error closing PreparedStatement", e);
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    logger.error("Error closing Connection", e);
                }
            }
        }
    }


    public static String getFormattedDate() {
        // 获取当前时间的时间戳
        long currentTimeMillis = System.currentTimeMillis();

        // 创建日期对象
        Date date = new Date(currentTimeMillis);

        // 定义日期格式
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

        // 将日期对象格式化为字符串
        String formattedDate = dateFormat.format(date);
        return formattedDate;
    }
}
