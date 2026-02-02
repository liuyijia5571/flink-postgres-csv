package com.lyj;

import com.lyj.util.ConfigLoader;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.jdbc.JdbcOutputFormat;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.lyj.util.ConfigLoader.getDatabasePassword;
import static com.lyj.util.ConfigLoader.getDatabaseUrl;
import static com.lyj.util.ConfigLoader.getDatabaseUsername;
import static com.lyj.util.TableUtil.getColumns;
import static com.lyj.util.TableUtil.getInsertSql;
import static com.lyj.util.TableUtil.getSqlTypes;


/**
 * postgresql db1表到db2表
 */
public class PostgresDB1toDB2App {

    private static final Logger logger = LoggerFactory.getLogger(PostgresDB1toDB2App.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // 通过命令行参来选择配置文件
        String oidActiveProfile = params.get("oid_db_profile");

        // 通过命令行参来选择配置文件
        String newActiveProfile = params.get("new_db_profile");

        boolean checkParamsResult = checkParams(oidActiveProfile, newActiveProfile);
        if (!checkParamsResult) {
            logger.error("params demo : " + "--oid_db_profile dev43  \n" + "--new_db_profile dev82  \n" + "--table_list C:\\青果\\Data_Result\\sql\\tableName.txt  ");
            return;
        }

        ConfigLoader.loadConfiguration(oidActiveProfile);

        String oldDatabaseUrl = getDatabaseUrl();
        String oldDatabaseUsername = getDatabaseUsername();
        String oldDatabasePassword = getDatabasePassword();

        ConfigLoader.loadConfiguration(newActiveProfile);
        String newDatabaseUrl = getDatabaseUrl();
        String newDatabaseUsername = getDatabaseUsername();
        String newDatabasePassword = getDatabasePassword();

        String sqlFilePath = params.get("table_list");

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
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        List<String> sqlLines = Files.readAllLines(Paths.get(sqlFilePath));

        for (String line : sqlLines) {
            //解析参数
            String[] split = line.split("\t", -1);
            String isTruncateStr = split[1];
            String allTable = split[0];
            String[] split1 = allTable.split("\\.");
            String schemaName = split1[0].toLowerCase();
            String tableName = split1[1].toLowerCase();

            boolean isTruncate = false;
            boolean isUpdate = false;
            if ("true".equalsIgnoreCase(isTruncateStr)) {
                isTruncate = true;
            }

            Map<String, List<String>> columns = getColumns(schemaName, tableName, isTruncate);
            List<String> colNames = columns.get("COL_NAMES");
            if (colNames.isEmpty()) {
                logger.error("table {} not is exist;", allTable);
                continue;
            }
            StringBuilder sbSql = new StringBuilder();
            String collStr = colNames.stream().map(u -> "\"" + u + "\"").reduce((s1, s2) -> s1 + "," + s2).orElse(null);
            sbSql.append("SELECT ").append(collStr).append(" FROM ").append(allTable);

            logger.info("selectSql is {}", sbSql);
            RowTypeInfo rowTypeInfo = getRowTypeInfo(columns);
            // 创建一个数据流从源数据库读取数据
            String whereStr = "";
            if ("renmasall.masbnk00".equalsIgnoreCase(allTable.trim()) && !isTruncate) {
                isUpdate = true;
                whereStr = " WHERE zkbbk1 = '1' ";
            }
            if ("renmasall.masbka00".equalsIgnoreCase(allTable.trim())) {
                isUpdate = true;
            }
            boolean finalIsUpdate = isUpdate;
            DataSet<Row> oldDataSet = env.createInput(JdbcInputFormat.buildJdbcInputFormat()
                    .setDrivername("org.postgresql.Driver")
                    .setDBUrl(oldDatabaseUrl)
                    .setUsername(oldDatabaseUsername)
                    .setPassword(oldDatabasePassword)
                    .setQuery(sbSql + whereStr)
                    .setRowTypeInfo(rowTypeInfo)
                    .finish());
            DataSet<Row> result = oldDataSet;
            DataSet<Row> updateResult = null;
            //提取关联字段
            String[] cols = isTruncateStr.split(",");

            if (cols.length > 0) {
                List<Integer> joinCols = getJoinCols(cols, colNames);
                int[] array = joinCols.stream().mapToInt(Integer::intValue).toArray();

                if (array.length > 0) {
                    DataSet<Row> newDataSet = env.createInput(JdbcInputFormat.buildJdbcInputFormat()
                            .setDrivername("org.postgresql.Driver")
                            .setDBUrl(newDatabaseUrl)
                            .setUsername(newDatabaseUsername)
                            .setPassword(newDatabasePassword)
                            .setQuery(sbSql.toString())
                            .setRowTypeInfo(rowTypeInfo)
                            .finish());


                    result = oldDataSet.leftOuterJoin(newDataSet)
                            .where(array).equalTo(array)
                            .with(new FlatJoinFunction<Row, Row, Row>() {
                                @Override
                                public void join(Row first, Row second, Collector<Row> out) {
                                    if (second == null) {
                                        out.collect(first);
                                    }
                                }
                            });
                    if (finalIsUpdate) {
                        updateResult = oldDataSet.leftOuterJoin(newDataSet)
                                .where(array).equalTo(array)
                                .with(new FlatJoinFunction<Row, Row, Row>() {
                                    @Override
                                    public void join(Row first, Row second, Collector<Row> out) {
                                        if (second != null) {
                                            if (finalIsUpdate) {
                                                out.collect(first);
                                            }

                                        }
                                    }
                                });
                    }
                }

            }
//            result.sortPartition(row->((BigDecimal)row.getField(0)).intValue(), Order.ASCENDING)
//                    .map(row->row.toString().replace(",","\t")).writeAsText("output/masAll/"+allTable+".txt", FileSystem.WriteMode.OVERWRITE);

            // 将数据写入 PostGreSQL 数据库
            String insertSql = getInsertSql(colNames, schemaName, tableName);
            int[] sqlTypes = getSqlTypes(columns);

            JdbcOutputFormat finish = JdbcOutputFormat.buildJdbcOutputFormat()
                    .setDrivername("org.postgresql.Driver")
                    .setDBUrl(newDatabaseUrl)
                    .setUsername(newDatabaseUsername)
                    .setPassword(newDatabasePassword)
                    .setQuery(insertSql)
                    .setSqlTypes(sqlTypes)
                    .finish();
            if (finalIsUpdate) {
//                result.filter(u->u.getKind() == RowKind.INSERT).sortPartition(row->((BigDecimal)row.getField(0)).intValue(), Order.ASCENDING)
//                    .map(row->row.toString().replace(",","\t")).writeAsText("output/insert/"+allTable+".txt", FileSystem.WriteMode.OVERWRITE);
//
//                result.filter(u->u.getKind() == RowKind.UPDATE_AFTER).sortPartition(row->((BigDecimal)row.getField(0)).intValue(), Order.ASCENDING)
//                        .map(row->row.toString().replace(",","\t")).writeAsText("output/update_after/"+allTable+".txt", FileSystem.WriteMode.OVERWRITE);
//                result.filter(u->u.getKind() == RowKind.UPDATE_BEFORE).sortPartition(row->((BigDecimal)row.getField(0)).intValue(), Order.ASCENDING)
//                        .map(row->row.toString().replace(",","\t")).writeAsText("output/update_before/"+allTable+".txt", FileSystem.WriteMode.OVERWRITE);
                result.output(finish);

                String updateSql = getUpdateSql(colNames, allTable, cols);
                if (cols.length > 0) {
                    List<Integer> updateCols = getUpdateCols(colNames, cols);
                    List<Integer> whereCols = getJoinCols(cols, colNames);
                    if (!StringUtils.isNullOrWhitespaceOnly(updateSql) || updateResult != null) {

                        int[] updateSqlTypes = getUpdateSqlTypes(columns.get("COL_CLASS"), updateCols, whereCols);
                        JdbcOutputFormat updateFinish = JdbcOutputFormat.buildJdbcOutputFormat()
                                .setDrivername("org.postgresql.Driver")
                                .setDBUrl(newDatabaseUrl)
                                .setUsername(newDatabaseUsername)
                                .setPassword(newDatabasePassword)
                                .setQuery(updateSql)
                                .setSqlTypes(updateSqlTypes)
                                .finish();
                        DataSet<Row> updateData = updateResult
                                .map(u -> {
                                    Row row = new Row(u.getArity());
                                    for (int i = 0; i < updateCols.size(); i++) {
                                        row.setField(i, u.getField(updateCols.get(i)));
                                    }
                                    for (int i = 0; i < whereCols.size(); i++) {
                                        row.setField(i + updateCols.size(), u.getField(whereCols.get(i)));
                                    }
                                    return row;
                                });
                        updateData.output(updateFinish);
                    }
                }


            } else {
                result.output(finish);
            }


        }


        // 执行任务
        env.execute(PostgresDB1toDB2App.class.getName() + System.currentTimeMillis());

    }


    private static List<Integer> getJoinCols(String[] cols, List<String> colNames) {
        List<Integer> joinCols = new ArrayList<>(10);
        for (int i = 0; i < cols.length; i++) {
            for (int j = 0; j < colNames.size(); j++) {
                if (cols[i].equalsIgnoreCase(colNames.get(j))) {
                    joinCols.add(j);
                    break;
                }
            }
        }
        return joinCols;
    }

    private static List<Integer> getUpdateCols(List<String> colNames, String[] cols) {
        List<Integer> updateCols = new ArrayList<>(20);
        for (int i = 0; i < colNames.size(); i++) {
            boolean isNotFind = true;
            for (int j = 0; j < cols.length; j++) {
                if (colNames.get(i).equalsIgnoreCase(cols[j])) {
                    isNotFind = false;
                    break;
                }
            }
            if (isNotFind) {
                updateCols.add(i);
            }
        }
        return updateCols;
    }

    private static String getUpdateSql(List<String> colNames, String allTable, String[] cols) {
        StringBuilder sb = new StringBuilder();
        List<String> whereCols = Arrays.asList(cols);
        if (whereCols.size() > 0) {
            sb.append("UPDATE  ");
            sb.append(allTable);
            sb.append(" SET ");
            String setSql = colNames.stream().filter(col -> !whereCols.contains(col)).map(col -> col + " = ? ").reduce((a, b) -> a + ", " + b).orElse(null);
            sb.append(setSql);
            sb.append(" WHERE ");
            String whereStr = whereCols.stream().map(col -> col + " = ? ").reduce((a, b) -> a + " AND " + b).orElse(null);
            sb.append(whereStr);
        }
        return sb.toString();
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


    private static RowTypeInfo getRowTypeInfo(Map<String, List<String>> columns) {
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
                    typeInformationList.add(SqlTimeTypeInfo.TIMESTAMP);
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


    private static int[] getUpdateSqlTypes(List<String> colClass, List<Integer> updateCols, List<Integer> whereCols) {
        int[] sqlTypes = new int[colClass.size()];
        for (int i = 0; i < updateCols.size(); i++) {
            String columnType = colClass.get(updateCols.get(i));
            setSqlType(columnType, sqlTypes, i);

        }
        for (int i = 0; i < whereCols.size(); i++) {
            String columnType = colClass.get(whereCols.get(i));
            setSqlType(columnType, sqlTypes, i + updateCols.size());
        }
        return sqlTypes;
    }

    private static void setSqlType(String columnType, int[] sqlTypes, int i) {
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

}