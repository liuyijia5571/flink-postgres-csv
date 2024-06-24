package com.lyj;

import com.lyj.util.ConfigLoader;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.lyj.util.ConfigLoader.getDatabasePassword;
import static com.lyj.util.ConfigLoader.getDatabaseUrl;
import static com.lyj.util.ConfigLoader.getDatabaseUsername;
import static com.lyj.util.TableUtil.getColumns;

public class PostgresTxt1App {

    private static final Logger logger = LoggerFactory.getLogger(PostgresTxt1App.class);

    static Map<String, String> siksmMap = new HashMap<>();
    static List<String> subTable = new ArrayList<>();

    static {
        //长应
        //长野
        siksmMap.put("CHO", "21");
        // 須坂支社
        siksmMap.put("SUZ", "22");
        //中野支社
        siksmMap.put("NAK", "23");
        // 船桥
        siksmMap.put("FUN", "34");
        //市川支社
        siksmMap.put("ICH", "35")
        ;
        subTable.add("KANFIL20");
        subTable.add("TKIFILU0");
        subTable.add("TKIHANM0");
        subTable.add("TKITOKM0");
        subTable.add("SUMURI00");
        subTable.add("KAISYO00");

        //ながのファイム                          FAM                     71
        //长应 的数据按联合规则放到联合表里去
        //联合
//        上　田			                UED		10
//        松　本			                MAT		11
//        諏　訪			                SUW	            12
//        佐　久			                SAK		            13
//        県央青			                TAK		            33
//        流通Ｃ                                       NRC                      70

    }

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            logger.error("args.length  < 3");
            return;
        }



        ConfigLoader.loadConfiguration(args[0]);

        String folderPath = args[1];

        String schema = args[2];

        // Set up the execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Set the parallelism to 1 to ensure all data goes to a single file
        env.setParallelism(1);

        // Get all table names
        Map<String, String> tableNames = getAllTableNames();

        extracted(tableNames, schema, env, folderPath);
        env.execute(PostgresTxt1App.class.getName() + System.currentTimeMillis());

    }

    public static void extracted(Map<String, String> tableNames, String schema, ExecutionEnvironment env, String folderPath) throws Exception {
        // Process each table
        for (String tableName : tableNames.keySet()) {
            String code = tableNames.get(tableName);
            if (subTable.contains(tableName)) {
                String[] splitTable = code.split("\n");
                for (int i = 0; i < splitTable.length; i++) {
                    String[] splitWhere = splitTable[i].split("->");
                    Map<String, List<String>> columns = getColumns(schema, tableName);
                    String sql = getSelectSql(tableName, code, null, schema, splitWhere[0], columns);
                    // 配置 JDBC 输入格式
                    if (sql != null) {
                        String fileName = splitWhere[1].replace(".", "_").toUpperCase();
                        String filePath = folderPath + File.separator + fileName + ".txt";
                        saveFile(env, columns, sql, filePath);
                    }
                }
            } else {
                if ("1".equals(code)) {
                    Map<String, List<String>> columns = getColumns(schema, tableName);
                    String sql = getSelectSql(tableName, code, null, schema, null, columns);
                    if (sql != null) {
                        String filePath = folderPath + File.separator + "RENDAYALL_" + tableName + ".txt";
                        // 配置 JDBC 输入格式
                        saveFile(env, columns, sql, filePath);
                    }
                } else {
                    Map<String, List<String>> columns = getColumns(schema, tableName);
                    for (String siksmKey : siksmMap.keySet()) {
                        String sql = getSelectSql(tableName, code, siksmMap.get(siksmKey), schema, null, columns);
                        if (sql != null) {
                            if (tableName.equalsIgnoreCase("KANFILD0")) {
                                String filePath = folderPath + File.separator + "RENBAK" + siksmKey + "_" + tableName + ".txt";
                                saveFile(env, columns, sql, filePath);
                            } else {
                                String filePath = folderPath + File.separator + "RENDAY" + siksmKey + "_" + tableName + ".txt";
                                saveFile(env, columns, sql, filePath);
                            }
                        }
                    }
                }
            }
        }

    }

    private static void saveFile(ExecutionEnvironment env, Map<String, List<String>> columns, String sql, String filePath) throws Exception {
        RowTypeInfo rowTypeInfo = getRowTypeInfo(columns);
        JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername("org.postgresql.Driver")
                .setDBUrl(getDatabaseUrl())
                .setUsername(getDatabaseUsername())
                .setPassword(getDatabasePassword())
                .setQuery(sql)
                .setRowTypeInfo(rowTypeInfo)
                .finish();
        DataSource<Row> dataStream = env.createInput(jdbcInputFormat);
        DataSet<String> result = dataStream.map(row -> {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < row.getArity(); ++i) {
                if (i > 0) {
                    sb.append("|");
                }
                sb.append(StringUtils.arrayAwareToString(row.getField(i)));
            }
            return sb.toString();
        });

        result.writeAsText(filePath, FileSystem.WriteMode.OVERWRITE);

    }

    private static Map<String, String> getAllTableNames() {
        Map<String, String> tableNames = new HashMap<>();
        tableNames.put("SUMURI00", "SIKUR1 = '21' and pdyur1 = '2022'->worcho22.sumuri00\n" +
                "SIKUR1 = '22' and pdyur1 = '2022'->worsuz22.sumuri00\n" +
                "SIKUR1 = '23' and pdyur1 = '2022'->wornak22.sumuri00\n" +
                "SIKUR1 = '34' and pdyur1 = '2022'->worfun22.sumuri00\n" +
                "SIKUR1 = '35' and pdyur1 = '2022'->worich22.sumuri00\n" +
                "SIKUR1 = '21' and pdyur1 = '2023'->worcho23.sumuri00\n" +
                "SIKUR1 = '22' and pdyur1 = '2023'->worsuz23.sumuri00\n" +
                "SIKUR1 = '23' and pdyur1 = '2023'->wornak23.sumuri00\n" +
                "SIKUR1 = '34' and pdyur1 = '2023'->worfun23.sumuri00\n" +
                "SIKUR1 = '35' and pdyur1 = '2023'->worich23.sumuri00");
        tableNames.put("TKIFILU0", "SIKHA1 = '21' and tdaha1 >= 20220101 and tdaha1 <= 20221231->worcho22.tkifilu0\n" +
                "SIKHA1 = '22' and tdaha1 >= 20220101 and tdaha1 <= 20221231->worsuz22.tkifilu0\n" +
                "SIKHA1 = '23' and tdaha1 >= 20220101 and tdaha1 <= 20221231->wornak22.tkifilu0\n" +
                "SIKHA1 = '34' and tdaha1 >= 20220101 and tdaha1 <= 20221231->worfun22.tkifilu0\n" +
                "SIKHA1 = '35' and tdaha1 >= 20220101 and tdaha1 <= 20221231->worich22.tkifilu0\n" +
                "SIKHA1 = '21' and tdaha1 >= 20230101 and tdaha1 <= 20231231->worcho23.tkifilu0\n" +
                "SIKHA1 = '22' and tdaha1 >= 20230101 and tdaha1 <= 20231231->worsuz23.tkifilu0\n" +
                "SIKHA1 = '23' and tdaha1 >= 20230101 and tdaha1 <= 20231231->wornak23.tkifilu0\n" +
                "SIKHA1 = '34' and tdaha1 >= 20230101 and tdaha1 <= 20231231->worfun23.tkifilu0\n" +
                "SIKHA1 = '35' and tdaha1 >= 20230101 and tdaha1 <= 20231231->worich23.tkifilu0");
        tableNames.put("TKIHANM0", "SIKUK1 = '21' and nenuk1 = '2022'->worcho22.tkihanm0\n" +
                "SIKUK1 = '22' and nenuk1 = '2022'->worsuz22.tkihanm0\n" +
                "SIKUK1 = '23' and nenuk1 = '2022'->wornak22.tkihanm0\n" +
                "SIKUK1 = '34' and nenuk1 = '2022'->worfun22.tkihanm0\n" +
                "SIKUK1 = '35' and nenuk1 = '2022'->worich22.tkihanm0\n" +
                "SIKUK1 = '21' and nenuk1 = '2023'->worcho23.tkihanm0\n" +
                "SIKUK1 = '22' and nenuk1 = '2023'->worsuz23.tkihanm0\n" +
                "SIKUK1 = '23' and nenuk1 = '2023'->wornak23.tkihanm0\n" +
                "SIKUK1 = '34' and nenuk1 = '2023'->worfun23.tkihanm0\n" +
                "SIKUK1 = '35' and nenuk1 = '2023'->worich23.tkihanm0\n" +
                "SIKUK1 = '21' and nenuk1 = '2024'->rendaycho.tkihanm0\n" +
                "SIKUK1 = '22' and nenuk1 = '2024'->rendaysuz.tkihanm0\n" +
                "SIKUK1 = '23' and nenuk1 = '2024'->rendaynak.tkihanm0\n" +
                "SIKUK1 = '34' and nenuk1 = '2024'->rendayfun.tkihanm0\n" +
                "SIKUK1 = '35' and nenuk1 = '2024'->rendayich.tkihanm0");
//        tableNames.put("TKITOKH0","SIKTH1");
        tableNames.put("TKITOKM0", "SIKTK1 = '21' and nentk1 = '2022'->worcho22.tkitokm0\n" +
                "SIKTK1 = '22' and nentk1 = '2022'->worsuz22.tkitokm0\n" +
                "SIKTK1 = '23' and nentk1 = '2022'->wornak22.tkitokm0\n" +
                "SIKTK1 = '34' and nentk1 = '2022'->worfun22.tkitokm0\n" +
                "SIKTK1 = '35' and nentk1 = '2022'->worich22.tkitokm0\n" +
                "SIKTK1 = '21' and nentk1 = '2023'->worcho23.tkitokm0\n" +
                "SIKTK1 = '22' and nentk1 = '2023'->worsuz23.tkitokm0\n" +
                "SIKTK1 = '23' and nentk1 = '2023'->wornak23.tkitokm0\n" +
                "SIKTK1 = '34' and nentk1 = '2023'->worfun23.tkitokm0\n" +
                "SIKTK1 = '35' and nentk1 = '2023'->worich23.tkitokm0\n" +
                "SIKTK1 = '21' and nentk1 = '2024'->rendaycho.tkitokm0\n" +
                "SIKTK1 = '22' and nentk1 = '2024'->rendaysuz.tkitokm0\n" +
                "SIKTK1 = '23' and nentk1 = '2024'->rendaynak.tkitokm0\n" +
                "SIKTK1 = '34' and nentk1 = '2024'->rendayfun.tkitokm0\n" +
                "SIKTK1 = '35' and nentk1 = '2024'->rendayich.tkitokm0");
        tableNames.put("KAISYO00", "SIKKR1 = '21' and TDAKR1 >= 220101 and TDAKR1 <= 221231->worcho22.kaisyo00\n" +
                "SIKKR1 = '22' and TDAKR1 >= 220101 and TDAKR1 <= 221231->worsuz22.kaisyo00\n" +
                "SIKKR1 = '23' and TDAKR1 >= 220101 and TDAKR1 <= 221231->wornak22.kaisyo00\n" +
                "SIKKR1 = '34' and TDAKR1 >= 220101 and TDAKR1 <= 221231->worfun22.kaisyo00\n" +
                "SIKKR1 = '35' and TDAKR1 >= 220101 and TDAKR1 <= 221231->worich22.kaisyo00\n" +
                "SIKKR1 = '21' and TDAKR1 >= 230101 and TDAKR1 <= 231231->worcho23.kaisyo00\n" +
                "SIKKR1 = '22' and TDAKR1 >= 230101 and TDAKR1 <= 231231->worsuz23.kaisyo00\n" +
                "SIKKR1 = '23' and TDAKR1 >= 230101 and TDAKR1 <= 231231->wornak23.kaisyo00\n" +
                "SIKKR1 = '34' and TDAKR1 >= 230101 and TDAKR1 <= 231231->worfun23.kaisyo00\n" +
                "SIKKR1 = '35' and TDAKR1 >= 230101 and TDAKR1 <= 231231->worich23.kaisyo00");
        tableNames.put("KANFIL20", "SIKSS1 = '21' and UDASS1 >= 202201 and UDASS1 <= 202212->worcho22.kanfil20\n" +
                "SIKSS1 = '22' and UDASS1 >= 202201 and UDASS1 <= 202212->worsuz22.kanfil20\n" +
                "SIKSS1 = '23' and UDASS1 >= 202201 and UDASS1 <= 202212->wornak22.kanfil20\n" +
                "SIKSS1 = '34' and UDASS1 >= 202201 and UDASS1 <= 202212->worfun22.kanfil20\n" +
                "SIKSS1 = '35' and UDASS1 >= 202201 and UDASS1 <= 202212->worich22.kanfil20\n" +
                "SIKSS1 = '21' and UDASS1 >= 202301 and UDASS1 <= 202312->worcho23.kanfil20\n" +
                "SIKSS1 = '22' and UDASS1 >= 202301 and UDASS1 <= 202312->worsuz23.kanfil20\n" +
                "SIKSS1 = '23' and UDASS1 >= 202301 and UDASS1 <= 202312->wornak23.kanfil20\n" +
                "SIKSS1 = '34' and UDASS1 >= 202301 and UDASS1 <= 202312->worfun23.kanfil20\n" +
                "SIKSS1 = '35' and UDASS1 >= 202301 and UDASS1 <= 202312->worich23.kanfil20\n" );

        return tableNames;
    }

    private static String getSelectSql(String tableName, String code, String sikmValue, String schema, String whereStr, Map<String, List<String>> columns) {
        StringBuilder sbSql = new StringBuilder();
        List<String> colNames = columns.get("COL_NAMES");
        if (colNames.isEmpty()) {
            return null;
        } else {
            String collStr = colNames.stream().map(u -> "\"" + u + "\"").reduce((s1, s2) -> s1 + "," + s2).orElse(null);
            sbSql.append("SELECT ").append(collStr).append(" FROM ").append(schema).append(".")
                    .append(tableName);
            if (null != sikmValue)
                sbSql.append(" where ").append(code).append(" = '").append(sikmValue).append("'");
            if (whereStr != null) {
                sbSql.append(" where ").append(whereStr);
            }
            logger.info("execute sql is {}", sbSql);
            return sbSql.toString();
        }
    }

    public static RowTypeInfo getRowTypeInfo(Map<String, List<String>> columns)  {
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
}