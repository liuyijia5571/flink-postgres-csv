package com.lyj;

import com.lyj.util.ConfigLoader;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;

import static com.lyj.util.ConfigLoader.DB_PROFILE;
import static com.lyj.util.TableUtil.COL_CLASS;
import static com.lyj.util.TableUtil.COL_NAMES;
import static com.lyj.util.TableUtil.NOW_DATE;
import static com.lyj.util.TableUtil.getColumns;
import static com.lyj.util.TableUtil.insertDB;
import static com.lyj.util.TableUtil.setFieldValue;

/**
 * csv 到postgresql 数据库
 */
public class CsvToPostGreSql {

    private static final Logger logger = LoggerFactory.getLogger(SourceToFileNamePostgresql.class);


    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String dbProfile = parameterTool.get(DB_PROFILE);

        String tableNameStr = parameterTool.get("table_name");

        String dataFile = parameterTool.get("data_file");

        boolean isTruncate = parameterTool.getBoolean("is_truncate", false);

        if (!checkParams(dbProfile, tableNameStr, dataFile)) {
            return;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ConfigLoader.loadConfiguration(dbProfile);

        DataSet<String> dataSource = env.readTextFile(dataFile);
//        DataSet<String> dataSource = env.readTextFile(dataFile, CHARSET_NAME_31J);
        String[] split1 = tableNameStr.split("\\.");
        String schema = split1[0];
        String tableName = split1[1];

        Map<String, List<String>> columns = getColumns(schema, tableName, isTruncate, true);
        List<String> colNames = columns.get(COL_NAMES);
        List<String> colClass = columns.get(COL_CLASS);

        DataSet<Row> insertData = dataSource.map(u -> {
            Row row = new Row(colNames.size());
            String[] split = u.split(",", -1);
            int offset = 0;
            if ("\"C\"".equals(split[0]) || "\"U\"".equals(split[0])) {
                offset = 1;
            }

            for (int i = 0; i < split.length - offset; i++) {
                if (i < row.getArity()) {
                    String classStr = colClass.get(i);
                    String colValue = split[i + offset];
//                    if (classStr.contains("character")) {
                        if (colValue.startsWith("\"") && colValue.endsWith("\"")) {
                            colValue = colValue.substring(1, colValue.length() - 1);
                        }
//                    }
                    setFieldValue(row, i, classStr, colValue, "0", tableName);
                } else {
                    logger.error("line is {}", u);
                    break;
                }
            }
            if (row.getArity() > split.length - offset) {
                for (int i = split.length - offset; i < row.getArity(); i++) {
                    String colClassName = colClass.get(i);
                    String colName = colNames.get(i);
                    if (colName.equalsIgnoreCase("insert_pro_id") ||
                            colName.equalsIgnoreCase("upd_user_id") ||
                            colName.equalsIgnoreCase("upd_job_id") ||
                            colName.equalsIgnoreCase("upd_pro_id")
                    ) {
                        setFieldValue(row, i, colClassName, "", "0", tableName);
                    } else if (colName.equalsIgnoreCase("insert_user_id") ||
                            colName.equalsIgnoreCase("partition_flag")
                    ) {
                        setFieldValue(row, i, colClassName, tableName.toUpperCase(), "0", tableName);
                    } else if (colName.equalsIgnoreCase("upd_sys_date") ||
                            colName.equalsIgnoreCase("insert_sys_date")) {
                        setFieldValue(row, i, colClassName, NOW_DATE,"0" , tableName);
                    } else if (colName.equalsIgnoreCase("insert_job_id")) {
                        setFieldValue(row, i, colClassName,  CsvToPostGreSql.class.getSimpleName(), "0" , tableName);
                    }
                }
            }
            return row;
        });

        insertData.print();
        insertDB(schema, colNames, tableName, columns, insertData);

        env.execute(CsvToPostGreSql.class.getName());
    }

    private static boolean checkParams(String dbProfile,  String tableName, String dataFile) {
        if (dbProfile == null) {
            logger.error("db_profile is null");
            return false;
        }

        if (tableName == null) {
            logger.error("table_name is null");
            return false;
        }
        if (!tableName.contains(".")) {
            logger.error("table_name is not contains .");
            return false;
        }
        if (dataFile == null) {
            logger.error("data_file is null");
            return false;
        }

        File file = new File(dataFile);
        if (!file.isFile()) {
            logger.error("data_file is not file");
            return false;
        }
        return true;
    }
}