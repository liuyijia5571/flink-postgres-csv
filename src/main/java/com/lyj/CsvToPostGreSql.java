package com.lyj;

import com.lyj.util.ConfigLoader;
import org.apache.commons.lang.StringUtils;
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
import static com.lyj.util.TableUtil.CHARSET_NAME_31J;
import static com.lyj.util.TableUtil.COL_CLASS;
import static com.lyj.util.TableUtil.COL_NAMES;
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

        String tableName = parameterTool.get("table_name");

        String schema = parameterTool.get("schema");

        String dataFile = parameterTool.get("data_file");

        boolean isTruncate = parameterTool.getBoolean("is_truncate", false);

        if (!checkParams(dbProfile, schema, tableName, dataFile)) {
            return;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ConfigLoader.loadConfiguration(dbProfile);

        DataSet<String> dataSource = env.readTextFile(dataFile,CHARSET_NAME_31J);

        Map<String, List<String>> columns = getColumns(schema, tableName, isTruncate, false);
        List<String> colNames = columns.get(COL_NAMES);
        List<String> colClass = columns.get(COL_CLASS);

        DataSet<Row> insertData = dataSource.map(u -> {
            Row row = new Row(colNames.size());
            String[] split = u.split(",", -1);
            for (int i = 0; i < split.length; i++) {
                if (i < row.getArity()) {
                    String classStr = colClass.get(i);
                    String colValue = split[i];
                    if (classStr.contains("character")) {
                        if (colValue.startsWith("\"") && colValue.endsWith("\"")) {
                            colValue = colValue.substring(1, colValue.length()-1);
                        }
                    }
                    setFieldValue(row, i, classStr, StringUtils.strip(colValue), "0", tableName);
                } else {
                    logger.error("line is {}", u);
                    break;
                }
            }
            return row;
        });

        insertData.print();
        insertDB(schema, colNames, tableName, columns, insertData);

        env.execute(CsvToPostGreSql.class.getName());
    }

    private static boolean checkParams(String dbProfile, String schema, String tableName, String dataFile) {
        if (dbProfile == null) {
            logger.error("db_profile is null");
            return false;
        }
        if (schema == null) {
            logger.error("schema is null");
            return false;
        }
        if (tableName == null) {
            logger.error("table_name is null");
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