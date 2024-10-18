package com.lyj;

import com.lyj.util.ConfigLoader;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.lyj.util.ConfigLoader.DB_PROFILE;
import static com.lyj.util.ConfigLoader.getDatabasePassword;
import static com.lyj.util.ConfigLoader.getDatabaseUrl;
import static com.lyj.util.ConfigLoader.getDatabaseUsername;
import static com.lyj.util.TableUtil.COL_CLASS;
import static com.lyj.util.TableUtil.getFormattedDate;
import static com.lyj.util.TableUtil.CHARSET_NAME_31J;
import static com.lyj.util.TableUtil.COL_NAMES;
import static com.lyj.util.TableUtil.getColumns;
import static com.lyj.util.TableUtil.getRowTypeInfo;
import static com.lyj.util.TableUtil.getTypeInformationArr;

/**
 * 比较数据文件
 * 参数：
 * --db_profile
 * dev43
 * --new_db_profile
 * dev43
 * --old_table
 * u12_prod_db
 * --new_table
 * u12_prod_db
 * --old_schema
 * xuexiaodingtest
 * --new_schema
 * xuexiaodingtest2
 */
public class CompareDB {

    private static final Logger logger = LoggerFactory.getLogger(CompareDB.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        // 通过命令行参来选择配置文件

        String activeProfile = params.get(DB_PROFILE);


        String newActiveProfile = params.get("new_db_profile");


        // old_schema
        String oldSchema = params.get("old_schema");

        // new_schema
        String newSchema = params.get("new_schema", oldSchema);


        //oldTable
        String oldTable = params.get("old_table");

        //newTable
        String newTable = params.get("new_table", oldTable);

        //result_file
        String resultFile = params.get("result_file", "output/" + oldSchema + "." + oldTable + "_result.csv");

        boolean checkParamsResult = checkParams(activeProfile, oldSchema, newSchema, oldTable, newTable);
        if (!checkParamsResult) {
            logger.error("params demo : " + "--db_profile dev43  \n" + "--new_db_profile dev43  \n" + "--old_table u12_prod_db　\n" + "--new_table u12_prod_db　\n" + "--old_schema xuexiaodingtest \n" + "--new_schema xuexiaodingtest2 \n");
            return;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ConfigLoader.loadConfiguration(activeProfile);
        Map<String, List<String>> oldColumns = getColumns(oldSchema, oldTable, false, true);
        List<String> oldColNames = oldColumns.get(COL_NAMES);
        List<String> colClass = oldColumns.get(COL_CLASS);
        RowTypeInfo oldRowTypeInfo = getRowTypeInfo(oldColumns);
        StringBuilder selectOldSql = new StringBuilder();
        String oldCollStr = oldColNames.stream().map(u -> "\"" + u + "\"").reduce((s1, s2) -> s1 + "," + s2).orElse(null);
        selectOldSql.append("SELECT ").append(oldCollStr).append(" FROM ").append(oldSchema).append(".").append(oldTable);

        logger.debug("collNames is {}", oldColNames.toString().replace(",","\t"));
        TypeInformation[] typeInformationArr = getTypeInformationArr(oldColumns);

        JdbcInputFormat oldFinish = JdbcInputFormat.buildJdbcInputFormat().setDrivername("org.postgresql.Driver").setDBUrl(getDatabaseUrl()).setUsername(getDatabaseUsername()).setPassword(getDatabasePassword()).setQuery(selectOldSql.toString()).setRowTypeInfo(oldRowTypeInfo).finish();
        DataSource<Row> oldDataSet = env.createInput(oldFinish);

        ConfigLoader.loadConfiguration(newActiveProfile);
        Map<String, List<String>> newColumns = getColumns(newSchema, newTable, false, true);
        List<String> newColNames = newColumns.get(COL_NAMES);
        RowTypeInfo newRowTypeInfo = getRowTypeInfo(newColumns);
        StringBuilder selectNewSql = new StringBuilder();
        String newCollStr = newColNames.stream().map(u -> "\"" + u + "\"").reduce((s1, s2) -> s1 + "," + s2).orElse(null);
        selectNewSql.append("SELECT ").append(newCollStr).append(" FROM ").append(newSchema).append(".").append(newTable);
        JdbcInputFormat newFinish = JdbcInputFormat.buildJdbcInputFormat().setDrivername("org.postgresql.Driver").setDBUrl(getDatabaseUrl()).setUsername(getDatabaseUsername()).setPassword(getDatabasePassword()).setQuery(selectNewSql.toString()).setRowTypeInfo(newRowTypeInfo).finish();
        DataSource<Row> newDataSet = env.createInput(newFinish);

        DataSet<Tuple2<String, Row>> resultDS = oldDataSet.fullOuterJoin(newDataSet)
                .where(row -> getKeyBy(row, newColNames))
                .equalTo(row -> getKeyBy(row, newColNames))
                .with((row1, row2) -> {
                    Tuple2<String, Row> tuple2 = new Tuple2<>();
                    if (row1 == null) {
                        tuple2.f0 = "new";
                        tuple2.f1 = row2;
                    }
                    if (row2 == null) {
                        tuple2.f0 = "old";
                        tuple2.f1 = row1;
                    }
                    if (row1 != null && row2 != null) {
                        tuple2.f0 = "same";
                        tuple2.f1 = row1;
                    }
                    return tuple2;
                }).returns(Types.TUPLE(Types.STRING, Types.ROW(typeInformationArr)));

        // 创建 CsvOutputFormat
        CsvOutputFormat<Tuple2<String, Row>> csvOutputFormat = new CsvOutputFormat<>(new Path(resultFile));
        csvOutputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
        csvOutputFormat.setCharsetName(CHARSET_NAME_31J); // 指定编码格式

        // 将 DataSet 写入 CSV 文件
        TypeInformation<?>[] rowTypes = new TypeInformation[colClass.size()];
        for (int i = 0; i < colClass.size(); i++) {
            rowTypes[i] = BasicTypeInfo.STRING_TYPE_INFO;
        }

        DataSet<Tuple2<String, Row>> resultStrDs = resultDS.map(u -> {
            Row row = u.f1;
            for (int i = 0; i < row.getArity(); i++) {
                String classStr = colClass.get(i);
                Object colValue = row.getField(i);
                if (colValue != null) {
                    if (classStr.contains("character")) {
                        row.setField(i, "\"" + colValue + "\"");
                    } else {
                        row.setField(i, colValue.toString());
                    }
                }
            }
            return u;
        }).returns(Types.TUPLE(Types.STRING, Types.ROW(rowTypes)));

        resultStrDs.output(csvOutputFormat).setParallelism(1);

        env.execute(CompareDB.class.getName() + "_" + getFormattedDate());
    }

    private static String getKeyBy(Row row, List<String> newColNames) {
        int offset = 0;
        String colNameTop = newColNames.get(0);
        //序列不比较
        if (colNameTop.contains("レコード") || "seq_no".equalsIgnoreCase(colNameTop)) {
            offset = 1;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = offset; i < row.getArity(); ++i) {
            String colName = newColNames.get(i);
            //通配字段不比较
            if (colName.equalsIgnoreCase("insert_pro_id") ||
                    colName.equalsIgnoreCase("upd_user_id") ||
                    colName.equalsIgnoreCase("upd_job_id") ||
                    colName.equalsIgnoreCase("upd_pro_id") ||
                    colName.equalsIgnoreCase("insert_user_id") ||
                    colName.equalsIgnoreCase("partition_flag") ||
                    colName.equalsIgnoreCase("upd_sys_date") ||
                    colName.equalsIgnoreCase("insert_sys_date") ||
                    colName.equalsIgnoreCase("insert_job_id")
            ) {
                continue;
            }
            if (i > offset) {
                sb.append("|");
            }
            sb.append(StringUtils.arrayAwareToString(row.getField(i)));
        }
//        logger.info("row is {}", row);
//        logger.info("join sb is {}", sb);
        return sb.toString();
    }

    private static boolean checkParams(String activeProfile, String oldSchema, String newSchema, String oldTable, String newTable) {

        if (activeProfile == null) {
            logger.error("db_profile is null!");
            return false;
        }

        if (oldSchema == null) {
            logger.error("old_schema is null!");
            return false;
        }
        if (newSchema == null) {
            logger.error("new_schema is null!");
            return false;
        }

        if (oldTable == null) {
            logger.error("old_table is null!");
            return false;
        }
        if (newTable == null) {
            logger.error("new_table is null!");
            return false;
        }

        return true;
    }
}
