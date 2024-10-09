package com.lyj;

import com.lyj.util.ConfigLoader;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.jdbc.JdbcOutputFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.lyj.util.ConfigLoader.DB_PROFILE;
import static com.lyj.util.ConfigLoader.getDatabasePassword;
import static com.lyj.util.ConfigLoader.getDatabaseUrl;
import static com.lyj.util.ConfigLoader.getDatabaseUsername;
import static com.lyj.util.TableUtil.getColumns;
import static com.lyj.util.TableUtil.getInsertSql;
import static com.lyj.util.TableUtil.getMaxSeq;
import static com.lyj.util.TableUtil.getRowTypeInfo;
import static com.lyj.util.TableUtil.getSqlTypes;
import static org.apache.flink.api.common.typeinfo.Types.TUPLE;

/**
 * 品名转换
 * 参数：
 * --db_profile
 * dev43
 * --schema
 * xuexiaodingtest
 * --code_convert_file
 * C:\青果\黄信中要的数据\code_convert.txt
 * --right_result_file
 * C:\青果\黄信中要的数据\right.txt
 */
public class ConvertMashin00Code {

    private static final Logger logger = LoggerFactory.getLogger(ConvertMashin00Code.class);

    private final static String INSERT_TABLE_NAME = "u15_prod_db";
    private final static String MASHIN_TABLE_NAME = "mashin00";

    /**
     * 序列
     */
    private final static String SEQ_COL = "U15_レコード";
    /**
     * 品名コード
     */
    private final static String HCOHN1 = "HCOHN1";

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // 通过命令行参来选择配置文件

        String activeProfile = params.get(DB_PROFILE);

        // schema
        String schema = params.get("schema");

        String folderPath = params.get("code_convert_file");

        String resultPath = params.get("right_result_file");

        boolean checkParamsResult = checkParams(activeProfile, schema, folderPath, resultPath);
        if (!checkParamsResult) {
            logger.error("params demo : " +
                    "--db_profile dev43  \n" +
                    "--schema xuexiaodingtest  \n" +
                    "--code_convert_file C:\\青果\\黄信中要的数据\\code_convert.txt  \n" +
                    "--right_result_file C:\\青果\\黄信中要的数据\\right.txt");
            return;
        }

        ConfigLoader.loadConfiguration(activeProfile);

        // Set up the execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Set the parallelism to 1 to ensure all data goes to a single file
        env.setParallelism(1);

        env.getConfig().setGlobalJobParameters(params);


        //先读取数据库数据
        Map<String, List<String>> columns = getColumns(schema, MASHIN_TABLE_NAME);
        List<String> colNames = columns.get("COL_NAMES");
        int index = -1;
        Map<String, Integer> mashinColMap = new HashMap<>();
        for (int i = 0; i < colNames.size(); i++) {
            if (HCOHN1.equalsIgnoreCase(colNames.get(i))) {
                index = i;
            }
            mashinColMap.put(colNames.get(i), i);
        }
        if (index == -1) {
            logger.error("HCOHN1 on table not found!");
            return;
        }
        StringBuilder sbSql = new StringBuilder();
        String collStr = colNames.stream().map(u -> "\"" + u + "\"").reduce((s1, s2) -> s1 + "," + s2).orElse(null);
        sbSql.append("SELECT ").append(collStr).append(" FROM ").append(schema).append(".").append(MASHIN_TABLE_NAME).append(" where  dnohn1 = 'D1' ");
        logger.info("select sql is {}", sbSql);
        RowTypeInfo rowTypeInfo = getRowTypeInfo(columns);
        JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat().setDrivername("org.postgresql.Driver").setDBUrl(getDatabaseUrl()).setUsername(getDatabaseUsername()).setPassword(getDatabasePassword()).setQuery(sbSql.toString()).setRowTypeInfo(rowTypeInfo).finish();
        DataSet<Row> dataStream = env.createInput(jdbcInputFormat);

        //读取txt 数据，
        TypeInformation<?>[] csvTypes = {Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING};
        DataSource<String> stringDataSource = env.readTextFile(folderPath);
        DataSet<Tuple5> csvData = stringDataSource.map(s ->
                {
                    Tuple5 tuple5 = new Tuple5();
                    String[] split = s.split("\t");
                    if (split.length >= 5) {
                        for (int i = 0; i < split.length; i++) {
                            if (1 == i) {
                                String s1 = split[i];
                                String value = s1;
                                for (int j = 0; j < 6; j++) {
                                    if (j >= s1.length()) {
                                        value += 0;
                                    }
                                }
                                tuple5.setField(value, i);
                            } else tuple5.setField(split[i], i);
                        }
                    }
                    return tuple5;
                }
        ).returns(TUPLE(csvTypes));

        int maxSeq = getMaxSeq(schema, INSERT_TABLE_NAME, SEQ_COL);
        int finalIndex = index;

        JoinOperator<Row, Tuple5, Tuple3> resultData = dataStream.rightOuterJoin(csvData)
                .where(row -> row.getField(finalIndex).toString())
                .equalTo(3).with((row, tuple5) ->
                        {
                            Tuple3 tuple3 = new Tuple3<>();
                            if (row == null) {
                                tuple3.f0 = "right";
                                tuple3.f1 = new Row(rowTypeInfo.getArity());
                            } else {
                                tuple3.f0 = "insert";
                                tuple3.f1 = row;
                            }
                            tuple3.f2 = tuple5;
                            return tuple3;
                        }
                ).returns(TUPLE(Types.STRING, Types.ROW(rowTypeInfo.getFieldTypes()), TUPLE(csvTypes)));

        //写入未配对的数据
        DataSet<Tuple5> rightData = resultData.filter(tuple3 -> "right".equals(tuple3.f0)).map(tuple3 -> (Tuple5) tuple3.f2).returns(TUPLE(csvTypes));

        rightData.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

        //插入新增数据
        columns = getColumns(schema, INSERT_TABLE_NAME);
        RowTypeInfo insertRowTypeInfo = getRowTypeInfo(columns);
        TypeInformation<?>[] insertTypes = insertRowTypeInfo.getFieldTypes();
        DataSet<Row> insertData = resultData.filter(tuple3 -> "insert".equals(tuple3.f0)).map(new MapFunction<Tuple3, Row>() {
            private long count = 0;
            @Override
            public Row map(Tuple3 tuple3) {
                count++;
                long seqNum = maxSeq + count;
                Row insertData = new Row(9);
                Tuple5 tuple5 = (Tuple5) tuple3.f2;
                Row row = (Row) tuple3.f1;
                if ("21".equals(tuple5.f0)) {
                    insertData = convert(row, tuple5.f1.toString(), 4, mashinColMap, seqNum);
                } else if ("34".equals(tuple5.f0)) {
                    insertData = convert(row, tuple5.f1.toString(), 5, mashinColMap, seqNum);
                } else if ("35".equals(tuple5.f0)) {
                    insertData = convert(row, tuple5.f1.toString(), 6, mashinColMap, seqNum);
                } else if ("23".equals(tuple5.f0)) {
                    insertData = convert(row, tuple5.f1.toString(), 7, mashinColMap, seqNum);
                } else if ("22".equals(tuple5.f0)) {
                    insertData = convert(row, tuple5.f1.toString(), 8, mashinColMap, seqNum);
                }
                return insertData;
            }

        }).returns(Types.ROW(insertTypes));


        // 将数据写入 PostgreSQL 数据库
        colNames = columns.get("COL_NAMES");
        String insertSql = getInsertSql(colNames, schema, INSERT_TABLE_NAME);
        int[] sqlTypes = getSqlTypes(columns);
        JdbcOutputFormat finish = JdbcOutputFormat.buildJdbcOutputFormat().setDrivername("org.postgresql.Driver").setDBUrl(getDatabaseUrl()).setUsername(getDatabaseUsername()).setPassword(getDatabasePassword()).setQuery(insertSql).setSqlTypes(sqlTypes).finish();
        insertData.output(finish);

        env.execute(ConvertMashin00Code.class.getName() + System.currentTimeMillis());
    }

    private static boolean checkParams(String activeProfile, String schema, String folderPath, String resultPath) {
        if (activeProfile == null) {
            logger.error("db_profile is null!");
            return false;
        }
        if (schema == null) {
            logger.error("schema is null!");
            return false;
        }

        if (folderPath == null) {
            logger.error("code_convert_file is null!");
            return false;
        }
        File file = new File(folderPath);

        if (!file.exists()) {
            logger.error("code_convert_file is not exists");
            return false;
        }

        if (file.isDirectory()) {
            logger.error("code_convert_file is directory");
            return false;
        }

        if (resultPath == null) {
            logger.error("right_result_file is null!");
            return false;
        }
        File resultFile = new File(resultPath);

        if (resultFile.isDirectory()) {
            logger.error("resultFile is directory");
            return false;
        }
        return true;
    }

    private static Row convert(Row row, String value, int index, Map<String, Integer> mashinColMap, long maxSeq) {
        Row tuple9 = new Row(9);
        //U15_品名コード1 取mashin00的hcohn1 字段的值
        //U15_品名コード2 取mashin00的hcohn2 字段的值
        //U15_実品名	取mashin00的hnmhn1 字段的值
        tuple9.setField(0, new BigDecimal(maxSeq));
        Object hcohn1 = row.getField(mashinColMap.get("hcohn1"));
        if (hcohn1 != null) {
            tuple9.setField(1, new BigDecimal(hcohn1.toString()));
        }
        Object hcohn2 = row.getField(mashinColMap.get("hcohn2"));
        if (hcohn2 != null) {
            tuple9.setField(2, new BigDecimal(hcohn2.toString()));
        }
        Object hnmhn1 = row.getField(mashinColMap.get("hnmhn1"));
        if (hnmhn1 != null) {
            tuple9.setField(3, hnmhn1.toString());
        }
        if (value != null) {
            tuple9.setField(index, new BigDecimal(value));
        }
        return tuple9;
    }


}
