package com.lyj;

import com.lyj.util.ConfigLoader;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.DistinctOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

import static com.lyj.util.ConfigLoader.DB_PROFILE;
import static com.lyj.util.TableUtil.COL_CLASS;
import static com.lyj.util.TableUtil.COL_NAMES;
import static com.lyj.util.TableUtil.getColumns;
import static com.lyj.util.TableUtil.getFormattedDate;
import static com.lyj.util.TableUtil.insertDB;
import static com.lyj.util.TableUtil.setFieldValue;

/**
 * U12导入数据库
 * 参数
 * --db_profile
 * dev43
 * --input_file_path
 * C:\青果\客様返送データ確認\過去データ移行専用\対応表資料\\u12_prod_db_荷主コード対応表
 * --schema
 * xuexiaodingtest2
 * --truncate
 * true
 */
public class U12ProdDbPostGreSql {

    private static final Logger logger = LoggerFactory.getLogger(U12ProdDbPostGreSql.class);

    private static final String INSERT_TABLE_NAME = "u12_prod_db";

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        // 通过命令行参来选择配置文件

        String activeProfile = params.get(DB_PROFILE);

        // schema
        String schema = params.get("schema");

        String inputFilePath = params.get("input_file_path");

        boolean isTruncate = params.getBoolean("truncate", false);

        boolean checkParamsResult = checkParams(activeProfile, schema, inputFilePath);
        if (!checkParamsResult) {
            logger.error("params demo : " + "--db_profile dev43  \n" + "--input_file_path C:\\青果\\黄信中要的数据　\n" + "--schema xuexiaodingtest2 \n");
            return;
        }

        ConfigLoader.loadConfiguration(activeProfile);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        File folder = new File(inputFilePath);
        File[] files = folder.listFiles();
        DataSet<Row> allDataSet = null;

        Map<String, List<String>> columns = getColumns(schema, INSERT_TABLE_NAME, isTruncate, true);
        List<String> colNameList = columns.get(COL_NAMES);
        List<String> colClassList = columns.get(COL_CLASS);


        for (File file : files) {
            if (!file.isDirectory()) {
                String fileName = file.getName();
                String csvFilePath = inputFilePath + File.separator + fileName;
                DataSource<String> stringDataSource = env.readTextFile(csvFilePath);
                DataSet<Row> dataSet = stringDataSource.map(line -> {
                    int offset = 0;
                    if (colNameList.get(0).contains("レコード") || "seq_no".equalsIgnoreCase(colNameList.get(0))) {
                        offset = 1;
                    }
                    Row row = getRowInfoByFileName(fileName, colNameList.size(), offset);
                    String[] split = line.split(",");
                    row.setField(2 + offset, split[0]);
                    row.setField(3 + offset, split[1]);
                    return row;
                });
                if (allDataSet == null) {
                    allDataSet = dataSet;
                } else {
                    allDataSet = allDataSet.union(dataSet);
                }
            }
        }
        if (allDataSet != null) {
            DistinctOperator<Row> distinct = allDataSet.distinct(u -> u.toString());
            MapOperator<Row, Row> insetDataSet = distinct.map(new MapFunction<Row, Row>() {
                private int index;

                @Override
                public Row map(Row row) throws ParseException {
                    //set seq
                    int tableIndex = 0;
                    if (colNameList.get(0).contains("レコード") || "seq_no".equalsIgnoreCase(colNameList.get(0))) {
                        index++;
                        setFieldValue(row, 0, colClassList.get(0), String.valueOf(index), "0", INSERT_TABLE_NAME);
                        tableIndex = 1;
                    }

                    for (int i = tableIndex; i < colClassList.size(); i++) {
                        setFieldValue(row, i, colClassList.get(i), (String) row.getField(i), "0", INSERT_TABLE_NAME);
                    }
                    return row;
                }
            }).setParallelism(1);

            insertDB(schema, colNameList, INSERT_TABLE_NAME, columns, insetDataSet);
            env.execute(U12ProdDbPostGreSql.class.getName() + "_" + getFormattedDate());
        }
    }

    private static Row getRowInfoByFileName(String fileName, int size, int offset) {
        Row row = new Row(size);
        //U12_拠点	U12_支社コード
        //長野        	21  cho
        //市川        	35  ich
        //須坂        	22  suz
        //船橋        	34  fun
        //中野        	23  nak
        int shiShaCode = 1 + offset;
        if (fileName.toLowerCase().contains("cho")) {
            row.setField(offset, "長野");
            row.setField(shiShaCode, "21");
        } else if (fileName.toLowerCase().contains("fun")) {
            row.setField(offset, "船橋");
            row.setField(shiShaCode, "34");
        } else if (fileName.toLowerCase().contains("ich")) {
            row.setField(offset, "市川");
            row.setField(shiShaCode, "35");
        } else if (fileName.toLowerCase().contains("nak")) {
            row.setField(offset, "中野");
            row.setField(shiShaCode, "23");
        } else if (fileName.toLowerCase().contains("suz")) {
            row.setField(offset, "須坂");
            row.setField(shiShaCode, "22");
        } else {
            row.setField(offset, "未知");
            row.setField(shiShaCode, "99");
        }
        return row;
    }

    private static boolean checkParams(String activeProfile, String schema, String inputFilePath) {
        if (activeProfile == null) {
            logger.error("db_profile is null!");
            return false;
        }

        if (schema == null) {
            logger.error("schema is null!");
            return false;
        }

        if (inputFilePath == null) {
            logger.error("input_file_path is null!");
            return false;
        }

        File resultFile = new File(inputFilePath);
        if (!resultFile.isDirectory()) {
            logger.error("input_file_path is not directory");
            return false;
        }
        return true;
    }
}
