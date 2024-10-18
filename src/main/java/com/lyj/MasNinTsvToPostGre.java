package com.lyj;


import com.lyj.util.ConfigLoader;
import com.lyj.util.ExcelUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.lyj.util.ConfigLoader.DB_PROFILE;
import static com.lyj.util.ExcelUtil.getCellValue;
import static com.lyj.util.TableUtil.FILE_NAME;
import static com.lyj.util.TableUtil.NOW_DATE;
import static com.lyj.util.TableUtil.getColumns;
import static com.lyj.util.TableUtil.getInsertSql;
import static com.lyj.util.TableUtil.insertDB;
import static com.lyj.util.TableUtil.setFieldValue;

/**
 * 配对市村町code
 * 参数
 * --dev
 * dev43
 * --txt_path
 * C:\青果\Data_Result\sql\data
 */
public class MasNinTsvToPostGre {


    private static final Logger logger = LoggerFactory.getLogger(MasNinTsvToPostGre.class);
    private static int indexKcosi1;
    private static int indexShcni1;

    public static void main(String[] args) throws Exception {
        // 通过命令行参来选择配置文件

        final ParameterTool params = ParameterTool.fromArgs(args);

        String activeProfile = params.get(DB_PROFILE);

        // CSV 文件路径
        String folderPath = params.get("txt_path");

        boolean checkParamsResult = checkParams(activeProfile, folderPath);
        if (!checkParamsResult) {
            logger.error("params demo : " + "--db_profile dev43  \n" + "--txt_path C:\\青果\\Data_Result\\sql\\data  \n" + "--is_truncate true  ");
            return;
        }

        String config = params.get("config_excel", "input/config/input.xlsx");

        //是否清空表
        boolean isTruncate = params.getBoolean("is_truncate", false);

        logger.info("truncate is {}", isTruncate);

        ConfigLoader.loadConfiguration(activeProfile);

        // 创建流执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 使用 Apache POI 读取 inputFile Excel 文件
        InputStream inputStream = new FileInputStream(config);
        Workbook workbook = WorkbookFactory.create(inputStream);

        // sheet 都道県市町村名
        String sheetName1 = "都道県市町村名";
        final List<org.apache.flink.types.Row> data1RowList = getDataRowList(workbook, sheetName1, config, 1, "A:J");
        logger.info("sheet1 name is {} ，data size is {}", sheetName1, data1RowList.size());

        //sheet df_dummyPost
        String sheetName2 = "df_dummyPost";
        List<org.apache.flink.types.Row> data2RowList = getDataRowList(workbook, sheetName2, config, 1, "A:I");
        logger.info("sheet2 name is {} ，data size is {}", sheetName2, data2RowList.size());

        // 关闭资源
        workbook.close();
        inputStream.close();

        DataSet<org.apache.flink.types.Row> dummyPostDs = env.fromCollection(data2RowList);

        DataSet<org.apache.flink.types.Row> configDs = dummyPostDs.flatMap(new RichFlatMapFunction<org.apache.flink.types.Row, org.apache.flink.types.Row>() {
            private final Map<String, Pattern> patternCache = new ConcurrentHashMap<>();

            @Override
            public void flatMap(org.apache.flink.types.Row row, Collector<org.apache.flink.types.Row> out) {
                String sityousonName = (String) row.getField(row.getArity() - 1);
                int bestMatchLength = 0;

                org.apache.flink.types.Row resultRow = new org.apache.flink.types.Row(6);
                for (org.apache.flink.types.Row line : data1RowList) {
                    String regex = (String) line.getField(line.getArity() - 1);
                    if (!StringUtils.isNullOrWhitespaceOnly(regex)) {
                        Pattern pattern = patternCache.computeIfAbsent(regex, Pattern::compile);
                        Matcher matcher = pattern.matcher(sityousonName);
                        // 检查是否匹配
                        if (matcher.find()) {
                            String match = matcher.group();
                            if (match.length() > bestMatchLength) {
                                bestMatchLength = match.length();
                                //IYUNI1
                                resultRow.setField(0, row.getField(1));
                                //ken-code
                                resultRow.setField(1, String.valueOf(Long.parseLong((String) line.getField(1))));
                                //sityouson-code
                                resultRow.setField(2, line.getField(2));

                                resultRow.setField(3, sityousonName);

                                resultRow.setField(4, regex);
                                resultRow.setField(5, bestMatchLength);
//                                logger.debug("匹配成功: {}，regex is {} sityousonName is {},bestMatchLength is {}", matcher.group(), regex, sityousonName, bestMatchLength);
                            }
                        }
                    }
                }
                if (resultRow.getField(0) != null) out.collect(resultRow);
            }
        }).returns(Types.ROW(Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.INT)).setParallelism(1);

        configDs.distinct(0, 1, 2).writeAsText("output/config.txt", FileSystem.WriteMode.OVERWRITE);
        DataSet<org.apache.flink.types.Row> resultConfigDs = configDs.distinct(u -> (String) u.getField(0));
//
        File folder = new File(folderPath);
        StringBuffer sb = new StringBuffer();
        if (folder.exists()) {

            File[] files = folder.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (!file.isDirectory()) {
                        String fileName = file.getName();
                        String csvFilePath = folderPath + File.separator + fileName;

                        String[] tableNameArr = fileName.split("_");
                        if (tableNameArr.length > 1) {
                            String schemaName = tableNameArr[0].toLowerCase();
                            String tableName = tableNameArr[1].split("\\.")[0].toLowerCase();

                            Map<String, List<String>> columns;
                            columns = getColumns(schemaName, tableName, isTruncate,true);
                            List<String> colClass = columns.get("COL_CLASS");
                            List<String> colNames = columns.get("COL_NAMES");
                            if (colNames.isEmpty()) continue;
                            String insertSql = getInsertSql(colNames, schemaName, tableName);

                            sb.append("SELECT ").append(colNames.stream().reduce((s1, s2) -> s1 + "," + s2).orElse(null)).append(" from ").append(schemaName).append(".").append(tableName).append(" order by seq_no ;\n");

                            logger.info("insertSql is {}", insertSql);
                            // 读取 CSV 文件并创建 DataStream
                            DataSet<String> csvDataStream = env.readTextFile(csvFilePath);
                            int index = -1;
                            for (int i = 0; i < colNames.size(); i++) {
                                if ("iyuni1".equalsIgnoreCase(colNames.get(i))) {
                                    index = i;
                                }
                                if ("kenni1".equalsIgnoreCase(colNames.get(i))) {
                                    indexKcosi1 = i;
                                }

                                if ("shcni1".equalsIgnoreCase(colNames.get(i))) {
                                    indexShcni1 = i;
                                }
                            }
                            if (index == -1) {
                                logger.error("iyuni1 on table not found!");
                                return;
                            }
                            int finalIndex = index;
                            JoinOperator<String, org.apache.flink.types.Row, String> insertDataDs = csvDataStream.leftOuterJoin(resultConfigDs).where(u -> u.split("\t")[finalIndex].trim()).equalTo(u -> (String) u.getField(0)).with((line, second) -> {
                                String[] split = line.split("\t");
                                split[indexKcosi1] = "0";
                                split[indexShcni1] = "0";
                                if (second != null) {
                                    split[indexKcosi1] = (String) second.getField(1);
                                    split[indexShcni1] = (String) second.getField(2);
                                }
                                return Arrays.stream(split).reduce((a, b) -> a + "\t" + b).get();
                            }).returns(Types.STRING);

                            MapOperator<String, org.apache.flink.types.Row> insertData = insertDataDs.map(line -> {

                                String[] split = line.split("\t", -1);
                                org.apache.flink.types.Row row = new org.apache.flink.types.Row(colNames.size());
                                String numericScale = "0";
                                for (int i = 0; i < colNames.size(); i++) {
                                    String colName = colNames.get(i);
                                    if (i > split.length) {
                                        //处理共同字段
                                        if (colName.equalsIgnoreCase("insert_job_id") || colName.equalsIgnoreCase("insert_pro_id") || colName.equalsIgnoreCase("upd_user_id") || colName.equalsIgnoreCase("upd_job_id") || colName.equalsIgnoreCase("upd_pro_id")) {
                                            setFieldValue(row, i, colClass.get(i), "", numericScale, tableName);
                                        } else if (colName.equalsIgnoreCase("insert_user_id") || colName.equalsIgnoreCase("partition_flag")) {
                                            setFieldValue(row, i, colClass.get(i), tableName, numericScale, tableName);
                                        } else if (colName.equalsIgnoreCase("upd_sys_date") || colName.equalsIgnoreCase("insert_sys_date")) {
                                            setFieldValue(row, i, colClass.get(i), NOW_DATE, numericScale, tableName);
                                        } else if (colName.equalsIgnoreCase(FILE_NAME)) {
                                            setFieldValue(row, i, colClass.get(i), fileName, numericScale, tableName);
                                        } else {
                                            setFieldValue(row, i, colClass.get(i), "", numericScale, tableName);
                                        }
                                    } else {
                                        if ("hksni1".equalsIgnoreCase(colName) && "masnin00".equalsIgnoreCase(tableName)) {
                                            if (split[i].length() > 1) {
                                                setFieldValue(row, i, colClass.get(i), split[i].substring(0, 1), numericScale, tableName);
                                            } else {
                                                setFieldValue(row, i, colClass.get(i), split[i], numericScale, tableName);
                                            }
                                        } else if (colName.equalsIgnoreCase(FILE_NAME)) {
                                            setFieldValue(row, i, colClass.get(i), fileName, numericScale, tableName);
                                        } else if (i == split.length) {
                                            setFieldValue(row, i, colClass.get(i), "", numericScale, tableName);
                                        } else {
                                            setFieldValue(row, i, colClass.get(i), split[i], numericScale, tableName);
                                        }
                                    }
                                }
                                return row;
                            });
                            insertDB(schemaName, colNames, tableName, columns, insertData);

                        }

                    }
                }
            }
        }
        logger.info("select sql is {}", sb);

        // 执行流处理
        logger.info("Flink MasNinTsvToPostGre job started");

        env.execute(MasNinTsvToPostGre.class.getName() + System.currentTimeMillis());

        logger.info("Flink MasNinTsvToPostGre job finished");
    }

    public static List<org.apache.flink.types.Row> getDataRowList(Workbook workbook, String sheetName, String inputFile, int skipRows, String parseCols) throws Exception {
        Sheet data = workbook.getSheet(sheetName);

        List<org.apache.flink.types.Row> dataRowList = new ArrayList<>();
        if (data == null) {
            logger.error("{} sheet is null", sheetName);
            return dataRowList;
        }
        for (int i = 0; i <= data.getLastRowNum(); i++) {
            Row row = data.getRow(i);

            if (i >= skipRows) {
                String[] split = parseCols.split(":");
                int end = ExcelUtil.columnToIndex(split[1]);
                int start = ExcelUtil.columnToIndex(split[0]);
                org.apache.flink.types.Row dataRow = new org.apache.flink.types.Row(end - start + 1);
                for (int j = start; j <= end; j++) {
                    dataRow.setField(j - start, getCellValue(row.getCell(j)));
                }
                dataRowList.add(dataRow);
            }
        }
        return dataRowList;
    }

    private static boolean checkParams(String activeProfile, String folderPath) {
        if (activeProfile == null) {
            logger.error("db_profile is null!");
            return false;
        }

        if (folderPath == null) {
            logger.error("txt_path is null!");
            return false;
        }
        File resultFile = new File(folderPath);

        if (!resultFile.isDirectory()) {
            logger.error("txt_path is not directory");
            return false;
        }
        return true;
    }
}
