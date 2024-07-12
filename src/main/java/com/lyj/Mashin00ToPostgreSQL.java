package com.lyj;

import com.lyj.util.ConfigLoader;
import com.lyj.util.TableUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.lyj.util.ConfigLoader.DB_PROFILE;
import static com.lyj.util.ExcelReaderTask.printTableHead;
import static com.lyj.util.ExcelUtil.getCellValue;
import static com.lyj.util.TableUtil.NOW_DATE;
import static com.lyj.util.TableUtil.getColumns;
import static com.lyj.util.TableUtil.getConnectionOptions;
import static com.lyj.util.TableUtil.getInsertSql;
import static com.lyj.util.TableUtil.jdbcExecutionOptions;

/**
 * data导入postgresql
 * 品名导入数据库
 * 品名マスタ_宮川→ジェミニ→RC作業後_20240622追加.xlsx
 * shellName = レンゴーに紐づく品番 join shellName = レンゴーに紐づかない品番
 * 操作之前需要把レンゴーに紐づかない品番 B列copy到文本，再copy 回去，因为有没办法解析的表达式
 */
public class Mashin00ToPostgreSQL {

    private static final Logger logger = LoggerFactory.getLogger(Mashin00ToPostgreSQL.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // 通过命令行参来选择配置文件
        String activeProfile = params.get(DB_PROFILE);

        String inputFile = params.get("input_file");

        boolean checkParamsResult = checkParams(activeProfile, inputFile);

        if (!checkParamsResult) {
            logger.error("params demo : " + "--db_profile dev43 " + "--input_file C:\\flink\\input\\品名マスタ_宮川→ジェミニ→RC作業後_20240622追加.xlsx　 \n"
            );
            return;
        }

        ConfigLoader.loadConfiguration(activeProfile);

        String schemaName = "renmasall";
        String tableName = "mashin00";

        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行数
        env.setParallelism(1);

        // 使用 Apache POI 读取 Excel 文件
        InputStream inputStream = new FileInputStream(inputFile);
        Workbook workbook = WorkbookFactory.create(inputStream);

        // sheet レンゴーに紐づく品番
        List<String> data1RowList = getData1RowList(workbook, inputFile);
        logger.info("sheet name is レンゴーに紐づく品番 ，data size is {}", data1RowList.size());

        //sheet レンゴーに紐づかない品番
        List<String> data2RowList = getData2RowList(workbook, inputFile);
        logger.info("sheet name is レンゴーに紐づかない品番 ，data size is {}", data2RowList.size());

        // 关闭资源
        workbook.close();
        inputStream.close();


        DataStreamSource<String> data1Ds = env.fromCollection(data1RowList);
        DataStreamSource<String> data2Ds = env.fromCollection(data2RowList);
        DataStream<String> allDataSet = data1Ds.union(data2Ds);
        // 将数据写入 PostgreSQL 数据库
        Map<String, List<String>> columns = getColumns(schemaName, tableName, true);
        List<String> colNames = columns.get("COL_NAMES");
        List<String> colClasses = columns.get("COL_CLASS");
        String insertSql = getInsertSql(colNames, schemaName, tableName);
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("SELECT ").append(colNames.stream().reduce((s1, s2) -> s1 + "," + s2).orElse(null)).append(" from ").append(schemaName).append(".").append(tableName).append(" order by seq_no;\n");
        logger.info("insertSql is {}", insertSql);
        logger.info("selectSql is {}", stringBuilder);
        allDataSet.addSink(JdbcSink.sink(insertSql, (ps, t) -> {
            // 对每个数据元素进行写入操作
            String[] datas = t.split("\t", -1);
            for (int i = 0; i < colNames.size(); i++) {
                String colName = colNames.get(i);
                String colClass = colClasses.get(i);
                //处理共同字段
                if (i < datas.length) {
                    setPsData(i + 1, colName, colClass, datas[i], ps, tableName);
                } else {
                    if (colName.equalsIgnoreCase("insert_job_id") || colName.equalsIgnoreCase("insert_pro_id") || colName.equalsIgnoreCase("upd_user_id") || colName.equalsIgnoreCase("upd_job_id") || colName.equalsIgnoreCase("upd_pro_id")) {
                        setPsData(i + 1, colName, colClass, "", ps, tableName);
                    } else if (colName.equalsIgnoreCase("insert_user_id") || colName.equalsIgnoreCase("partition_flag")) {
                        setPsData(i + 1, colName, colClass, tableName.toUpperCase(), ps, tableName);
                    } else if (colName.equalsIgnoreCase("upd_sys_date") || colName.equalsIgnoreCase("insert_sys_date")) {
                        setPsData(i + 1, colName, colClass, NOW_DATE, ps, tableName);
                    }
                }
            }
        }, jdbcExecutionOptions, getConnectionOptions()));

        env.execute("Flink Mashin00ToPostgreSQL job");
    }

    private static List<String> getData2RowList(Workbook workbook, String inputFile) throws Exception {
        // 读取 数据
        Sheet data1 = workbook.getSheet("レンゴーに紐づかない品番");

        List<String> data1RowList = new ArrayList<>();
        if (data1 == null) {
            logger.error("レンゴーに紐づかない品番 sheet is null ,File is {}", inputFile);
            return data1RowList;
        }
        for (int i = 0; i <= data1.getLastRowNum(); i++) {
            Row row = data1.getRow(i);
            logger.debug("sheet name is レンゴーに紐づかない品番 rowId is {}", i);
            if (i >= 3) {
                String data = getCellValue(row.getCell(11)).toString();
                if ("*".equals(data)) {
                    break;
                }
                StringBuilder line = new StringBuilder();
                for (int j = 11; j <= 43; j++) {
                    String cellValue = getCellValue(row.getCell(j)).toString();
                    if (j == 11 && cellValue.isEmpty()) {
                        cellValue = "HN";
                    } else if (j == 12 && cellValue.isEmpty()) {
                        cellValue = "D1";
                    }
                    line.append(cellValue);
                    if (j < 43) {
                        line.append("\t");
                    }
                }
                data1RowList.add(line.toString());
                if (i % 1000 == 0) {
                    logger.info("sheet name is レンゴーに紐づかない品番 rowId is {}", i);
                }
            } else {
                printTableHead(row, i, inputFile);
            }
        }
        return data1RowList;
    }

    public static List<String> getData1RowList(Workbook workbook, String inputFile) throws Exception {
        // 读取 数据
        Sheet data1 = workbook.getSheet("レンゴーに紐づく品番");

        List<String> data1RowList = new ArrayList<>();
        if (data1 == null) {
            logger.error("レンゴーに紐づく品番 sheet is null ,File is {}", inputFile);
            return data1RowList;
        }
        for (int i = 0; i <= data1.getLastRowNum(); i++) {
            Row row = data1.getRow(i);

            if (i >= 2) {
                String data = getCellValue(row.getCell(14)).toString();
                if (data.isEmpty()) {
                    break;
                }
                StringBuilder line = new StringBuilder();
                for (int j = 14; j <= 46; j++) {
                    line.append(getCellValue(row.getCell(j)));
                    if (j < 46) {
                        line.append("\t");
                    }
                }
                data1RowList.add(line.toString());
            } else {
                printTableHead(row, i, inputFile);
            }
        }
        return data1RowList;
    }

    private static boolean checkParams(String activeProfile, String inputPath) {
        if (activeProfile == null) {
            logger.error("db_profile is null!");
            return false;
        }

        if (inputPath == null) {
            logger.error("input_file is null!");
            return false;
        }

        File txtFile = new File(inputPath);
        if (!txtFile.isFile()) {
            logger.error("input_file is not file");
            return false;
        }
        return true;
    }

    private static void setPsData(int parameterIndex, String colName, String colClass, String dataValue, PreparedStatement ps, String tableName) throws SQLException {
        //SKNHN1 前面补0
        if (colName.equalsIgnoreCase("SKNHN1")) {
            StringBuilder result = new StringBuilder();
            int length = dataValue.length();
            for (int i = 0; i < 48 - length; i++) {
                result.append(0);
            }
            result.append(dataValue);
            dataValue = result.toString();
        }
        TableUtil.setPsData(parameterIndex, colName, colClass, dataValue, ps, tableName);
    }
}