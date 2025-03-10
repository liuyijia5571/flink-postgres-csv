package com.lyj;

import com.lyj.util.ConfigLoader;
import com.lyj.util.ExcelUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.types.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.lyj.util.ConfigLoader.DB_PROFILE;
import static com.lyj.util.ExcelReaderTask.printTableHead;
import static com.lyj.util.ExcelUtil.getCellValue;
import static com.lyj.util.TableUtil.COL_CLASS;
import static com.lyj.util.TableUtil.COL_NAMES;
import static com.lyj.util.TableUtil.NUMERIC_SCALE;
import static com.lyj.util.TableUtil.getColumns;
import static com.lyj.util.TableUtil.getFormattedDate;
import static com.lyj.util.TableUtil.getMaxSeq;
import static com.lyj.util.TableUtil.insertDB;
import static com.lyj.util.TableUtil.setFieldValue;

/**
 * u15 导入数据
 * 参数
 * --db_profile
 * dev43
 * --input_file
 * C:\flink\input\品名マスタ_宮川→ジェミニ→RC作業後_20240622追加.xlsx
 * --schema
 * xuexiaodingtest
 * --truncate
 * true
 */
public class U15ToPostGreSql {

    private static final Logger logger = LoggerFactory.getLogger(U15ToPostGreSql.class);

    private final static String INSERT_TABLE_NAME = "u15_prod_db";

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool params = ParameterTool.fromArgs(args);
        // 通过命令行参来选择配置文件

        String activeProfile = params.get(DB_PROFILE);

        // schema
        String schema = params.get("schema");

        String inputFile = params.get("input_file");

        boolean checkParamsResult = checkParams(activeProfile, schema, inputFile);

        boolean isTruncate = params.getBoolean("truncate", false);

        if (!checkParamsResult) {
            logger.error("params demo : " + "--db_profile dev43  \n" + "--input_file C:\\flink\\input\\品名マスタ_宮川→ジェミニ→RC作業後_20240622追加.xlsx　\n" + "--schema xuexiaodingtest2 \n");
            return;
        }


        ConfigLoader.loadConfiguration(activeProfile);

        Map<String, List<String>> columns = getColumns(schema, INSERT_TABLE_NAME, isTruncate, true);
        List<String> colNameList = columns.get(COL_NAMES);

        // 使用 Apache POI 读取 Excel 文件
        InputStream inputStream = new FileInputStream(inputFile);
        Workbook workbook = WorkbookFactory.create(inputStream);

        // sheet レンゴーに紐づく品番
        List<Row> data1RowList = getData1RowList(workbook, inputFile, colNameList);
        logger.info("sheet name is レンゴーに紐づく品番 ，data size is {}", data1RowList.size());

        //sheet レンゴーに紐づかない品番
        List<Row> data2RowList = getData2RowList(workbook, inputFile, colNameList);
        logger.info("sheet name is レンゴーに紐づかない品番 ，data size is {}", data2RowList.size());


        //sheet M05
        List<Row> M05DataList = getDataRowList(workbook, inputFile, "M05");
        logger.info("sheet name is M05，data size is {}", M05DataList.size());

        //sheet M05F
        List<Row> M05FDataList = getDataRowList(workbook, inputFile, "M05F");
        logger.info("sheet name is M05F，data size is {}", M05FDataList.size());

        //sheet M05F
        List<Row> M05KDataList = getDataRowList(workbook, inputFile, "M05K");
        logger.info("sheet name is M05K，data size is {}", M05KDataList.size());

        //sheet M05N
        List<Row> M05NDataList = getDataRowList(workbook, inputFile, "M05N");
        logger.info("sheet name is M05F，data size is {}", M05NDataList.size());

        //sheet M05Z
        List<Row> M05ZDataList = getDataRowList(workbook, inputFile, "M05Z");
        logger.info("sheet name is M05K，data size is {}", M05ZDataList.size());


        // 关闭资源
        workbook.close();
        inputStream.close();

        DataSet<Row> data1Ds = env.fromCollection(data1RowList);
        DataSet<Row> data2Ds = env.fromCollection(data2RowList);

        TypeInformation[] rowTypes = getTypeInformationStr(getColumns(schema, INSERT_TABLE_NAME, isTruncate, false));
        //补充字段
        DataSet<Row> allDataSet = data1Ds.union(data2Ds).map(u -> {
            for (int i = 4; i <= 8; i++) {
                if (StringUtils.isEmpty((String) u.getField(i))) {
                    u.setField(i, u.getField(1));
                }
            }
            return u;
        }).returns(Types.ROW(rowTypes));

        DataSet<Row> mo5Ds = env.fromCollection(M05DataList);
        DataSet<Row> mo5FDs = env.fromCollection(M05FDataList);
        DataSet<Row> m05KDs = env.fromCollection(M05KDataList);
        DataSet<Row> m05Nds = env.fromCollection(M05NDataList);
        DataSet<Row> m05ZDs = env.fromCollection(M05ZDataList);


        DataSet<Row> addM05Ds = getRowDataSet(mo5Ds, allDataSet, 4, rowTypes);
        DataSet<Row> addM05FDs = getRowDataSet(mo5FDs, allDataSet, 5, rowTypes);
        DataSet<Row> addM05KDs = getRowDataSet(m05KDs, allDataSet, 6, rowTypes);
        DataSet<Row> addM05NDs = getRowDataSet(m05Nds, allDataSet, 7, rowTypes);
        DataSet<Row> addM05ZDs = getRowDataSet(m05ZDs, allDataSet, 8, rowTypes);

        allDataSet = allDataSet.union(addM05Ds).union(addM05FDs).union(addM05KDs).union(addM05NDs).union(addM05ZDs);

        List<String> colNames = columns.get(COL_NAMES);
        List<String> colClass = columns.get(COL_CLASS);
        List<String> numericScaleList = columns.get(NUMERIC_SCALE);

        //拼接需要插入数据库的Row对象
        int maxSeq = getMaxSeq(schema, INSERT_TABLE_NAME);
        logger.info("table name is {} ,count is {}", INSERT_TABLE_NAME, maxSeq);

        MapOperator<Row, Row> insertData = allDataSet.map(new MapFunction<Row, Row>() {
            private int index = 0;

            @Override
            public Row map(Row row) throws ParseException {
                index++;
                Row newRow = new Row(colNames.size());
                //set seq
                int tableIndex = 0;
                if (colNames.get(0).contains("レコード") || "seq_no".equalsIgnoreCase(colNames.get(0))) {
                    int seqNo = maxSeq + index;
                    setFieldValue(newRow, 0, colClass.get(0), String.valueOf(seqNo), numericScaleList.get(0), INSERT_TABLE_NAME);
                    tableIndex = 1;
                }
                for (int i = tableIndex; i < colNames.size(); i++) {
                    setFieldValue(newRow, i, colClass.get(i), (String) row.getField(i), numericScaleList.get(i), INSERT_TABLE_NAME);
                }
                return newRow;
            }
        }).setParallelism(1);
        insertDB(schema, colNameList, INSERT_TABLE_NAME, columns, insertData);
        String formattedDate = getFormattedDate();
        env.execute(U15ToPostGreSql.class.getName() + "_" + formattedDate);
    }

    private static TypeInformation[] getTypeInformationStr(Map<String, List<String>> columns) {
        List<String> colClass = columns.get("COL_CLASS");
        TypeInformation<?>[] rowTypes = new TypeInformation[colClass.size()];
        for (int i = 0; i < colClass.size(); i++) {
            rowTypes[i] = BasicTypeInfo.STRING_TYPE_INFO;
        }
        return rowTypes;
    }

    private static DataSet<Row> getRowDataSet (DataSet < Row > mo5Ds, DataSet < Row > allDataSet,
        int index, TypeInformation[] types){
            DataSet<Row> addM05Ds = mo5Ds.join(allDataSet)
                    .where(u -> {
                        String str = u.getField(0).toString();
                        if (StringUtils.isNotBlank(str)) {
                            double numericValue = Double.parseDouble(str);
                            // 检查是否是整数
                            if (numericValue == (long) numericValue) {
                                return new BigDecimal((long) numericValue);
                            } else {
                                return new BigDecimal(str);
                            }
                        }
                        return new BigDecimal(-2);
                    })
                    .equalTo(u -> {
                        String str = u.getField(index).toString();
                        if (StringUtils.isNotBlank(str)) {
                            double numericValue = Double.parseDouble(str);
                            // 检查是否是整数
                            if (numericValue == (long) numericValue) {
                                return new BigDecimal((long) numericValue);
                            } else {
                                return new BigDecimal(str);
                            }
                        }
                        return new BigDecimal(-1);
                    })
                    .with((FlatJoinFunction<Row, Row, Row>) (first, second, out) -> {
                        if(!first.getField(1).equals(second.getField(index))){
                            double numericValue = Double.parseDouble((String) first.getField(1));
                            if (numericValue == (long) numericValue) {
                                String field = String.valueOf((long) numericValue);
                                if (!field.equals(second.getField(index))) {
                                    for (int i = 4; i <= 8; i++) {
                                        second.setField(i, "0");
                                    }
                                    second.setField(index, first.getField(1));
                                    out.collect(second);
                                }
                            }
                        }
                    }).returns(Types.ROW(types));

            return addM05Ds;
        }

    private static List<Row> getDataRowList (Workbook workbook, String inputFile, String sheetName) throws Exception
        {
            List<Row> dataRowList = new ArrayList<>();
            Sheet data = workbook.getSheet(sheetName);
            if (data == null) {
                logger.error(" {} sheet is null ,file is {}", sheetName, inputFile);
                return dataRowList;
            }
            for (int i = 0; i <= data.getLastRowNum(); i++) {
                org.apache.poi.ss.usermodel.Row row = data.getRow(i);
                if (i >= 2) {
                    Row dataRow = new Row(2);
                    String oldCode = getCellValue(row.getCell(0)).toString();
                    if (oldCode.isEmpty()) {
                        break;
                    }
                    String newCode = getCellValue(row.getCell(4)).toString();
                    dataRow.setField(0, oldCode);
                    dataRow.setField(1, newCode);
                    dataRowList.add(dataRow);
                } else {
                    printTableHead(row, i, inputFile);
                }
            }
            return dataRowList;
        }

        /**
         * sheet レンゴーに紐づく品番
         * C列和D列是联合的CODE
         * F列是 実品名
         * J-N列是长印的CODE
         */
        private static List<Row> getData1RowList (Workbook workbook, String inputFile, List < String > colNameList) throws
        Exception {
            // 读取 数据
            Sheet data1 = workbook.getSheet("レンゴーに紐づく品番");

            List<Row> data1RowList = new ArrayList<>();
            if (data1 == null) {
                logger.error("レンゴーに紐づく品番 sheet is null ,File is {}", inputFile);
                return data1RowList;
            }
            boolean lastLine = false;
            for (int i = 0; i <= data1.getLastRowNum(); i++) {
                org.apache.poi.ss.usermodel.Row row = data1.getRow(i);
                int maShinCodeIndex = ExcelUtil.columnToIndex("D");
                int longCodeIndex = ExcelUtil.columnToIndex("K");

                if (i >= 2) {
                    Row dataRow = new Row(colNameList.size());
                    for (int j = 0; j < colNameList.size(); j++) {
                        String colName = colNameList.get(j);
                        if (colName.contains("品名コード")) {
                            // C列和D列是联合的CODE
                            String data = getCellValue(row.getCell(maShinCodeIndex)).toString();
                            if ("*".equals(data)) {
                                lastLine = true;
                                break;
                            }
                            dataRow.setField(j, data);
                            maShinCodeIndex++;
                        } else if (colName.contains("実品名")) {
                            // F列是 実品名
                            String data = getCellValue(row.getCell(5)).toString();
                            dataRow.setField(j, data);
                        } else if (colName.contains("長印M05")) {
                            //J-N列是长印的CODE
                            String data = String.valueOf(getCellValue(row.getCell(longCodeIndex)));
//                        if(!StringUtils.isNotBlank(data)){
//                            // 取A列的值
//                            data = String.valueOf(getCellValue(row.getCell(0)));
//                        }
                            dataRow.setField(j, data);
                            longCodeIndex++;
                        }
                    }
                    if (lastLine)
                        break;
                    data1RowList.add(dataRow);
                } else {
                    printTableHead(row, i, inputFile);
                }
            }
            return data1RowList;
        }

        /**
         * sheet レンゴーに紐づかない品番
         * N列和O列是联合的CODE
         * G-K列是长印的CODE
         * Q列是 実品名
         */
        private static List<Row> getData2RowList (Workbook workbook, String inputFile, List < String > colNameList) throws
        Exception {
            List<Row> data2RowList = new ArrayList<>();
            Sheet data2 = workbook.getSheet("レンゴーに紐づかない品番");
            if (data2 == null) {
                logger.error("レンゴーに紐づかない品番 sheet is null ,file is {}", inputFile);
                return data2RowList;
            }

            boolean lastLine = false;
            for (int i = 0; i <= data2.getLastRowNum(); i++) {
                org.apache.poi.ss.usermodel.Row row = data2.getRow(i);
                if (row == null) {
                    break;
                }
                if ("*".equals(row.getCell(0))) {
                    break;
                }
                int maShinCodeIndex = ExcelUtil.columnToIndex("N");
                int longCodeIndex = ExcelUtil.columnToIndex("G");
                if (i >= 3) {
                    Row dataRow = new Row(colNameList.size());
                    for (int j = 0; j < colNameList.size(); j++) {
                        String colName = colNameList.get(j);
                        if (colName.contains("品名コード")) {
                            // N列和O列是联合的CODE 从0开始 N:13列，O:14列
                            String data = getCellValue(row.getCell(maShinCodeIndex)).toString();
                            if ("*".equals(data)) {
                                lastLine = true;
                                break;
                            }
                            dataRow.setField(j, data);
                            maShinCodeIndex++;
                        } else if (colName.contains("実品名")) {
                            // Q列是 実品名 从0开始 Q列:16列
                            String data = getCellValue(row.getCell(16)).toString();
                            dataRow.setField(j, data);
                        } else if (colName.contains("長印M05")) {
                            //G-K列是长印的CODE 6到10
                            String data = String.valueOf(getCellValue(row.getCell(longCodeIndex)));
                            dataRow.setField(j, data);
                            longCodeIndex++;
                        }
                    }
                    if (lastLine) {
                        logger.info("sheet name is レンゴーに紐づかない品番 rowId is {} ,lastLine is {}", i, lastLine);
                        break;
                    }
                    data2RowList.add(dataRow);
                    if (i % 1000 == 0) {
                        logger.info("sheet name is レンゴーに紐づかない品番 rowId is {}", i);
                    }
                } else {
                    printTableHead(row, i, inputFile);
                }
            }
            return data2RowList;
        }

        private static boolean checkParams (String activeProfile, String schema, String inputPath){
            if (activeProfile == null) {
                logger.error("db_profile is null!");
                return false;
            }

            if (schema == null) {
                logger.error("schema is null!");
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
}
