package com.lyj.util;

import org.apache.flink.types.Row;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static com.lyj.util.ExcelUtil.getCellValue;
import static com.lyj.util.TableUtil.COL_CLASS;
import static com.lyj.util.TableUtil.COL_NAMES;
import static com.lyj.util.TableUtil.getLocalDateTime;

public class ExcelReaderTask implements Callable<List<List<Row>>> {

    private static final Logger logger = LoggerFactory.getLogger(ExcelReaderTask.class);

    private final String excelFilePath;
    private final Map<String, List<String>> columns;

    public ExcelReaderTask(String filePath, Map<String, List<String>> columns) {
        this.excelFilePath = filePath;
        this.columns = columns;
    }

    @Override
    public List<List<Row>> call() throws Exception {
        List<List<Row>> data = new ArrayList<>();

        List<String> colClassLiSt = columns.get(COL_CLASS);
        List<String> colNameList = columns.get(COL_NAMES);
        // 使用 Apache POI 读取 Excel 文件
        InputStream inputStream = new FileInputStream(excelFilePath);
        Workbook workbook = WorkbookFactory.create(inputStream);

        // 读取 運賃日報 的数据
        List<Row> dataList = new ArrayList<>();
        Sheet daySheet = workbook.getSheet("運賃日報");
        if (daySheet == null) {
            logger.error("data sheet is null file path is {}", excelFilePath);
            data.add(new ArrayList<>());
            data.add(new ArrayList<>());
            return data;
        }

        boolean lastLine = false;
        for (int i = 0; i <= daySheet.getLastRowNum(); i++) {
            org.apache.poi.ss.usermodel.Row row = daySheet.getRow(i);
            boolean maShinCode = false;
            if (i >= 3) {
                if (row.getLastCellNum() >= colNameList.size()) {
                    Row dataRow = new Row(colNameList.size() - 1);
                    for (int j = 1; j < colNameList.size(); j++) {
                        String colClass = colClassLiSt.get(j);
                        String colName = colNameList.get(j);
                        if ("U16_発生年月日".equalsIgnoreCase(colName)) {
                            String dataValue = getCellValue(row.getCell(j + 1)).toString();
                            if (dataValue.isEmpty()) {
                                lastLine = true;
                                break;
                            }
                            LocalDateTime date = getLocalDateTime(dataValue, excelFilePath);
                            StringBuilder dateSb = new StringBuilder();
                            dateSb.append(date.getYear()).append(String.format("%02d", date.getMonth().getValue())).append(String.format("%02d", date.getDayOfMonth()));
                            dataRow.setField(j - 1, dateSb.toString());
                        } else if ("U16_品名コード".equalsIgnoreCase(colName)) {
                            //j ==5 rowIndex 4 ,""
                            dataRow.setField(j - 1, "");
                            maShinCode = true;
                        } else {
                            if (!maShinCode) {
                                //j ==1  rowIndex 0 ,2
                                //j ==2  rowIndex 1 ,3
                                //j ==3  rowIndex 2 ,4
                                //j ==4  rowIndex 3 ,5
                                String dataValue = getCellValue(row.getCell(j + 1)).toString();
                                dataRow.setField(j - 1, dataValue);
                            } else {
                                //j ==6 rowIndex 5 ,6
                                String dataValue = getCellValue(row.getCell(j)).toString();
                                if ("U16_数量".equalsIgnoreCase(colName)) {
                                    dataValue = dataValue.replace(".", ",");
                                }
                                dataRow.setField(j - 1, dataValue);
                            }
                        }
                    }
                    if (lastLine)
                        break;
                    dataList.add(dataRow);
                }
            } else {
                printTableHead(row, i, excelFilePath);
            }
        }

        // 读取 品目マスタ 的数据
        List<Row> maShinList = new ArrayList<>();
        Sheet maShinSheet = workbook.getSheet("品目マスタ");
        if (maShinSheet == null) {
            logger.error("maShin sheet is null file path is {}", excelFilePath);
            data.add(new ArrayList<>());
            data.add(new ArrayList<>());
            return data;
        }
        for (int i = 0; i <= maShinSheet.getLastRowNum(); i++) {
            org.apache.poi.ss.usermodel.Row row = maShinSheet.getRow(i);
            if (i >= 2) {
                Row maShinRow = new Row(2);
                if (row.getLastCellNum() >= 4) {
                    for (int j = 2; j < 4; j++) {
                        Cell cell = row.getCell(j);
                        maShinRow.setField(j - 2, cell.toString());
                    }
                }
                maShinList.add(maShinRow);
            } else {
                printTableHead(row, i, excelFilePath);
            }
        }
        // 关闭资源
        workbook.close();
        inputStream.close();

        if (dataList.isEmpty() || maShinList.isEmpty()) {
            logger.error("data is null file path is {}", excelFilePath);
        }
        logger.info("excelFilePath is {},data size is {}", excelFilePath, dataList.size());
        data.add(dataList);
        data.add(maShinList);
        return data;
    }

    private static void printTableHead(org.apache.poi.ss.usermodel.Row row, int i, String excelFilePath) {
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < row.getLastCellNum(); j++) {
            Cell cell = row.getCell(j);
            sb.append(cell);
            if (j < row.getLastCellNum() - 1) {
                sb.append("\t");
            }
        }
        if (sb.length() > 0)
            logger.debug("file path is {} ,i = {},table head row is ({})", excelFilePath, i, sb);
    }
}
