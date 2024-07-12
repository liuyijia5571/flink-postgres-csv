package com.lyj.util;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellValue;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Workbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.regex.Pattern;

public class ExcelUtil {

    private static final Logger logger = LoggerFactory.getLogger(ExcelUtil.class);

    // 获取单元格的实际值
    public static Object getCellValue(Cell cell) throws Exception {
        Object cellValue;
        if (cell != null) {
            switch (cell.getCellType()) {
                case STRING:
                    cellValue = cell.getRichStringCellValue().getString();
                    break;
                case NUMERIC:
                    if (DateUtil.isCellDateFormatted(cell)) {
                        cellValue = cell.getDateCellValue();
                    } else {
                        double numericValue = cell.getNumericCellValue();
                        String stringValue = Double.toString(numericValue);

                        // 检查是否是整数
                        if (numericValue == (long) numericValue) {
                            cellValue = (long) numericValue;
                        } else if (isScientificNotation(stringValue)) {
                            cellValue = formatNumericValue(numericValue);
                        } else {
                            cellValue = numericValue;
                        }

                    }
                    break;
                case BOOLEAN:
                    cellValue = cell.getBooleanCellValue();
                    break;
                case FORMULA:
                    // 获取公式的实际值
                    cellValue = getFormulaCellValue(cell);
                    break;
                case BLANK:
                    cellValue = ""; // 空白单元格
                    break;
                default:
                    cellValue = "";
            }
        } else {
            cellValue = "";
        }

        return cellValue;
    }

    // 获取公式单元格的实际值
    public static Object getFormulaCellValue(Cell cell) throws Exception {
        Object cellValue;
        try {
            Workbook workbook = cell.getSheet().getWorkbook();
            FormulaEvaluator formulaEvaluator = workbook.getCreationHelper().createFormulaEvaluator();
            CellValue cellEvaluatedValue = formulaEvaluator.evaluate(cell);
            switch (cellEvaluatedValue.getCellType()) {
                case STRING:
                    cellValue = cellEvaluatedValue.getStringValue();
                    break;
                case NUMERIC:
                    cellValue = cellEvaluatedValue.getNumberValue();
                    break;
                case BOOLEAN:
                    cellValue = cellEvaluatedValue.getBooleanValue();
                    break;
                default:
                    cellValue = "";
            }
        } catch (Exception e) {
            logger.error("Failed to evaluate cell: {}", cell.getAddress());
            logger.error("Cell value: {}", cell);
            e.printStackTrace();
            throw new Exception(e.getMessage());
        }
        return cellValue;
    }

    private static String formatNumericValue(double value) {
        DecimalFormat decimalFormat = new DecimalFormat("#.################");
        return decimalFormat.format(value);
    }

    // 检查字符串是否为科学计数法表示
    private static boolean isScientificNotation(String value) {
        Pattern scientificNotationPattern = Pattern.compile("[-+]?\\d*\\.?\\d+[eE][-+]?\\d+");
        return scientificNotationPattern.matcher(value).matches();
    }

    // 将 Excel 列标识符转换为从 0 开始的下标
    public static int columnToIndex(String column) {
        int index = 0;
        for (int i = 0; i < column.length(); i++) {
            char c = column.charAt(i);
            index = index * 26 + (c - 'A' + 1);
        }
        return index - 1; // 因为 Excel 列从 A 开始，所以需要减去 1 得到从 0 开始的下标
    }
}
