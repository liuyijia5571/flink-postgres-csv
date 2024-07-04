package com.lyj.util;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellValue;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Workbook;

public class ExcelUtil {

    // 获取单元格的实际值
    public static Object getCellValue(Cell cell) {
        Object cellValue = null;
        if (cell != null) {
            switch (cell.getCellType()) {
                case STRING:
                    cellValue = cell.getRichStringCellValue().getString();
                    break;
                case NUMERIC:
                    if (DateUtil.isCellDateFormatted(cell)) {
                        cellValue = cell.getDateCellValue();
                    } else {
                        cellValue = cell.getNumericCellValue();
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
        }else{
            cellValue = "";
        }

        return cellValue;
    }

    // 获取公式单元格的实际值
    public static Object getFormulaCellValue(Cell cell) {
        Object cellValue = null;
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
            cellValue = "";
        }
        return cellValue;
    }
}
