package com.lyj;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.lyj.util.ExcelUtil.getCellValue;
import static com.lyj.util.TableUtil.CHARSET_NAME_31J;

public class findNinCodeByMFile {

    public static void main(String[] args) throws Exception {

        String pattern = "^M\\d+\\.csv$";
        Pattern compiledPattern = Pattern.compile(pattern);
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String ninCodeFilePath = parameterTool.get("", "C:\\青果\\荷主");

        String excelFilePath = parameterTool.get("", "C:\\青果\\20240723.xlsx");

        InputStream inputStream = new FileInputStream(excelFilePath);
        Workbook workbook = WorkbookFactory.create(inputStream);

        Sheet dataSheet = workbook.getSheet("r26出荷者");
        if (dataSheet == null) {
            return;
        }

        List<String> ninCodeList = new ArrayList<>();
        for (int i = 1; i <= dataSheet.getLastRowNum(); i++) {
            Row row = dataSheet.getRow(i);
            String ninCode = getCellValue(row.getCell(1)).toString();
            String shiXiaCode = getCellValue(row.getCell(0)).toString();
            if (ninCode.length() < 8) {
                for (int j = 0; j <= 8 - ninCode.length(); j++) {
                    //00100000
                    ninCode = "0" + ninCode;
                }
            }
            ninCodeList.add(shiXiaCode + "\t" + ninCode);
        }

        // 关闭资源
        workbook.close();
        inputStream.close();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> ninCodeDs = env.fromCollection(ninCodeList);
        File file = new File(ninCodeFilePath);
        DataSet<String> allDataSet = null;
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (int i = 0; i < files.length; i++) {
                String fileName = files[i].getName();
                FilterOperator<String> leftJoinDs = null;
                FilterOperator<String> stringDataSource = env.readTextFile(ninCodeFilePath + File.separator + fileName, CHARSET_NAME_31J).filter(line -> !"".equals(line));
                // 创建 Pattern 对象

                Matcher matcher = compiledPattern.matcher(fileName);
                boolean matches = matcher.matches();
                if (matches) {
                    leftJoinDs = ninCodeDs.filter(u -> u.contains("21" + "\t"));
                } else if (fileName.contains("F.csv")) {
                    leftJoinDs = ninCodeDs.filter(u -> u.contains("34" + "\t"));
                } else if (fileName.contains("K.csv")) {
                    leftJoinDs = ninCodeDs.filter(u -> u.contains("35" + "\t"));
                } else if (fileName.contains("N.csv")) {
                    leftJoinDs = ninCodeDs.filter(u -> u.contains("23" + "\t"));
                } else if (fileName.contains("Z.csv")) {
                    leftJoinDs = ninCodeDs.filter(u -> u.contains("22" + "\t"));
                }
                if (leftJoinDs != null) {
                    DataSet<String> result = leftJoinDs.join(stringDataSource).where(u -> u).equalTo(u -> {
                        String[] split = u.split(",");
                        String col1 = split[0];
                        if (fileName.contains("M11")) {
                            col1 = split[3];
                        }
                        if (col1.length() < 8) {
                            for (int j = 0; j <= 8 - col1.length(); j++) {
                                //00100000
                                col1 = "0" + col1;
                            }
                        }
                        if (matches) {
                            return "21" + "\t" + col1;
                        } else if (fileName.contains("F.csv")) {
                            return "34" + "\t" + col1;
                        } else if (fileName.contains("K.csv")
                        ) {
                            return "35" + "\t" + col1;
                        } else if (fileName.contains("N.csv")
                        ) {
                            return "23" + "\t" + col1;
                        } else if (fileName.contains("Z.csv")
                        ) {
                            return "22" + "\t" + col1;
                        }
                        return col1;
                    }).with((row1, row2) -> fileName + "\t" + row1 + "\t" + row2);
                    if (allDataSet == null) {
                        allDataSet = result;
                    } else {
                        allDataSet = allDataSet.union(result);
                    }
                }


            }
        }

        if (allDataSet != null) {
            allDataSet.writeAsText("output/ninCode.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
            env.execute(findNinCodeByMFile.class.getName());
        }
    }

}
