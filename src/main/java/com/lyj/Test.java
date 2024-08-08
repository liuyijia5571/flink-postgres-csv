package com.lyj;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.lyj.util.TableUtil.CHARSET_NAME_31J;

public class Test {


    public static void main(String[] args) throws Exception {

        String pattern = "^M\\d+\\.csv$";


        ParameterTool parameterTool = ParameterTool.fromArgs(args);


        String ninCodeFilePath = parameterTool.get("input_path", "C:\\青果\\荷主");

        String outputPath = parameterTool.get("output_path_file", "output/oldNinCode.txt");


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        File file = new File(ninCodeFilePath);
        DataSet<String> allDataSet = null;
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (int i = 0; i < files.length; i++) {
                String fileName = files[i].getName();
                FilterOperator<String> stringDataSource = env.readTextFile(ninCodeFilePath + File.separator + fileName, CHARSET_NAME_31J).filter(line -> !"".equals(line));
                Pattern compiledPattern = Pattern.compile(pattern);
                Matcher matcher = compiledPattern.matcher(fileName);
                boolean matches = matcher.matches();
                DataSet<String> result = stringDataSource.map(u -> {
                    String[] split = u.split(",");
                    String col1 = split[0];
                    if (fileName.contains("M11")) {
                        col1 = split[3];
                    }
                    col1 += "\t" + fileName;
                    if (matches||fileName.contains("H.csv")) {
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
                });
                if (allDataSet == null) {
                    allDataSet = result;
                } else {
                    allDataSet = allDataSet.union(result);
                }
            }
        }

        if (allDataSet != null) {
            allDataSet.writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
            env.execute(Test.class.getName());
        }
    }

}
