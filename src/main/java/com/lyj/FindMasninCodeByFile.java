package com.lyj;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static com.lyj.util.TableUtil.CHARSET_NAME_31J;
import static com.lyj.util.TableUtil.getFormattedDate;

/**
 * 参数
 * --input_path
 * C:\flink\input\20240622提供分\マスタファイル
 * --masnin_code_file
 * C:\flink\input\masnin_code.txt
 */
public class FindMasninCodeByFile {

    private static final Logger logger = LoggerFactory.getLogger(FindMasninCodeByFile.class);

    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String inputPath = parameterTool.get("input_path");

        String masninCodeFile = parameterTool.get("masnin_code_file");

        String resultFile = parameterTool.get("result_file","output/findMasninCode.csv");

        boolean checkParamsResult = checkParams(inputPath, masninCodeFile);
        if (!checkParamsResult) {
            logger.error("params demo : " + "--db_profile dev43  \n" + "--input_file_path C:\\青果\\黄信中要的数据　\n" + "--schema xuexiaodingtest2 \n");
            return;
        }
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        File file = new File(inputPath);
        DataSource<String> masNinCodeDataSet = env.readTextFile(masninCodeFile);

        DataSet<Tuple2> allResultDs = null;

        for (File listFile : file.listFiles()) {
            if (listFile.isFile()) {
                String fileName = listFile.getName();
                String inputFilePath = inputPath + File.separator + fileName;
                DataSet<Row> rowDataSet = env.readTextFile(inputFilePath, CHARSET_NAME_31J)
                        .filter(line -> !"".equals(line))
                        .map(line -> {
                            String[] split = line.split(",");
                            Row row = new Row(split.length);
                            for (int i = 0; i < split.length; i++) {
                                row.setField(i, split[i]);
                            }
                            return row;
                        });
                DataSet<Tuple2> resultDs = masNinCodeDataSet.join(rowDataSet).where(u -> u)
                        .equalTo(u -> u.getField(0).toString()).with((row1, row2) -> {
                            Tuple2 tuple2 = new Tuple2();
                            tuple2.f0 = fileName;
                            tuple2.f1 = row2.toString();
                            return tuple2;
                        }).returns(Types.TUPLE(Types.STRING, Types.STRING));
                if (allResultDs == null) {
                    allResultDs = resultDs;
                } else {
                    allResultDs = allResultDs.union(resultDs);
                }
            }
        }
        // 创建 CsvOutputFormat
        CsvOutputFormat<Tuple2> csvOutputFormat = new CsvOutputFormat<>(new Path(resultFile));
        csvOutputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
        csvOutputFormat.setCharsetName(CHARSET_NAME_31J); // 指定编码格式

        // 将 DataSet 写入 CSV 文件
        allResultDs.output(csvOutputFormat).setParallelism(1);

        env.execute(FindMasninCodeByFile.class.getName() + "_" + getFormattedDate());
    }

    private static boolean checkParams(String inputPath, String masninCodeFile) {
        if (inputPath == null) {
            logger.error("input_path is null!");
            return false;
        }
        File inputFile = new File(inputPath);

        if (!inputFile.exists()) {
            logger.error("input_path is null!");
            return false;
        }

        if (!inputFile.isDirectory()) {
            logger.error("input_path is not directory!");
            return false;
        }
        if (masninCodeFile == null) {
            logger.error("masnin_code_file is null!");
            return false;
        }
        File masninFile = new File(masninCodeFile);
        if (!masninFile.exists()) {
            logger.error("input_path is null!");
            return false;
        }

        if (!masninFile.isFile()) {
            logger.error("input_path is not file");
            return false;
        }
        return true;
    }
}
