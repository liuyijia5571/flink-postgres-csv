package com.lyj.master;

import com.lyj.util.CustomTextOutputFormat;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.File;
import java.util.Arrays;

import static com.lyj.util.TableUtil.CHARSET_NAME_31J;

public class I25Replace {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        String inputFilePath = params.get("input_file_path", "C:\\flink\\I25");

        String newInputFilePath = params.get("result_file_path", "C:\\flink\\I25new");


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        File folder = new File(inputFilePath);
        if (folder.exists()) {
            File[] files = folder.listFiles();
            if (files != null) {
                for (int i = 0; i < files.length; i++) {
                    String fileName = files[i].getName();
                    DataSource<String> stringDataSource = env.readTextFile(inputFilePath + File.separator + fileName, CHARSET_NAME_31J);
                    MapOperator<String, Object> result = stringDataSource.map(u -> u.replace(","," "));
                    String newFileName = fileName.replace("csv", "txt");
                    CustomTextOutputFormat textOutputR052z = new CustomTextOutputFormat(new Path(newInputFilePath + File.separator + newFileName), CHARSET_NAME_31J, "\r\n");
                    textOutputR052z.setWriteMode(FileSystem.WriteMode.OVERWRITE);
                    result.output(textOutputR052z).setParallelism(1);
                }
            }
            env.execute(I25Replace.class.getName());
        }
    }
}
