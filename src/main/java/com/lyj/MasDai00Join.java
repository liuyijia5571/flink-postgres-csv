package com.lyj;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static com.lyj.util.TableUtil.CHARSET_NAME_31J;

/**
 *
 */
public class MasDai00Join {

    private static final Logger logger = LoggerFactory.getLogger(MasDai00Join.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        // 通过命令行参来获取配置文件

        String inputFile = params.get("input_file");

        if (inputFile == null) {
            logger.error("input_file is null");
            return;
        }
        File file = new File(inputFile);

        if (!file.isFile()) {
            logger.error("input_file is not file");
            return;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String[]> map = env.readTextFile(inputFile, CHARSET_NAME_31J).map(
                u -> u.split("\t")
        );
        FilterOperator<String[]> liftDs = map.filter(u -> u[5].matches("^[0-9]{2}$"));
        FilterOperator<String[]> rightDs = map.filter(u -> u[5].matches("^[a-zA-Z]{2}$"));
        DataSet<String> result = liftDs.leftOuterJoin(rightDs).where(u -> u[9]).equalTo(u -> u[9])
                .with((row1, row2) ->{
                    String row2Str = row2 == null ? "" : row2[5];
                    return row1[9] + "\t" + row1[5] + "\t" +row2Str;
                });
        result.writeAsText("output/masdai.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        env.execute(MasDai00Join.class.getName());
    }
}
