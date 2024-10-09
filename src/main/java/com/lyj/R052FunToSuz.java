package com.lyj;

import com.lyj.util.CustomTextOutputFormat;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.lyj.util.TableUtil.CHARSET_NAME_31J;

/**
 * 把R052F 中须板的数据分出来
 */
public class R052FunToSuz {


    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        String inputFilePath = params.get("input_file_path", "C:\\flink\\data");

        String newInputFilePath = params.get("new_input_file_path", "C:\\flink\\newData");

        String suzCodeFilePath = params.get("suz_code_file", "C:\\flink\\suz_code.txt");

        String jobFile = params.get("job_file", "C:\\flink\\funToSuz.txt");


        List<Tuple5> jobMap = Files.readAllLines(Paths.get(jobFile)).stream().map(u -> {
            Tuple5 tuple5 = new Tuple5();
            String[] split = u.split("\t", -1);
            for (int i = 0; i < split.length; i++) {
                if (i < 5) tuple5.setField(split[i], i);
            }
            return tuple5;
        }).collect(Collectors.toList());
        Map<String, List<Tuple5>> resultMap = jobMap.stream().collect(Collectors.groupingBy(u -> (String) u.f0));


        List<Tuple5> tuple5ListR052Z = resultMap.get("R052Z");

        List<Tuple5> tuple5ListR052F = resultMap.get("R052F");


        if (tuple5ListR052Z.isEmpty() || tuple5ListR052F.isEmpty()) {
            return;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> suzCodeDs = env.readTextFile(suzCodeFilePath);

        env.setParallelism(1);

        for (int i = 0; i < tuple5ListR052Z.size(); i++) {
            Tuple5 tuple5R052Z = tuple5ListR052Z.get(i);
            Tuple5 tuple5R052f = tuple5ListR052F.get(i);

            DataSet<String> r052fDs = env.readTextFile(inputFilePath + File.separator + tuple5R052f.f2, CHARSET_NAME_31J);
            DataSet<String> r052zDs = env.readTextFile(inputFilePath + File.separator + tuple5R052Z.f2, CHARSET_NAME_31J);

            DataSet<Tuple2> unionR05fDs = r052fDs.leftOuterJoin(suzCodeDs).where(u -> {
                if ("".equals(u)) {
                    return new BigDecimal(0);
                } else {
                    String key = u.split(",")[0];
                    return new BigDecimal(key);
                }
            }).equalTo(u -> new BigDecimal(u)).with((row1, row2) -> {
                Tuple2 tuple2 = new Tuple2();
                if (row2 == null) {
                    tuple2.f0 = "fun";
                } else {
                    tuple2.f0 = "suz";
                }
                tuple2.f1 = row1;
                return tuple2;
            }).returns(Types.TUPLE(Types.STRING, Types.STRING));
            MapOperator<Tuple2, String> newR05fDs = unionR05fDs.filter(u -> "fun".equals(u.f0)).map(u -> (String) u.f1);

            MapOperator<Tuple2, String> newR05ZDs = unionR05fDs.filter(u -> "suz".equals(u.f0)).map(u -> (String) u.f1);


            CustomTextOutputFormat textOutputR052z = new CustomTextOutputFormat(new Path(newInputFilePath + File.separator + tuple5R052Z.f2), CHARSET_NAME_31J, "\r\n");
            textOutputR052z.setWriteMode(FileSystem.WriteMode.OVERWRITE);
            newR05ZDs.union(r052zDs).output(textOutputR052z).setParallelism(1);


            CustomTextOutputFormat textOutputFormatR05f = new CustomTextOutputFormat(new Path(newInputFilePath + File.separator + tuple5R052f.f2), CHARSET_NAME_31J, "\r\n");
            textOutputFormatR05f.setWriteMode(FileSystem.WriteMode.OVERWRITE);
            newR05fDs.output(textOutputFormatR05f).setParallelism(1);

        }

        env.execute(R052FunToSuz.class.getName());
    }
}

