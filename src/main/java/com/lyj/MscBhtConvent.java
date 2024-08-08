package com.lyj;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;


public class MscBhtConvent {

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String mscbht00File = parameterTool.get("mscbht00_file", "input/data1/renpgmcho_mscbht00.txt");

        String mscbht00FileOutput = parameterTool.get("mscbht00_file", "output/renpgmcho_mscbht00.txt");

        String kaiCodeConventFile = parameterTool.get("m27_cho_file", "input/data1/m27_cho.txt");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataSet<String[]> mscbhtDs = env.readTextFile(mscbht00File).map(u -> u.split("\t"));

        DataSet<String[]> m27ChoDs = env.readTextFile(kaiCodeConventFile).map(u -> u.split(","));

        JoinOperator<String[], String[], Tuple2> result = mscbhtDs.leftOuterJoin(m27ChoDs)
                .where(u -> Integer.valueOf(u[5]))
                .equalTo(u -> Integer.valueOf(u[0])).with((row1, row2) -> {
                    Tuple2 tuple2 = new Tuple2<String, String[]>();
                    if (row2 != null) {
                        row1[5] = row2[1];
                        tuple2.f0 = "success";
                        tuple2.f1 = row1;
                    } else {
                        if ("0000".equals(row1[5])) {
                            row1[5] = "00000";
                            tuple2.f0 = "success";
                            tuple2.f1 = row1;
                        } else {
                            tuple2.f0 = "error";
                            tuple2.f1 = row1;
                        }
                    }
                    return tuple2;
                }).returns(Types.TUPLE(Types.STRING, Types.OBJECT_ARRAY(Types.STRING)));

        DataSet<String> strResult = result.filter(u -> "error".equals(u.f0)).map(u -> {
            StringBuilder sb = new StringBuilder();
            String[] row = (String[]) u.f1;
            for (int i = 0; i < row.length; i++) {
                if (i > 0) {
                    sb.append("\t");
                }
                sb.append(row[i]);
            }

            return sb.toString();
        });
        strResult.writeAsText(mscbht00FileOutput, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        env.execute(MscBhtConvent.class.getName());
    }
}
