package com.lyj.check;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static com.lyj.util.TableUtil.getFormattedDate;

/**
 * 担当者マスター
 * 转换 支社内買人コード Y列 下标， 24
 * 来源，担当者マスター 担当者マスタ_細谷_宮川修正.xlsx
 * 关联条件：C列，データNO DNOSA1
 * D列 支社区分 SIKSA1
 * E列 コード HTOSA1
 */
public class Masser00ConventKaiCode {


    private static final Logger logger = LoggerFactory.getLogger(Masser00ConventKaiCode.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        String masser00File = params.get("MASSER00_FILE");

        String masser00RightFile = params.get("MASSER00_RIGHT_FILE");


        if (masser00File == null) {
            logger.error("MASSER00_FILE is null!");
            return;
        }

        File txtFile = new File(masser00File);
        if (!txtFile.isFile()) {
            logger.error("MASSER00_FILE is not file");
            return;
        }

        if (masser00RightFile == null) {
            logger.error("MASSER00_RIGHT_FILE is null!");
            return;
        }

        txtFile = new File(masser00RightFile);
        if (!txtFile.isFile()) {
            logger.error("MASSER00_RIGHT_FILE is not file");
            return;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataSet<Tuple5> masserDs = env.readTextFile(masser00File).map(u -> {
            String[] split = u.split("\t");
            Tuple5 tuple = new Tuple5<String, String, String, String, String>();
            tuple.f0 = split[0];
            tuple.f1 = split[2];
            tuple.f2 = split[3];
            tuple.f3 = split[4];
            tuple.f4 = split[24];
            return tuple;
        }).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING));

        DataSet<Tuple4> masserRightDs = env.readTextFile(masser00RightFile).map(u -> {
            String[] split = u.split("\t");
            Tuple4 tuple = new Tuple4<String, String, String, String>();
            tuple.f0 = split[2];
            tuple.f1 = split[3];
            tuple.f2 = split[4];
            tuple.f3 = split[24];
            return tuple;
        }).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING));

        DataSet<String> resultDs = masserDs.leftOuterJoin(masserRightDs)
                .where(1, 2, 3).equalTo(0, 1, 2)
                .with(((first, second) -> {
                    if (second != null) {
                        String f2 = (String) first.f2;
                        //21，22，23，34，35
                        if ("21".equals(f2) || "22".equals(f2) || "23".equals(f2) || "34".equals(f2) || "35".equals(f2)) {
                            first.f4 = second.f3;
                        } else {
                            logger.error(" SIKSA1 not in (21，22，23，34，35) data is {}", first);
                        }
                    } else {
                        logger.error("match fail data is {}", first);
                    }
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < first.getArity(); i++) {
                        sb.append(first.getField(i));
                        if (1 < first.getArity() - 1) {
                            sb.append("\t");
                        }
                    }
                    return sb.toString();
                }));

        resultDs.writeAsText("result/masser00_result.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute(Masser00ConventKaiCode.class.getName() + "_" + getFormattedDate());
    }
}
