package com.lyj.check;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static com.lyj.util.TableUtil.getFormattedDate;

/**
 * 须板买人code 生成有问题，需要替换
 * 买人code
 * 同通：担当者マスタ
 * ＦＡＸマスター_SUZ
 * INSERT_USER_ID 为 m27Z_prod_db 数据
 * 旧的买人code
 * 取引先口座マスタ_SUZ
 * INSERT_USER_ID 为m04z_prod_db 数据
 * 買人マスタ_SUZ
 * 替换错误的code
 * 差分替换
 */
public class ConventMasKaiListCode {

    private static final Logger logger = LoggerFactory.getLogger(ConventMasKaiListCode.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);


        String maskai00CodeMap = params.get("maskai00_code_map");

        String maskai00CodeNewMap = params.get("maskai00_code_new_map");

        String file = params.get("kai_file");


        boolean checkParamsResult = checkParams(file, maskai00CodeNewMap, maskai00CodeMap);
        if (!checkParamsResult) {
            logger.error("params demo : " + "--kai_file 取引先メール送信マスタ_ICH.xlsx  \n" + "--masnin00_code_new_map masnin00_ich.txt　\n" + "--maskai00_code_map m27_ich.txt ");
            return;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataSet<Tuple2<String, String>> maskai00CodeDs = env.readTextFile(maskai00CodeMap).map(u -> {
            Tuple2<String, String> tuple2 = new Tuple2();
            String[] split = u.split(",");
            tuple2.f0 = split[0];
            tuple2.f1 = split[1];
            return tuple2;
        }).returns(Types.TUPLE(Types.STRING, Types.STRING));

        DataSet<Tuple2<String, String>> maskai00CodeNewDs = env.readTextFile(maskai00CodeNewMap).map(u -> {
            Tuple2<String, String> tuple2 = new Tuple2();
            String[] split = u.split(",");
            tuple2.f0 = split[0];
            tuple2.f1 = split[1];
            return tuple2;
        }).returns(Types.TUPLE(Types.STRING, Types.STRING));

        DataSet<Tuple3<String, String, String>> joinDs = maskai00CodeDs.join(maskai00CodeNewDs).where(0).equalTo(0)
                .with(((first, second) -> {
                    Tuple3<String, String, String> tuple = new Tuple3();
                    tuple.f0 = first.f0;
                    tuple.f1 = first.f1;
                    tuple.f2 = second.f1;
                    return tuple;
                })).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING));

        DataSet<Row> masKaiDs = env.readTextFile(file).map(u -> {
            String[] split = u.split("\t");
            Row row = new Row(split.length);
            for (int i = 0; i < split.length; i++) {
                row.setField(i, split[i]);
            }
            return row;
        });

        DataSet<Row> result = masKaiDs.leftOuterJoin(joinDs).where(row -> (String) row.getField(2)).equalTo(u -> u.f1)
                .with((first, second) -> {
                    if (second != null) {
                        String update = (String) first.getField(0);
                        if("NEW".equals(update)){
                            if (first.getArity() > 3 ) {
                                String insertUserId = (String) first.getField(2);
                                if (insertUserId.contains("m04") || insertUserId.contains("m27")) {
                                    first.setField(2, second.f2);
                                }
                            } else {
                                first.setField(2, second.f2);
                            }
                        }
                    }
                    return first;
                });

        DataSet<String> resultDs = result.map(u -> {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < u.getArity(); i++) {
                sb.append(u.getField(i));
                if (i < u.getArity() - 1) {
                    sb.append("\t");
                }
            }
            return sb.toString();
        });
        resultDs.writeAsText("result/" + file + "_result.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute(ConventMasKaiListCode.class.getName() + "_" + getFormattedDate());
    }


    private static boolean checkParams(String file, String maskai00CodeNewMap, String maskai00CodeMap) {
        if (file == null) {
            logger.error("kai_file is null!");
            return false;
        }

        if (maskai00CodeNewMap == null) {
            logger.error("maskai00_code_new_map is null!");
            return false;
        }

        if (maskai00CodeMap == null) {
            logger.error("maskai00_code_map is null!");
            return false;
        }


        File txtFile = new File(file);
        if (!txtFile.isFile()) {
            logger.error("kai_file is not file");
            return false;
        }

        txtFile = new File(maskai00CodeNewMap);
        if (!txtFile.isFile()) {
            logger.error("maskai00_code_new_map is not file");
            return false;
        }

        txtFile = new File(maskai00CodeMap);
        if (!txtFile.isFile()) {
            logger.error("maskai00_code_map is not file");
            return false;
        }

        return true;

    }
}
