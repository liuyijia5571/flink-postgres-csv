package com.lyj.check;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static com.lyj.util.TableUtil.getFormattedDate;

/**
 * MSCBSM00（取引先メール送信マスタ）
 */
public class Mscbsm00Insert {

    private static final Logger logger = LoggerFactory.getLogger(Mscbsm00Insert.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        String masnin00CodeMap = params.get("masnin00_code_map");

        String maskai00CodeMap = params.get("maskai00_code_map");

        String qa1028File = params.get("QA1028_file");

        String siksm1 = params.get("SIKSM1");

        boolean checkParamsResult = checkParams(qa1028File, masnin00CodeMap, maskai00CodeMap, siksm1);
        if (!checkParamsResult) {
            logger.error("params demo : " + "--QA1028_file 取引先メール送信マスタ_ICH.xlsx  \n" + "--masnin00_code_map masnin00_ich.txt　\n" + "--maskai00_code_map m27_ich.txt \n --SIKSM1 35");
            return;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        DataSet<Tuple2<String, String>> masnin00Ds = env.readTextFile(masnin00CodeMap).map(u -> {
            Tuple2<String, String> tuple2 = new Tuple2();
            String[] split = u.split(",");
            tuple2.f0 = split[0];
            tuple2.f1 = split[1];
            return tuple2;
        }).returns(Types.TUPLE(Types.STRING, Types.STRING));

        DataSet<Tuple2<String, String>> maskai00Ds = env.readTextFile(maskai00CodeMap).map(u -> {
            Tuple2<String, String> tuple2 = new Tuple2();
            String[] split = u.split(",");
            tuple2.f0 = split[0];
            tuple2.f1 = split[1];
            return tuple2;
        }).returns(Types.TUPLE(Types.STRING, Types.STRING));

        DataSet<Row> qa1028Ds = env.readTextFile(qa1028File).map(u -> {
            Row row = new Row(10);
            String[] split = u.split("\t", -1);
            row.setField(0, "BS");
            row.setField(1, siksm1);

            //Ｋ＝御買上明細書
            //Ｂ＝売買仕切書、Ｆ＝御買上データ、Ｋ＝御買上明細書、Ｕ＝売立通知書
            if ("御買上明細書".equals(split[1]) || "御買上データ".equals(split[1])) {
                //買人
                row.setField(2, "D2");
                //Ｂ＝売買仕切書 Ｕ＝売立通知書 農協出荷地物データ
            } else if ("売買仕切書".equals(split[1]) || "売立通知書".equals(split[1])) {
                //荷主
                row.setField(2, "D1");
            } else {
                row.setField(2, "");
            }
            row.setField(3, split[3]);
            row.setField(4, split[4]);
            if ("PDFファイル".equals(split[2]) || "売立通知書".equals(split[1])) {
                row.setField(5, "1");
                row.setField(6, "");
            } else if ("CSVファイル".equals(split[2])) {
                row.setField(5, "");
                row.setField(6, "1");
            }
            if (split.length > 5)
                row.setField(7, split[5]);
            else
                row.setField(7, "");
            if (split.length > 7)
                row.setField(8, split[7]);
            else
                row.setField(8, "");
            row.setField(9, "");
            return row;
        });

        DataSet<Row> result = qa1028Ds.leftOuterJoin(masnin00Ds)
                .where(u -> u.getField(2) + "_" + u.getField(3))
                .equalTo(u -> "D1_" + u.getField(0)).with((first, second) -> {
                    if (second != null) {
                        first.setField(3, second.getField(1));
                    }
                    return first;
                }).leftOuterJoin(maskai00Ds)
                .where(u ->  u.getField(2) + "_" + u.getField(3))
                .equalTo(u -> "D2" + "_" + u.getField(0)).with(((first, second) -> {
                    if (second != null) {
                        first.setField(3, second.getField(1));
                    }
                    return first;
                }));
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
        resultDs.writeAsText("result/mscbsm00_" + siksm1 + ".txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute(Mscbsm00Insert.class.getName() + "_" + getFormattedDate());
    }

    private static boolean checkParams(String qa1028File, String masnin00CodeMap, String maskai00CodeMap, String siksm1) {
        if (qa1028File == null) {
            logger.error("QA1028_file is null!");
            return false;
        }

        if (masnin00CodeMap == null) {
            logger.error("masnin00_code_map is null!");
            return false;
        }

        if (maskai00CodeMap == null) {
            logger.error("maskai00_code_map is null!");
            return false;
        }

        if (siksm1 == null) {
            logger.error("SIKSM1 is null!");
            return false;
        }

        File txtFile = new File(qa1028File);
        if (!txtFile.isFile()) {
            logger.error("QA1028_file is not file");
            return false;
        }

        txtFile = new File(masnin00CodeMap);
        if (!txtFile.isFile()) {
            logger.error("masnin00_code_map is not file")                                                                               ;
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
