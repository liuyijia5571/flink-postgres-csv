package com.lyj.check;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static com.lyj.util.TableUtil.CHARSET_NAME_31J;
import static com.lyj.util.TableUtil.getFormattedDate;

/**
 * 船橋品種マスタ
 * M63F_船橋品種マスタ
 * M05F_船橋品目マスタ
 * join
 */
public class Mscfhs00Join {

    private static final Logger logger = LoggerFactory.getLogger(Mscfhs00Join.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        String m63fFile = params.get("M63F_file", "D:\\船橋品種マスタ\\M63F.csv");

        String m05fFile = params.get("M05F_file", "D:\\船橋品種マスタ\\M05F.csv");

        String hinFile = params.get("M05F_file", "D:\\船橋品種マスタ\\品名マスタ(中野)_新旧対比表.txt");
        boolean checkParamsResult = checkParams(m63fFile, m05fFile);

        if (!checkParamsResult) {
            logger.error("params demo : " + "--M63F_file M63F.CSV  \n"
                    + "--M05F_file M05F.CSV　\n");
            return;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataSet<String> m63fDs = env.readTextFile(m63fFile, CHARSET_NAME_31J).filter(u -> !"".equals(u));

        DataSet<String> m05fDs = env.readTextFile(m05fFile, CHARSET_NAME_31J).filter(u -> !"".equals(u));

        DataSet<String[]> hinDs = env.readTextFile(hinFile).map(u -> u.split("\t")).filter(u -> u.length > 2);

        DataSet<Row> result = m63fDs.leftOuterJoin(m05fDs)
                .where(u -> u.split(",")[0]).equalTo(
                        u -> {
                            String[] split = u.split(",");
                            if (split.length >= 2)
                                return split[split.length - 2];
                            return u;
                        }
                ).with(((first, second) -> {
                    Row row = new Row(9);
                    String[] firstSplit = first.split(",");
                    //品種コード M63_品種CD
                    row.setField(0, firstSplit[0]);
                    //品種名 M63_品種名
                    row.setField(1, firstSplit[1].replace("　", ""));
                    //品種名ｶﾅ
                    row.setField(2, "");
                    //品名コード M05_品目CD
                    row.setField(3, second != null ? second.split(",")[0] : "0");
                    //品名コード2
                    row.setField(4, "0");
                    //削除区分
                    row.setField(5, "");
                    //予備1
                    row.setField(6, "");
                    //予備２
                    row.setField(7, "0");
                    return row;
                }));

        result.leftOuterJoin(hinDs).where(u -> u.getField(3).toString() + u.getField(4).toString())
                .equalTo(u -> u[2]).with((first, second) -> {
            if (second != null) {
                //品種名 M63_品種名
                first.setField(1, second[0]);
                //品種名ｶﾅ
                first.setField(2, second[1]);
                first.setField(8, "");
            } else {
                first.setField(8,"fail");
            }
            return first;
        }).map(u -> {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < u.getArity(); i++) {
                if (i == u.getArity() - 1) {
                    sb.append(u.getField(i));
                } else {
                    sb.append(u.getField(i)).append("\t");
                }
            }
            return sb.toString();
        }).writeAsText("result/mscfhs00.txt", FileSystem.WriteMode.OVERWRITE);
        env.execute(Mscfhs00Join.class.getName() + "_" + getFormattedDate());
    }

    private static boolean checkParams(String m63fFile, String m05fFile) {
        if (m63fFile == null) {
            logger.error("M63F_file is null!");
            return false;
        }

        if (m05fFile == null) {
            logger.error("M05F_file is null!");
            return false;
        }

        File txtFile = new File(m63fFile);
        if (!txtFile.isFile()) {
            logger.error("M63F_file is not file");
            return false;
        }

        txtFile = new File(m05fFile);
        if (!txtFile.isFile()) {
            logger.error("M05F_file is not file");
            return false;
        }
        return true;
    }

}
