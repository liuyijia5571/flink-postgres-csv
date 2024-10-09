package com.lyj.check;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.shaded.netty4.io.netty.util.internal.StringUtil;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static com.lyj.util.TableUtil.CHARSET_NAME_31J;
import static com.lyj.util.TableUtil.getFormattedDate;

/**
 * 船橋品種マスタ
 * M63F_船橋船橋品種マスタ
 * M05F_船橋品目マスタ
 * join
 */
public class Mscbus00Inset {

    private static final Logger logger = LoggerFactory.getLogger(Mscbus00Inset.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        String m09File = params.get("M09_file",null);

        String m25File = params.get("M25_file",null);

        String sikus1 = params.get("SIKUS1");

        boolean checkParamsResult = checkParams(m09File, m25File, sikus1);

        if (!checkParamsResult) {
            logger.error("params demo : " + "--M09_file M09.CSV  \n"
                    + "--M25_file M25.CSV　\n --SIKUS1 21");
            return;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataSet<Row> inputDs = null;
        if (StringUtils.isNotBlank(m09File)) {
            DataSet<String> m09Ds = env.readTextFile(m09File, CHARSET_NAME_31J).filter(u -> !"".equals(u));
            DataSet<Row> temp = m09Ds.map(u -> {
                Row row = new Row(12);
                String[] split = u.split(",");
                row.setField(0, sikus1);
                row.setField(1, split[0]);
                row.setField(2, split[1]);
                row.setField(3, "");
                row.setField(4, "");
                row.setField(5, "");
                row.setField(6, "");
                row.setField(7, "1");
                row.setField(8, split[2]);
                row.setField(9, "");
                row.setField(10, "");
                row.setField(11, "");
                return row;
            });
            inputDs = temp;
        }
        if (StringUtils.isNotBlank(m25File)) {
            DataSet<String> m25Ds = env.readTextFile(m25File, CHARSET_NAME_31J).filter(u -> !"".equals(u));
            DataSet<Row> temp = m25Ds.map(u -> {
                Row row = new Row(11);
                String[] split = u.split(",");
                row.setField(0, sikus1);
                row.setField(1, split[0]);
                row.setField(2, split[1]);
                row.setField(3, "");
                row.setField(4, split[2]);
                row.setField(5, split[3]);
                row.setField(6, split[4]);
                row.setField(7, "2");
                row.setField(8, split[5]);
                row.setField(9, "");
                row.setField(10, "");
                return row;
            });
            if (inputDs != null) {
                inputDs = inputDs.union(temp);
            } else {
                inputDs = temp;
            }
        }

        if (inputDs != null) {
            inputDs.map(u -> {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < u.getArity(); i++) {
                    if (i == u.getArity() - 1) {
                        sb.append(u.getField(i));
                    } else {
                        sb.append(u.getField(i)).append("\t");
                    }
                }
                return sb.toString();
            }).writeAsText("result/mscbus00_" + sikus1 + ".txt", FileSystem.WriteMode.OVERWRITE);
            env.execute(Mscbus00Inset.class.getName() + "_" + getFormattedDate());
        }

    }

    private static boolean checkParams(String m09File, String m25File, String sikus1) {
        if (m25File == null && m09File == null) {
            logger.error("m25File and m09File is null!");
            return false;
        }

        if (sikus1 == null) {
            logger.error("SIKUS1 is null!");
            return false;
        }

        return true;
    }

}
