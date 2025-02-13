package com.lyj.check;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.lyj.util.TableUtil.CHARSET_NAME_31J;
import static com.lyj.util.TableUtil.getFormattedDate;

/**
 * 運送者・運送会社マスタ
 *
 */
public class Mscbus00Insert {

    private static final Logger logger = LoggerFactory.getLogger(Mscbus00Insert.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        //C:\\SVN\\java\\kppDataMerge\\data\\txtData\\M09Z.csv
        String m09File = params.get("M09_file");

        //C:\SVN\java\kppDataMerge\data\txtData\M25F.csv
        String m25File = params.get("M25_file","C:\\SVN\\java\\kppDataMerge\\data\\txtData\\M25K.csv");

        //22:Z 须板
        //34:F 船桥
        //35:K 市川
        String sikus1 = params.get("SIKUS1","35");

        boolean checkParamsResult = checkParams(m09File, m25File, sikus1);

        if (!checkParamsResult) {
            logger.error("params demo : " + "--M09_file M09F.CSV  \n"
                    + "--M25_file M25.CSV --SIKUS1 22　\n");
            return;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataSet<Row> ds = null;
        if (m09File != null) {
            DataSet<String> m09Ds = env.readTextFile(m09File, CHARSET_NAME_31J).filter(u -> !"".equals(u));
            DataSet<Row> rowDs = m09Ds.map(u -> {
                String[] split = u.split(",");
                Row row = new Row(12);
                row.setField(0,sikus1);
                row.setField(1,split[0]);
                row.setField(2,split[1]);
                row.setField(3,"カナ");
                row.setField(4,"");
                row.setField(5,"");
                row.setField(6,"");
                row.setField(7,"1");
                row.setField(8,split[2]);
                row.setField(9,"");
                row.setField(10,"");
                row.setField(11,"");
                return row;
            });
            ds = rowDs;
        }
        if (m25File != null) {
            DataSet<String> m25Ds = env.readTextFile(m25File, CHARSET_NAME_31J).filter(u -> !"".equals(u));
            DataSet<Row> rowDs = m25Ds.map(u -> {
                Row row = new Row(12);
                String[] split = u.split(",");
                row.setField(0,sikus1);
                row.setField(1,split[0]);
                row.setField(2,split[1]);
                row.setField(3,split[2]);
                row.setField(4,"カナ");
                row.setField(5,split[3]);
                row.setField(6,split[4]);
                row.setField(7,"2");
                row.setField(8,split[5]);
                row.setField(9,"");
                row.setField(10,"");
                row.setField(11,"");
                return row;
            });
            if (ds != null) {
                ds = ds.union(rowDs);
            } else {
                ds = rowDs;
            }
        }
        MapOperator<Row, String> result = ds.map(u -> u.toString().replace(",", "\t"));
        result.writeAsText("result/mscbus00.txt", FileSystem.WriteMode.OVERWRITE);
        env.execute(Mscbus00Insert.class.getName() + "_" + getFormattedDate());

    }

    private static boolean checkParams(String m09File, String m25File, String sikus1) {
        if (sikus1 != null && (m09File != null || m25File != null)) {
            return true;
        }
        return false;
    }
}


