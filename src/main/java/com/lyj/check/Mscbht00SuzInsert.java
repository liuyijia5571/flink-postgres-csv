package com.lyj.check;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static com.lyj.util.TableUtil.CHARSET_NAME_31J;
import static com.lyj.util.TableUtil.getFormattedDate;

/**
 * 発着地マスタ
 * 来自M10
 * 买人code 需要替换成新code  荷主code 需要替换新code
 */
public class Mscbht00SuzInsert {


    private static final Logger logger = LoggerFactory.getLogger(Mscbht00SuzInsert.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        String m10File = params.get("M10_FILE","C:\\SVN\\java\\kppDataMerge\\data\\txtData\\M10Z.csv");


        if (checkParams(m10File)) return;


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataSet<Row> m10fDs = env.readTextFile(m10File, CHARSET_NAME_31J).filter(u -> !"".equals(u)).map(u -> {
            Row row = new Row(14);
            String[] split = u.split(",", -1);
            row.setField(0, "22");
            row.setField(1, "999999");
            row.setField(2, split[0]);
            row.setField(3, split[1]);
            //買人コード
            row.setField(4, split[2]);
            row.setField(5, "");
            row.setField(6, "");
            //運送区分
            row.setField(7, split[3]);
            //荷主コード
            row.setField(8, "");
            row.setField(9, "");
            row.setField(10, "");
            row.setField(11, "");
            row.setField(12, "");
            row.setField(13, "");
            return row;
        });
        MapOperator<Row, String> result = m10fDs.map(u -> u.toString().replace(",", "\t"));

        result.writeAsText("result/mscbht_suz.tsv", FileSystem.WriteMode.OVERWRITE);

        env.execute(Mscbht00SuzInsert.class.getName() + "_" + getFormattedDate());
    }

    private static boolean checkParams(String m10File) {
        if (m10File == null) {
            logger.error("M10_FILE is null!");
            return true;
        }

        File txtFile = new File(m10File);
        if (!txtFile.isFile()) {
            logger.error("M10_FILE is not file");
            return true;
        }


        return false;
    }

}
