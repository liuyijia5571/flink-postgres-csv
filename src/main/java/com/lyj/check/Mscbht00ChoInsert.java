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

import static com.lyj.util.TableUtil.CHARSET_NAME_31J;
import static com.lyj.util.TableUtil.getFormattedDate;

/**
 * 発着地マスタ
 * 来自M10
 * 买人code 需要替换成新code  荷主code 需要替换新code
 */
public class Mscbht00ChoInsert {


    private static final Logger logger = LoggerFactory.getLogger(Mscbht00ChoInsert.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        String m10File = params.get("M10_FILE","C:\\SVN\\java\\kppDataMerge\\data\\txtData\\M10.csv");

        String maskaiCode = params.get("maskai00_code_map","maskai00_code_map/m27_cho.txt");

        String masnin00CodeMap = params.get("masnin00_code_map","masnin00_code_map/masnin00_cho.txt");

        if (checkParams(m10File, maskaiCode, masnin00CodeMap)) return;


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataSet<Row> m10fDs = env.readTextFile(m10File, CHARSET_NAME_31J).filter(u -> !"".equals(u)).map(u -> {
            Row row = new Row(14);
            String[] split = u.split(",",-1);
            row.setField(0, "21");
            row.setField(1, "999999");
            row.setField(2, split[0]);
            row.setField(3, split[1]);
            //買人コード
            row.setField(4, split[2]);
            row.setField(5, "");
            row.setField(6, split[3]);
            row.setField(7, "");
            //荷主コード
            row.setField(8, split[4]);
            row.setField(9, "");
            row.setField(10, "");
            row.setField(11, "");
            row.setField(12, "");
            row.setField(13, "");
            return row;
        });

        DataSet<Tuple3> maskaiDs = env.readTextFile(maskaiCode).map(u -> {
            String[] split = u.split(",");
            Tuple3 tuple3 = new Tuple3<String, String, String>();
            tuple3.f0 = split[0];
            tuple3.f1 = split[1];
            tuple3.f2 = maskaiCode;
            return tuple3;
        }).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING));

        DataSet<Tuple2<String, String>> masnin00Ds = env.readTextFile(masnin00CodeMap).map(u -> {
            Tuple2<String, String> tuple2 = new Tuple2();
            String[] split = u.split(",");
            tuple2.f0 = split[0];
            tuple2.f1 = split[1];
            return tuple2;
        }).returns(Types.TUPLE(Types.STRING, Types.STRING));

        DataSet<Row> temp = m10fDs.leftOuterJoin(maskaiDs).where(
                u ->{
                    String field = (String) u.getField(4);
                    if ("".equals(field)){
                        return -1L;
                    }else{
                        return Long.valueOf(field);
                    }
                })
                .equalTo(u -> Long.valueOf((String) u.getField(0)))
                .with(((first, second) -> {
                    if (second == null) {
                        if (!"0000".equals(first.getField(4)))
                        logger.error("match masKaiDs fail data is {} ", first);
                    } else {
                        first.setField(4, second.f1);
                    }
                    return first;
                }));

        DataSet<String> result = temp.leftOuterJoin(masnin00Ds).where(u->{
                    String field = (String) u.getField(8);
                    if ("".equals(field)){
                        return -1L;
                    }else{
                        return Long.valueOf(field);
                    }
                })
                .equalTo(u -> Long.valueOf((String) u.getField(0)))
                .with(((first, second) -> {
                    if (second == null) {
                        if (!"".equals(first.getField(8))) {
                            logger.error("match masNinDs fail data is {} ", first);
                        }
                    } else {
                        first.setField(8, second.f1);
                    }
                    return first.toString().replace(",","\t");

                }));


        result.writeAsText("result/mscbht.tsv", FileSystem.WriteMode.OVERWRITE);

        env.execute(Mscbht00ChoInsert.class.getName() + "_" + getFormattedDate());
    }

    private static boolean checkParams(String m10File, String maskaiCode, String masnin00CodeMap) {
        if (m10File == null) {
            logger.error("M10_FILE is null!");
            return true;
        }

        File txtFile = new File(m10File);
        if (!txtFile.isFile()) {
            logger.error("M10_FILE is not file");
            return true;
        }

        if (maskaiCode == null) {
            logger.error("MASKAI_CODE is null!");
            return true;
        }

        txtFile = new File(maskaiCode);
        if (!txtFile.isFile()) {
            logger.error("MASKAI_CODE is not file");
            return true;
        }

        if (masnin00CodeMap == null) {
            logger.error("masnin00_code_map is null!");
            return true;
        }

        txtFile = new File(masnin00CodeMap);
        if (!txtFile.isFile()) {
            logger.error("masnin00_code_map is not file");
            return true;
        }
        return false;
    }

}
