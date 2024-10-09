package com.lyj.check;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
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
public class Mscbht00Insert {


    private static final Logger logger = LoggerFactory.getLogger(Mscbht00Insert.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        String m10File = params.get("M10_FILE");

        String maskaiCode = params.get("maskai00_code_map");

        String maskaiChoCode = params.get("maskai00_cho_code_map");

        if (m10File == null) {
            logger.error("M10_FILE is null!");
            return;
        }

        File txtFile = new File(m10File);
        if (!txtFile.isFile()) {
            logger.error("M10_FILE is not file");
            return;
        }

        if (maskaiCode == null) {
            logger.error("MASKAI_CODE is null!");
            return;
        }

        txtFile = new File(maskaiCode);
        if (!txtFile.isFile()) {
            logger.error("MASKAI_CODE is not file");
            return;
        }


        if (maskaiChoCode == null) {
            logger.error("maskai00_cho_code_map is null!");
            return;
        }

        txtFile = new File(maskaiChoCode);
        if (!txtFile.isFile()) {
            logger.error("maskai00_cho_code_map is not file");
            return;
        }


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataSet<Tuple2> m05fDs = env.readTextFile(m10File, CHARSET_NAME_31J).filter(u -> !"".equals(u)).map(u -> {
            int index = u.indexOf(",");
            String other = u.substring(index);
            String oldMaskaiCode = u.substring(0, index);
            Tuple2 tuple2 = new Tuple2();
            tuple2.f0 = oldMaskaiCode;
            tuple2.f1 = other;
            return tuple2;
        }).returns(Types.TUPLE(Types.STRING, Types.STRING));

        DataSet<Tuple3> maskaiDs = env.readTextFile(maskaiCode).map(u -> {
            String[] split = u.split(",");
            Tuple3 tuple3 = new Tuple3<String, String, String>();
            tuple3.f0 = split[0];
            tuple3.f1 = split[1];
            tuple3.f2 = maskaiCode;
            return tuple3;
        }).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING));

        DataSet<Tuple3> maskaiChoDs = env.readTextFile(maskaiChoCode).map(u -> {
            String[] split = u.split(",");
            Tuple3 tuple3 = new Tuple3<String, String, String>();
            tuple3.f0 = split[0];
            tuple3.f1 = split[1];
            tuple3.f2 = maskaiChoCode;
            return tuple3;
        }).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING));

        DataSet<Tuple3> distinct = maskaiDs.union(maskaiChoDs).groupBy(u -> (String) u.getField(0)) // 根据 A 字段分组
                .reduce((value1, value2) -> {
                    // 随机选择 B 字段的值
                    if (maskaiCode.equals(value1.f2)) {
                        return value1;
                    } else if (maskaiCode.equals(value2.f2)) {
                        return value2;
                    }
                    return value1;
                });
        ;

        DataSet<String> result = m05fDs.leftOuterJoin(distinct).where(u -> Long.valueOf((String) u.getField(0)))
                .equalTo(u -> Long.valueOf((String) u.getField(0)))
                .with(((first, second) -> {
                    if (second == null) {
                        logger.error("match fail data is {} ", first);
                        String replace = first.toString().replace(",", "\t");
                        return "22\t999999\t" + replace;
                    } else {
                        String replace = ((String) second.f1 + first.f1).replace(",", "\t");
                        return "22\t999999\t" + replace;
                    }
                }));


        result.writeAsText("result/mscbht.tsv", FileSystem.WriteMode.OVERWRITE);

        env.execute(Mscbht00Insert.class.getName() + "_" + getFormattedDate());
    }

}
