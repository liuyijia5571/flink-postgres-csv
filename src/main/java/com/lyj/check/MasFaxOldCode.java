package com.lyj.check;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static com.lyj.util.TableUtil.getFormattedDate;

/**
 * ＦＡＸマスター
 * add old code
 */
public class MasFaxOldCode {

    private static final Logger logger = LoggerFactory.getLogger(Mscfhs00Join.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        String masfax00File = params.get("MASFAX00_FILE");

        String masninCode = params.get("MASNIN_CODE");

        String maskaiCode = params.get("MASKAI_CODE");

        boolean checkParamsResult = checkParams(masfax00File, masninCode,maskaiCode);

        if (!checkParamsResult) {
            logger.error("params demo : " + "--MASKOZ00_FILE M63F.CSV  \n" + "--MASNIN_CODE M05F.CSV　\n"+ "--MASKAI_CODE M05F.CSV　\n");
            return;
        }
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataSource<String> masfaxDs = env.readTextFile(masfax00File);

        DataSet<Tuple2> masninDs = env.readTextFile(masninCode).map(u -> {
            String[] split = u.split(",");
            Tuple2 tuple2 = new Tuple2<String, String>();
            tuple2.f0 = split[0];
            tuple2.f1 = split[1];
            return tuple2;
        }).returns(Types.TUPLE(Types.STRING, Types.STRING));

        DataSet<Tuple2> maskaiDs = env.readTextFile(maskaiCode).map(u -> {
            String[] split = u.split(",");
            Tuple2 tuple2 = new Tuple2<String, String>();
            tuple2.f0 = split[0];
            tuple2.f1 = split[1];
            return tuple2;
        }).returns(Types.TUPLE(Types.STRING, Types.STRING));

        DataSet<String> result = masfaxDs.leftOuterJoin(masninDs).where(u -> {
            String[] split = u.split("\t");
            return split[4];
        }).equalTo(u -> u.f1.toString()).with(((first, second) -> {
            String oldCode = second != null ? second.f0.toString() : "";
            String[] split = first.split("\t");
            String type = split[split.length - 1];
            if ("masnin".equals(type)){
                String line = first + "\t" + oldCode;
                return line;
            }
            return first;
        })).leftOuterJoin(maskaiDs).where(u -> {
            String[] split = u.split("\t");
            return split[4];
        }).equalTo(u -> u.f1.toString()).with(((first, second) -> {
            String oldCode = second != null ? second.f0.toString() : "";
            String[] split = first.split("\t");
            String type = split[split.length - 1];
            if ("maskai".equals(type)){
                String line = first + "\t" + oldCode;
                return line;
            }
            return first;
        }));
        result.writeAsText("result/masfax00_old_fun.tsv", FileSystem.WriteMode.OVERWRITE);

        env.execute(MasFaxOldCode.class.getName() + "_" + getFormattedDate());
    }

    private static boolean checkParams(String maskozFile, String masninCode,String maskaiCode) {
        if (maskozFile == null) {
            logger.error("MASKOZ00_FILE is null!");
            return false;
        }

        if (masninCode == null) {
            logger.error("MASNIN_CODE is null!");
            return false;
        }

        if (maskaiCode == null) {
            logger.error("MASKAI_CODE is null!");
            return false;
        }

        File txtFile = new File(maskozFile);
        if (!txtFile.isFile()) {
            logger.error("MASKOZ00_FILE is not file");
            return false;
        }

        txtFile = new File(masninCode);
        if (!txtFile.isFile()) {
            logger.error("MASNIN_CODE is not file");
            return false;
        }

        txtFile = new File(maskaiCode);
        if (!txtFile.isFile()) {
            logger.error("MASKAI_CODE is not file");
            return false;
        }
        return true;
    }
}
