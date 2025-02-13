package com.lyj.check;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

import static com.lyj.util.TableUtil.CHARSET_NAME_31J;
import static com.lyj.util.TableUtil.getFormattedDate;


/**
 * 支払集約コード 要填什么呀，目前代码里是有这一块逻辑的
 * 第21位6，7需要处理
 * 6是市川市農協 7是千葉東葛農協
 */
public class MasninFunInsert {

    /**
     * 市川市農協
     */
    private final static String ICH_CODE = "1286";
    /**
     * 千葉東葛農協
     */
    private final static String SB_CODE = "1287";

    private static final Logger logger = LoggerFactory.getLogger(MasninFunInsert.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        //copy 字段 SEQ_NO，SCONI1，SHSNI1，
        String masnin00File = params.get("MASNIN00_FILE","masnin00_file/masnin00_fun.tsv");

        String masninCode = params.get("MASNIN_CODE","masnin00_code_map/masnin00_fun.txt");

        String m28File = params.get("M28_FILE","C:\\SVN\\java\\kppDataMerge\\data\\txtData\\M28F.csv");

        boolean checkParamsResult = checkParams(masnin00File, masninCode, m28File);
        if (!checkParamsResult) {
            logger.error("params demo : " + "--MASNIN00_FILE masnin00.txt  \n" + "--MASNIN_CODE masnin00_fun.txt　\n" + "--M28_FILE M28F.txt　\n");
            return;
        }
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        DataSource<String> masninDs = env.readTextFile(masnin00File,CHARSET_NAME_31J);

        DataSet<Tuple2> masninCodeDs = env.readTextFile(masninCode).map(u -> {
            String[] split = u.split(",");
            Tuple2 tuple2 = new Tuple2<String, String>();
            tuple2.f0 = split[0];
            tuple2.f1 = split[1];
            return tuple2;
        }).returns(Types.TUPLE(Types.STRING, Types.STRING));

        List<Tuple2> list = masninCodeDs.filter(u -> ICH_CODE.equals(u.f0) || SB_CODE.equals(u.f0)).collect();
        if (list.size() != 2) {
            logger.error(" {} , {}  in {} not exists ! ", ICH_CODE, SB_CODE, masninCode);
            return;
        }
        Tuple2 tuple2List0 = list.get(0);
        Tuple2 tuple2List1 = list.get(1);

        String newIchCode = getNewCode(ICH_CODE, tuple2List0, tuple2List1);

        String newSbCode = getNewCode(SB_CODE, tuple2List0, tuple2List1);

        if ("-1".equals(newIchCode) || "-1".equals(newSbCode)) {
            logger.error(" {} , {}  in {} not exists ! ", ICH_CODE, SB_CODE, masninCode);
            return;
        }
        logger.debug("newIchCode is {}, newSbCode is {}", newIchCode, newSbCode);


        DataSet<Row> m28Ds = env.readTextFile(m28File, CHARSET_NAME_31J).filter(u -> !"".equals(u)).map(u -> {
            Row row = new Row(4);
            String[] split = u.split(",");
            String remark = split[split.length - 1];
            String remark27 = remark.length() > 21 ? remark.substring(20, 21) : "";
            row.setField(1, Long.valueOf(split[0]) + "");
            switch (remark27) {
                case "6":
                    row.setField(0, remark27);
                    row.setField(2, newIchCode);
                    break;
                case "7":
                    row.setField(0, remark27);
                    row.setField(2, newSbCode);
                    break;
                default:
                    row.setField(0, "-1");
                    break;
            }
            return row;
        }).filter(u -> !"-1".equals(u.getField(0)));

        DataSet<Row> conventDs = m28Ds.leftOuterJoin(masninCodeDs).where(u -> (String) u.getField(1)).equalTo(u -> (String) u.getField(0)).with((first, second) -> {
            if (second == null) {
                logger.error("m28 data and masninCode match fail ! data is {}", first);
            } else {
                first.setField(3, second.f1);
            }
            return first;
        });

        DataSet<String> result = masninDs.leftOuterJoin(conventDs).where(u -> u.split("\t")[5])
                .equalTo(u -> String.valueOf(u.getField(3))).with(((first, second) -> {
            String[] split = first.split("\t");
            if (second != null) {
                return split[0] + "\t" + split[5] + "\t" + second.getField(2) + "\t" + second;
            }
            if ("131307".equals(split[0])){
                logger.info(first);
            }
            return split[0] + "\t" + split[5] + "\t" + split[41];
        }));
        result.writeAsText("result/masnin00_result_fun.tsv", FileSystem.WriteMode.OVERWRITE);

        env.execute(MasFaxOldCode.class.getName() + "_" + getFormattedDate());
    }

    private static String getNewCode(String sbCode, Tuple2 tuple2List0, Tuple2 tuple2List1) {
        String newCode;
        if (sbCode.equals(tuple2List0.f0)) {
            newCode = (String) tuple2List0.f1;
        } else if (sbCode.equals(tuple2List1.f0)) {
            newCode = (String) tuple2List1.f1;
        } else {
            newCode = "-1";
        }
        return newCode;
    }

    private static boolean checkParams(String masnin00File, String masninCode, String m28File) {
        if (masnin00File == null) {
            logger.error("MASNIN00_FILE is null!");
            return false;
        }

        if (masninCode == null) {
            logger.error("MASNIN_CODE is null!");
            return false;
        }

        if (m28File == null) {
            logger.error("M28_FILE is null!");
            return false;
        }

        File txtFile = new File(masnin00File);
        if (!txtFile.isFile()) {
            logger.error("MASNIN00_FILE is not file");
            return false;
        }

        txtFile = new File(masninCode);
        if (!txtFile.isFile()) {
            logger.error("MASNIN_CODE is not file");
            return false;
        }

        txtFile = new File(m28File);
        if (!txtFile.isFile()) {
            logger.error("M28_FILE is not file");
            return false;
        }
        return true;
    }
}
