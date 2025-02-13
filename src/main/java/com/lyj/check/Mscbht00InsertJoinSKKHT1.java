package com.lyj.check;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static com.lyj.util.TableUtil.CHARSET_NAME_31J;
import static com.lyj.util.TableUtil.getFormattedDate;

/**
 * 発着地マスタ
 * 需要替换 SKKHT1 請求間隔区分   用老文件里的請求間隔区分    替换新生成的文件
 */
public class Mscbht00InsertJoinSKKHT1 {


    private static final Logger logger = LoggerFactory.getLogger(Mscbht00InsertJoinSKKHT1.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        String oldFile = params.get("OLD_FILE");

        String newFile = params.get("NEW_FILE");


        if (checkParams(oldFile, newFile)) return;


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataSet<String> oldFileDs = env.readTextFile(oldFile, CHARSET_NAME_31J).filter(u -> !"".equals(u));

        DataSet<String> newFileDs = env.readTextFile(newFile, CHARSET_NAME_31J).filter(u -> !"".equals(u));

        JoinOperator<String, String, String> result = newFileDs.leftOuterJoin(oldFileDs).where(u -> {
            String[] split = u.split("\t");
            return split[2];
        }).equalTo(u -> {
            String[] split = u.split("\t");
            return split[2];
        }).with(((first, second) -> {
            if (second != null) {
                String[] split = second.split("\t");
                logger.error("match old data fail data is {}", first);
                return first + "\t" + split[split.length - 1];
            } else {

                return first + "\t";
            }
        }));

        result.writeAsText("result/mscbht_skkht1.tsv", FileSystem.WriteMode.OVERWRITE);

        env.execute(Mscbht00InsertJoinSKKHT1.class.getName() + "_" + getFormattedDate());
    }

    private static boolean checkParams(String oldFile, String newFile) {
        if (oldFile == null) {
            logger.error("OLD_FILE is null!");
            return true;
        }

        File txtFile = new File(oldFile);
        if (!txtFile.isFile()) {
            logger.error("OLD_FILE is not file");
            return true;
        }

        if (newFile == null) {
            logger.error("NEW_FILE is null!");
            return true;
        }

        txtFile = new File(newFile);
        if (!txtFile.isFile()) {
            logger.error("NEW_FILE is not file");
            return true;
        }

        return false;
    }

}
