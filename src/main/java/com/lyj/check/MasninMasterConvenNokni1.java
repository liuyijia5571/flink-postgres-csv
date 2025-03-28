package com.lyj.check;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static com.lyj.util.TableUtil.CHARSET_NAME_31J;
import static com.lyj.util.TableUtil.getFormattedDate;

/**
 *農協マスタの9999999のデータを登録しないです。　4523
 *需要各个荷主master 删掉荷主code 为9999999 和農協集約荷主コード 为 9999999 改为荷主code
 */
public class MasninMasterConvenNokni1 {


    private static final Logger logger = LoggerFactory.getLogger(MasninMasterConvenNokni1.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        String masnin00File = params.get("MASNIN00_FILE", "masnin00_file/masnin00_ich.tsv");

        if (masnin00File == null) {
            logger.error("MASNIN00_FILE is null");
            return;
        }

        File file = new File(masnin00File);
        if (!file.isFile()) {
            logger.error("MASSER00_FILE is not file");
            return;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        MapOperator<String, String> resultDs = env.readTextFile(masnin00File,CHARSET_NAME_31J).map(u -> {
            String[] split = u.split("\t");
            if ("9999999".equals(split[50])) {
                split[50] = split[5];
            }
            return String.join("\t", split);
        });

        resultDs.writeAsText("result/result_masnin.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute(MasninMasterConvenNokni1.class.getName() + "_" + getFormattedDate());

    }
}
