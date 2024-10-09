package com.lyj.check;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.types.Row;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;

import static com.lyj.MasNinTsvToPostGre.getDataRowList;
import static com.lyj.util.TableUtil.getFormattedDate;

/**
 * MSCBSM00（取引先メール送信マスタ） 検査
 */
public class Mscbsm00Check {

    private static final Logger logger = LoggerFactory.getLogger(Mscbsm00Check.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        String mscbsm00File = params.get("mscbsm00_file");

        String masnin00CodeMap = params.get("masnin00_code_map");

        String maskai00CodeMap = params.get("maskai00_code_map");

        String funMaskai00CodeMap = params.get("fun_maskai00_code_map");

        String sheetName = params.get("sheet_name");


        String qa1028File = params.get("QA1028_file");

        boolean checkParamsResult = checkParams(mscbsm00File, masnin00CodeMap, maskai00CodeMap, qa1028File, sheetName);

        if (!checkParamsResult) {
            logger.error("params demo : " + "--mscbsm00_file 取引先メール送信マスタ_ICH.xlsx  \n" + "--masnin00_code_map masnin00_ich.txt　\n" + "--maskai00_code_map m27_ich.txt \n" + "--QA1028_file QA1028_2_取引先メール送信マスタデータ登録関連のご確認依頼.xlsx \n" + "--sheetName 市川 \n");
            return;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 使用 Apache POI 读取 inputFile Excel 文件
        InputStream inputStream = new FileInputStream(mscbsm00File);
        Workbook workbook = WorkbookFactory.create(inputStream);
        // sheet
        final List<Row> dataRowList = getDataRowList(workbook, sheetName, mscbsm00File, 11, "B:F");
        logger.info("sheet1 name is {} ，data size is {}", sheetName, dataRowList.size());
        // 关闭资源
        workbook.close();
        inputStream.close();

        // 使用 Apache POI 读取 inputFile Excel 文件
        inputStream = new FileInputStream(qa1028File);
        workbook = WorkbookFactory.create(inputStream);
        // sheet
        final List<Row> resultRowList = getDataRowList(workbook, "一覧表", qa1028File, 2, "A:D");
        logger.info("sheet1 name is {} ，data size is {}", "一覧表", resultRowList.size());
        // 关闭资源
        workbook.close();
        inputStream.close();

        DataSource<Row> rowDataSource = env.fromCollection(dataRowList);
        DataSource<Row> resultDataSource = env.fromCollection(resultRowList);

        DataSet<Row> resultDs = resultDataSource.filter(row -> sheetName.equals(row.getField(0)));

        DataSet<Row> ninDataSet = rowDataSource.filter(u -> "D1".equals(u.getField(2)));
        DataSet<Row> kaiDataSet = rowDataSource.filter(u -> "D2".equals(u.getField(2)));

        DataSet<Tuple2<String, String>> masnin00Ds = env.readTextFile(masnin00CodeMap).map(u -> {
            Tuple2<String, String> tuple2 = new Tuple2();
            String[] split = u.split(",");
            tuple2.f0 = split[0];
            tuple2.f1 = split[1];
            return tuple2;
        }).returns(Types.TUPLE(Types.STRING, Types.STRING));

        DataSet<Tuple2<String, String>> maskai00Ds = env.readTextFile(maskai00CodeMap).map(u -> {
            Tuple2<String, String> tuple2 = new Tuple2();
            String[] split = u.split(",");
            tuple2.f0 = split[0];
            tuple2.f1 = split[1];
            return tuple2;
        }).returns(Types.TUPLE(Types.STRING, Types.STRING));

        if (funMaskai00CodeMap != null) {
            DataSet<Tuple2<String, String>> maskai00FumDs = env.readTextFile(funMaskai00CodeMap).map(u -> {
                Tuple2<String, String> tuple2 = new Tuple2();
                String[] split = u.split(",");
                tuple2.f0 = split[0];
                tuple2.f1 = split[1];
                return tuple2;
            }).returns(Types.TUPLE(Types.STRING, Types.STRING));;
            maskai00Ds = maskai00Ds.union(maskai00FumDs);
        }
        DataSet<Row> resultNin = ninDataSet.leftOuterJoin(masnin00Ds).where(u -> String.valueOf(u.getField(3)))
                .equalTo(u -> u.getField(1).toString()).with((first, second) -> {
                    Row row = new Row(first.getArity() + 1);
                    row.setField(0,second != null ? second.getField(0) : null);
                    for (int i = 0; i < first.getArity(); i++) {
                        row.setField(i + 1, first.getField(i));
                    }
                    return row;
                }).leftOuterJoin(resultDs).where(u -> String.valueOf(u.getField(0))).equalTo(u -> String.valueOf(u.getField(3))).with(((first, second) -> {
                    Row row = new Row(first.getArity() + 1);
                    row.setField(0, second != null ? second.getField(0) : null);
                    for (int i = 0; i < first.getArity(); i++) {
                        row.setField(i + 1, first.getField(i));
                    }
                    return row;
                }));


        DataSet<Row> resultKai = kaiDataSet.leftOuterJoin(maskai00Ds).where(u -> String.valueOf(u.getField(3)))
                .equalTo(u -> u.getField(1).toString()).with((first, second) -> {
                    Row row = new Row(first.getArity() + 1);
                    row.setField(0, second != null ? second.getField(0) : null);
                    for (int i = 0; i < first.getArity(); i++) {
                        row.setField(i + 1, first.getField(i));
                    }
                    return row;
                }).leftOuterJoin(resultDs).where(u -> String.valueOf(u.getField(0)))
                .equalTo(u -> String.valueOf(u.getField(3))).with(((first, second) -> {
                    Row row = new Row(first.getArity() + 1);
                    row.setField(0,second != null ? second.getField(0) : null);
                    for (int i = 0; i < first.getArity(); i++) {
                        row.setField(i + 1, first.getField(i));
                    }
                    return row;
                }));
//        logger.info("resultKai count is {}", resultKai.filter(u -> u.getField(0) == null).count());
//        logger.info("resultNin count is {}", resultNin.filter(u -> u.getField(0) == null).count());
        resultNin.union(resultKai).filter(u -> u.getField(0) == null).writeAsText("result/result.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute(Mscbsm00Check.class.getName() + "_" + getFormattedDate());

    }

    private static boolean checkParams(String mscbsm00File, String masnin00CodeMap, String maskai00CodeMap, String qa1028File, String sheetName) {

        if (mscbsm00File == null) {
            logger.error("mscbsm00_file is null!");
            return false;
        }

        if (sheetName == null) {
            logger.error("sheet_name is null!");
            return false;
        }

        if (maskai00CodeMap == null) {
            logger.error("maskai00_code_map is null!");
            return false;
        }

        if (qa1028File == null) {
            logger.error("QA1028_file is null!");
            return false;
        }

        if (qa1028File == null) {
            logger.error("QA1028_file is null!");
            return false;
        }

        File txtFile = new File(mscbsm00File);
        if (!txtFile.isFile()) {
            logger.error("mscbsm00_file is not file");
            return false;
        }

        txtFile = new File(masnin00CodeMap);
        if (!txtFile.isFile()) {
            logger.error("masnin00_code_map is not file");
            return false;
        }

        txtFile = new File(maskai00CodeMap);
        if (!txtFile.isFile()) {
            logger.error("maskai00_code_map is not file");
            return false;
        }

        txtFile = new File(qa1028File);
        if (!txtFile.isFile()) {
            logger.error("QA1028_file is not file");
            return false;
        }
        return true;
    }
}
