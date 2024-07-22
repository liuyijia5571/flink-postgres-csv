package com.lyj;

import com.lyj.util.ConfigLoader;
import com.lyj.util.ExcelReaderTask;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.lyj.util.ConfigLoader.DB_PROFILE;
import static com.lyj.util.TableUtil.getFormattedDate;
import static com.lyj.util.TableUtil.COL_CLASS;
import static com.lyj.util.TableUtil.COL_NAMES;
import static com.lyj.util.TableUtil.FILE_NAME;
import static com.lyj.util.TableUtil.deleteDataByFileName;
import static com.lyj.util.TableUtil.getColumns;
import static com.lyj.util.TableUtil.insertDB;
import static com.lyj.util.TableUtil.setFieldValue;

public class U16ToFileNamePostGreSql {

    private static final Logger logger = LoggerFactory.getLogger(U16ToFileNamePostGreSql.class);

    private static final String U16_TABLE_NAME = "u16_prod_db";


    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool params = ParameterTool.fromArgs(args);
        // 通过命令行参来选择配置文件

        String activeProfile = params.get(DB_PROFILE);

        // schema
        String schema = params.get("schema");

        String inputPath = params.get("input_file_path");

        boolean checkParamsResult = checkParams(activeProfile, schema, inputPath);

        boolean isTruncate = params.getBoolean("truncate", false);

        if (!checkParamsResult) {
            logger.error("params demo : " + "--db_profile dev43  \n" + "--input_file_path C:\\青果\\黄信中要的数据　\n" + "--schema xuexiaodingtest2 \n");
            return;
        }

        ConfigLoader.loadConfiguration(activeProfile);

        Map<String, List<String>> columns = getColumns(schema, U16_TABLE_NAME, isTruncate, true);

        int numThreads = Runtime.getRuntime().availableProcessors(); // 设置线程数，最好根据 CPU 核心数来动态调整

        logger.debug("nThreads is {}", numThreads);

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<List>> futures = new ArrayList<>();
        File directory = new File(inputPath);
        File[] files = directory.listFiles();
        DataSet<Row> newDataDs = null;
        for (int i = 0; i < files.length; i++) {
            File file = files[i];
            if (file.isFile() && (file.getName().contains(".xls") || file.getName().contains(".xlsx"))) {
                Callable<List> task = new ExcelReaderTask(inputPath, file.getName(), columns);
                Future<List> future = executor.submit(task);
                futures.add(future);
            }
        }

        // 等待所有任务完成
        List<List> allData = new ArrayList<>();
        for (int i = 0; i < futures.size(); i++) {
            List data = futures.get(i).get();
            allData.add(data);
        }

        executor.shutdown();

        for (List data : allData) {
            List<Row> dataList = (List<Row>) data.get(0);
            List<Row> maShinList = (List<Row>) data.get(1);
            String fileName = (String) data.get(2);

            if (!dataList.isEmpty() && !maShinList.isEmpty()) {
                DataSource<Row> dataDs = env.fromCollection(dataList);
                DataSet<Row> maShinDs = env.fromCollection(maShinList).distinct().setParallelism(1);

                DataSet<Row> temp = dataDs.leftOuterJoin(maShinDs).where(row -> {
                    String field = (String) row.getField(3);
                    return field.trim();
                }).equalTo(row -> {
                    String field = (String) row.getField(0);
                    return field.trim();
                }).with((row1, row2) -> {
                    if (row2 != null) {
                        row1.setField(4, row2.getField(1));
                    } else {
                        logger.error("data size is {},row is {}", dataList.size(), row1);
                    }
                    return row1;
                });
                DataSet<Row> rowDataSet = addSeq(schema, temp, U16_TABLE_NAME, fileName, columns);
                if (newDataDs == null) {
                    newDataDs = rowDataSet;
                } else {
                    newDataDs = newDataDs.union(rowDataSet);
                }
            }

        }
        if (newDataDs != null) {
            List<String> colNames = columns.get(COL_NAMES);
            insertDB(schema, colNames, U16_TABLE_NAME, columns, newDataDs);
            String formattedDate = getFormattedDate();
            env.execute(U16ToFileNamePostGreSql.class.getName() + "_" + formattedDate);
        }
    }


    private static DataSet<Row> addSeq(String schema, DataSet<Row> newDataDs, String tableName, String fileName, Map<String, List<String>> columns) {
        List<String> colNames = columns.get(COL_NAMES);
        List<String> colClass = columns.get(COL_CLASS);

        //拼接需要插入数据库的Row对象
        for (int i = 0; i < colNames.size(); i++) {
            if (FILE_NAME.equalsIgnoreCase(colNames.get(i))) {
                boolean execute = deleteDataByFileName(schema, tableName, fileName);
                if (execute) {
                    logger.debug("file_name is {} delete success! schema is {},tableName is {}", fileName, schema, tableName);
                }
                break;
            }
        }

        DataSet<Row> insertData = newDataDs.map(new RichMapFunction<Row, Row>() {
            private int index = 0;

            @Override
            public Row map(Row line) throws Exception {
                index++;
                Row row = new Row(colNames.size());
                //set seq
                int tableIndex = 0;
                if (colNames.get(0).contains("レコード") || "seq_no".equalsIgnoreCase(colNames.get(0))) {
                    setFieldValue(row, 0, colClass.get(0), String.valueOf(index), tableName);
                    tableIndex = 1;
                }
                for (int i = tableIndex; i < colNames.size(); i++) {
                    if (tableIndex == 1) {
                        setFieldValue(row, i, colClass.get(i), (String) line.getField(i - 1), tableName);
                    } else {
                        setFieldValue(row, i, colClass.get(i), (String) line.getField(i), tableName);
                    }
                }
                return row;
            }
        }).setParallelism(1);
        return insertData;
    }


    private static boolean checkParams(String activeProfile, String schema, String inputFilePath) {
        if (activeProfile == null) {
            logger.error("db_profile is null!");
            return false;
        }

        if (schema == null) {
            logger.error("schema is null!");
            return false;
        }

        if (inputFilePath == null) {
            logger.error("input_file_path is null!");
            return false;
        }

        File resultFile = new File(inputFilePath);
        if (!resultFile.isDirectory()) {
            logger.error("input_file_path is not directory");
            return false;
        }
        return true;
    }
}

