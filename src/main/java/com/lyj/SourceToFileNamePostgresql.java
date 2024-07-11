package com.lyj;

import com.lyj.util.ConfigLoader;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.lyj.util.ConfigLoader.DB_PROFILE;
import static com.lyj.util.FileSearchUtil.findDirectoriesContainingFile;
import static com.lyj.util.TableUtil.CHARSET_NAME_31J;
import static com.lyj.util.TableUtil.COL_CLASS;
import static com.lyj.util.TableUtil.COL_LENGTH;
import static com.lyj.util.TableUtil.COL_NAMES;
import static com.lyj.util.TableUtil.FILE_NAME;
import static com.lyj.util.TableUtil.I34;
import static com.lyj.util.TableUtil.M03;
import static com.lyj.util.TableUtil.NUMERIC_SCALE;
import static com.lyj.util.TableUtil.R05_DATE_SPLIT;
import static com.lyj.util.TableUtil.deleteDataByFileName;
import static com.lyj.util.TableUtil.getColumns;
import static com.lyj.util.TableUtil.getGroupName;
import static com.lyj.util.TableUtil.insertDB;
import static com.lyj.util.TableUtil.setFieldValue;

/**
 * source to postgresql
 */
public class SourceToFileNamePostgresql {

    private static final Logger logger = LoggerFactory.getLogger(SourceToFileNamePostgresql.class);

    private static final String ADD_SEQ = "ADD_SEQ";

    private static final String ADD_SEQ_AND_GROUP = "ADD_SEQ_AND_GROUP";

    private static final String ADD_SEQ_AND_JOIN_DAYS = "ADD_SEQ_AND_JOIN_DAYS";

    private static final String ADD_SEQ_AND_DATE = "ADD_SEQ_AND_DATE";

    private static final String NOW_DATE;

    static {
        // 获取当前时间的时间戳
        long currentTimeMillis = System.currentTimeMillis();

        // 创建日期对象
        Date date = new Date(currentTimeMillis);

        // 定义日期格式
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        NOW_DATE = dateFormat.format(date);
    }

    public static void main(String[] args) throws Exception {


        final ParameterTool params = ParameterTool.fromArgs(args);

        // 通过命令行参来选择配置文件

        String activeProfile = params.get(DB_PROFILE);

        String inputFilePath = params.get("input_file_path");

        // schema
        String schema = params.get("schema");

        String jobFile = params.get("job_file");

        boolean checkParamsResult = checkParams(activeProfile, inputFilePath, schema, jobFile);

        if (!checkParamsResult) {
            logger.error("params demo : " + "--db_profile dev43  \n" + "--input_file_path C:\\青果\\黄信中要的数据　\n" + "--schema xuexiaodingtest2 \n" + "--job_file C:\\青果\\job.txt");
            return;
        }
        ConfigLoader.loadConfiguration(activeProfile);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        boolean isTruncate = params.getBoolean("truncate", false);

        List<Tuple5> jobMap = Files.readAllLines(Paths.get(jobFile)).stream().map(u -> {
            Tuple5 tuple5 = new Tuple5();
            String[] split = u.split("\t", -1);
            for (int i = 0; i < split.length; i++) {
                if (i < 5) tuple5.setField(split[i], i);
            }
            return tuple5;
        }).collect(Collectors.toList());

        //处理只需要加seq的数据 addSeqJob
        Map<String, List<Tuple5>> addSeqJob = new HashMap<>();
        jobMap.stream().filter(u -> ADD_SEQ.equalsIgnoreCase((String) u.f4)).forEach(u -> {
            List<Tuple5> oldTupleList = addSeqJob.get((String) u.f1);
            if (oldTupleList == null) {
                List<Tuple5> tuple5List = new ArrayList<>();
                tuple5List.add(u);
                addSeqJob.put((String) u.f1, tuple5List);
            } else {
                oldTupleList.add(u);
            }
        });
        logger.info("addSeqJob value is {}", addSeqJob);

        Map<String, List<Tuple5>> addSeqAndGroupJob = new HashMap<>();
        //处理只需要加seq和支社名称的txt数据 addSeqAndGroupJob
        jobMap.stream().filter(u -> ADD_SEQ_AND_GROUP.equalsIgnoreCase((String) u.f4)).forEach(u -> {
            List<Tuple5> oldTupleList = addSeqAndGroupJob.get((String) u.f1);
            if (oldTupleList == null) {
                List<Tuple5> tuple5List = new ArrayList<>();
                tuple5List.add(u);
                addSeqAndGroupJob.put((String) u.f1, tuple5List);
            } else {
                oldTupleList.add(u);
            }
        });
        logger.info("addSeqAndGroupJob value is {}", addSeqAndGroupJob);

        //处理添加seq和年份的数据 addSeqAndYearJob
        Map<String, List<Tuple5>> addSeqAndYearJob = new HashMap<>();
        jobMap.stream().filter(u -> ADD_SEQ_AND_DATE.equalsIgnoreCase((String) u.f4)).forEach(u -> {
            List<Tuple5> oldTupleList = addSeqAndYearJob.get((String) u.f1);
            if (oldTupleList == null) {
                List<Tuple5> tuple5List = new ArrayList<>();
                tuple5List.add(u);
                addSeqAndYearJob.put((String) u.f1, tuple5List);
            } else {
                oldTupleList.add(u);
            }
        });
        logger.info("addSeqAndYearJob value is {}", addSeqAndYearJob);

        //处理添加seq和合并天的数据 addSeqAndJoinDaysJob
        Map<String, List<Tuple5>> addSeqAndJoinDaysTempJob = new HashMap<>();
        Map<String, List<Tuple5>> addSeqAndJoinDaysJob = new HashMap<>();
        jobMap.stream().filter(u -> ADD_SEQ_AND_JOIN_DAYS.equalsIgnoreCase((String) u.f4)).forEach(u -> {
            List<Tuple5> oldTupleList = addSeqAndJoinDaysTempJob.get((String) u.f1);
            if (oldTupleList == null) {
                List<Tuple5> tuple5List = new ArrayList<>();
                String fileListStr = (String) u.f2;
                String[] fileList = fileListStr.split(",");
                for (String file : fileList) {
                    tuple5List.add(new Tuple5(u.f0, u.f1, file, u.f3, u.f4));
                }
                addSeqAndJoinDaysTempJob.put((String) u.f1, tuple5List);
            } else {
                String fileListStr = (String) u.f2;
                String[] fileList = fileListStr.split(",");
                for (String file : fileList) {
                    oldTupleList.add(new Tuple5(u.f0, u.f1, file, u.f3, u.f4));
                }
            }
        });
        addSeqAndJoinDaysTempJob.forEach((tableName, tableInfo) -> {
            tableInfo.forEach(tuple5 -> {
                String f3 = (String) tuple5.f3;
                String f1 = (String) tuple5.f1;
                String f0 = (String) tuple5.f0;
                String groupId = f1.substring(f0.length(), f0.length() + 1);
                String groupName = getGroupName("H");
                if (!groupId.equalsIgnoreCase("_")) {
                    groupName = getGroupName(groupId);
                }
                List<String> directoriesContainingFile = findDirectoriesContainingFile(inputFilePath + File.separator + f3 + File.separator + groupName, (String) tuple5.f2);
                for (String filePath : directoriesContainingFile) {
                    List<Tuple5> oldTupleList = addSeqAndJoinDaysJob.get(tableName);
                    if (oldTupleList == null) {
                        List<Tuple5> tuple5List = new ArrayList<>();
                        Tuple5 newTuple5 = new Tuple5(tuple5.f0, tuple5.f1, groupName + filePath + File.separator + tuple5.f2, tuple5.f3, tuple5.f4);
                        tuple5List.add(newTuple5);
                        addSeqAndJoinDaysJob.put(tableName, tuple5List);
                    } else {
                        Tuple5 newTuple5 = new Tuple5(tuple5.f0, tuple5.f1, groupName + filePath + File.separator + tuple5.f2, tuple5.f3, tuple5.f4);
                        oldTupleList.add(newTuple5);
                    }
                }

            });
        });
        logger.info("addSeqAndJoinDaysJob value is {}", addSeqAndJoinDaysJob);


        List<Tuple2> errList = new ArrayList<>();

        //处理 addSeqJob
        insertJob(addSeqJob, schema, inputFilePath, env, errList, isTruncate);

        //处理 addSeqAndGroupJob
        insertJob(addSeqAndGroupJob, schema, inputFilePath, env, errList, "|", isTruncate);

        //处理 addSeqAndYearJob
        insertJob(addSeqAndYearJob, schema, inputFilePath, env, errList, isTruncate);

        //处理 addSeqAndJoinDaysJob
        insertJob(addSeqAndJoinDaysJob, schema, inputFilePath, env, errList, isTruncate);

        if (!errList.isEmpty()) {
            logger.error("error info is {}", errList);
        }

        env.execute(SourceToFileNamePostgresql.class.getName() + "_" + NOW_DATE);

    }

    private static void insertJob(Map<String, List<Tuple5>> groupJob, String schema, String inputFilePath, ExecutionEnvironment env, List<Tuple2> errList, String regexStr, boolean isTruncate) throws Exception {
        for (Map.Entry<String, List<Tuple5>> entry : groupJob.entrySet()) {
            String tableName = entry.getKey();
            List<Tuple5> tableInfo = entry.getValue();
            Map<String, List<String>> columns = null;
            if ("M03".equalsIgnoreCase((String) tableInfo.get(0).f0) || "M04".equalsIgnoreCase((String) tableInfo.get(0).f0)|| "M32".equalsIgnoreCase((String) tableInfo.get(0).f0)) {
                //有些文件没有日期，就是取最近的一份
                if (!isTruncate) {
                    //清空表数据
                    columns = getColumns(schema, tableName, true, true);
                }
            }
            if (columns == null) {
                columns = getColumns(schema, tableName, isTruncate, true);
            }
            DataSet<Row> allFileContents = getAllFileContents(schema, inputFilePath, env, errList, tableName, tableInfo, columns, regexStr);
            changeAndInsertDBData(schema, allFileContents, tableName, columns, regexStr);
        }
    }

    private static void insertJob(Map<String, List<Tuple5>> addSeqAndYearJob, String schema, String inputFilePath, ExecutionEnvironment env, List<Tuple2> errList, boolean isTruncate) throws Exception {
        insertJob(addSeqAndYearJob, schema, inputFilePath, env, errList, ",", isTruncate);
    }

    /**
     * get all table data
     * @param inputFilePath
     * @param env
     * @param errList
     * @param tableName
     * @param tableInfo
     * @param columns
     * @return
     * @throws Exception
     */
    private static DataSet<Row> getAllFileContents(String schema, String inputFilePath, ExecutionEnvironment env, List<Tuple2> errList, String tableName, List<Tuple5> tableInfo, Map<String, List<String>> columns, String regexStr) throws Exception {
        // 用于存储所有文件内容的 DataSet
        DataSet<Row> allFileContents = null;
        List<String> colClass = columns.get(COL_CLASS);
        List<String> colNames = columns.get(COL_NAMES);
        List<String> colLengths = columns.get(COL_LENGTH);
        List<String> numericScaleList = columns.get(NUMERIC_SCALE);
        List<String> newColName = null;
        if (tableName.contains("m041")) {
            String newTable = tableName.replace("m041", "m04");
            Map<String, List<String>> newColumns = getColumns(schema, newTable, false, true);
            newColName = newColumns.get(COL_NAMES);
        }
        if (!colClass.isEmpty()) {
            for (Tuple5 tuple5 : tableInfo) {
                String fileName = (String) tuple5.f2;
                String filePath = inputFilePath + File.separator + tuple5.f3 + File.separator + fileName;
                logger.info("read file path is {}", filePath);
                File readFilePath = new File(filePath);
                if (readFilePath.exists()) {
                    //除以小数位，只处理I34 和M03吗
                    if (!(I34.equalsIgnoreCase((String) tuple5.f0) || M03.equalsIgnoreCase((String) tuple5.f0))) {
                        List<String> array = numericScaleList.stream().map(u -> "0").collect(Collectors.toList());
                        columns.put("NUMERIC_SCALE", array);
                    }
                    FilterOperator<String> stringDataSource = env.readTextFile(filePath, CHARSET_NAME_31J).filter(line -> !"".equals(line));
                    // 合并 DataSet
                    //R05的数据是   202310  以前的逻辑是 是按顺序插入，202312 之后是
                    // 取  arr(0) & "," & arr(1) & "," & arr(2) & "," & arr(3) & "," & arr(4) & "," & arr(5) & "," & arr(6) &
                    // "," & arr(7) & "," & arr(8) & "," & 0 & "," & arr(18) & "," & arr(19) & ","& 0 & "," & arr(20) & ","
                    // & arr(21) & "," & arr(25) & "," & arr(29)
                    if ("R05".equalsIgnoreCase((String) tuple5.f0)) {
                        String f0 = (String) tuple5.f0;
                        int indexOf = f0.length();
                        String date = ((String) tuple5.f2).substring(indexOf + 1, indexOf + 5);
                        int dateInt = Integer.valueOf(date);
                        if (dateInt >= R05_DATE_SPLIT) {
                            MapOperator<String, String> mapOperator = stringDataSource.map(line -> {
                                String[] split = line.split(regexStr, -1);
                                StringBuilder lineStr = new StringBuilder();
                                if (split.length > 29) {
                                    for (int i = 0; i < 9; i++) {
                                        lineStr.append(split[i]).append(regexStr);
                                    }
                                    lineStr.append("0").append(regexStr).append(split[18]).append(regexStr).append(split[19]).append(regexStr).append("0").append(regexStr).append(split[20]).append(regexStr).append(split[21]).append(regexStr).append(split[25]).append(regexStr).append(split[29]);
                                    return lineStr.toString();
                                }
                                return line;
                            });

                            DataSet<Row> stringRowMapOperator = getStringRowMapOperator(mapOperator, schema, tableName, columns, regexStr, fileName);
                            if (allFileContents == null) {
                                allFileContents = stringRowMapOperator;
                            } else {
                                allFileContents = allFileContents.union(stringRowMapOperator);
                            }
                        } else {
                            DataSet<Row> stringRowMapOperator = getStringRowMapOperator(stringDataSource, schema, tableName, columns, regexStr, fileName);
                            if (allFileContents == null) {
                                allFileContents = stringRowMapOperator;
                            } else {
                                allFileContents = allFileContents.union(stringRowMapOperator);
                            }
                        }
                    } else if ("R27".equalsIgnoreCase((String) tuple5.f0)) {
                        String f0 = (String) tuple5.f0;
                        int indexOf = f0.length();
                        String date = ((String) tuple5.f2).substring(indexOf + 1, indexOf + 7);
                        MapOperator<String, String> mapOperator = stringDataSource.map(line -> {
                            String[] split = line.split(regexStr, -1);
                            StringBuilder lineStr = new StringBuilder();
                            for (int i = 0; i < split.length; i++) {
                                if (i == 3) {
                                    lineStr.append(date).append(regexStr);
                                }
                                lineStr.append(split[i]);
                                if (i < split.length - 1) lineStr.append(regexStr);
                            }
                            return lineStr.toString();
                        });
                        DataSet<Row> stringRowMapOperator = getStringRowMapOperator(mapOperator, schema, tableName, columns, regexStr, fileName);
                        if (allFileContents == null) {
                            allFileContents = stringRowMapOperator;
                        } else {
                            allFileContents = allFileContents.union(stringRowMapOperator);
                        }
                    } else if ("M041".equalsIgnoreCase((String) tuple5.f0)) {
                        String f0 = (String) tuple5.f0;
                        int indexOf = f0.length();
                        String date = "20" + ((String) tuple5.f2).substring(indexOf, indexOf + 6);
                        List<String> finalNewColName = newColName;
                        MapOperator<String, String> mapOperator = stringDataSource.map(new MapFunction<String, String>() {
                            @Override
                            public String map(String line) throws Exception {
                                String[] split = line.split(regexStr, -1);
                                StringBuilder lineStr = new StringBuilder();
                                if (finalNewColName != null) {
                                    lineStr.append(date).append(regexStr);
                                    for (int i = 1; i < colNames.size(); i++) {
                                        String colName = colNames.get(i);
                                        if (!FILE_NAME.equalsIgnoreCase(colName)) {
                                            int colIndex = getColIndex(finalNewColName, colName);
                                            lineStr.append(split[colIndex]);
                                            lineStr.append(regexStr);
                                        }
                                    }
                                }
                                return lineStr.substring(0, lineStr.length() - 1);
                            }

                            private int getColIndex(List<String> finalNewColName, String colName) {
                                for (int i = 0; i < finalNewColName.size(); i++) {
                                    String newColName = colName.replace("m041", "m04").replace("M041", "M04");
                                    if (newColName.equalsIgnoreCase(finalNewColName.get(i))) {
                                        return i;
                                    }
                                }
                                return -1;
                            }
                        });
                        DataSet<Row> stringRowMapOperator = getStringRowMapOperator(mapOperator, schema, tableName, columns, regexStr, fileName);
                        if (allFileContents == null) {
                            allFileContents = stringRowMapOperator;
                        } else {
                            allFileContents = allFileContents.union(stringRowMapOperator);
                        }
                    } else if (ADD_SEQ_AND_GROUP.equalsIgnoreCase((String) tuple5.f4)) {
                        //获取groupName
                        String f0 = (String) tuple5.f0;
                        int indexOf = f0.length();
                        String groupId = ((String) tuple5.f2).substring(indexOf, indexOf + 1);
                        String groupName = getGroupName(groupId);

                        MapOperator<String, String> mapOperator = stringDataSource.map(line -> {
                            StringBuilder sb = new StringBuilder();
                            sb.append(groupName).append(regexStr);
                            int subStringIndex = 0;
                            char[] chars = line.toCharArray();
                            for (int i = 2; i < colLengths.size(); i++) {
                                int colLength = Integer.valueOf(colLengths.get(i));
                                if (i < colLengths.size() - 1) {
                                    if (line.length() > subStringIndex + colLength) {
                                        char[] colChar = new char[colLength];
                                        for (int j = 0; j < colChar.length; j++) {
                                            colChar[j] = chars[subStringIndex + j];
                                        }
                                        sb.append(new String(colChar));
                                    } else {
                                        if (line.length() > subStringIndex) {
                                            char[] colChar = new char[colLength];
                                            for (int j = 0; j < colChar.length; j++) {
                                                if (subStringIndex + j < line.length())
                                                    colChar[j] = chars[subStringIndex + j];
                                            }
                                            sb.append(new String(colChar));
                                        }
                                    }
                                    sb.append(regexStr);
                                } else {
                                    if (line.length() > subStringIndex) {
                                        char[] colChar = new char[colLength];
                                        for (int j = 0; j < colChar.length; j++) {
                                            colChar[j] = chars[subStringIndex + j];
                                        }
                                        sb.append(new String(colChar));
                                    }
                                }
                                subStringIndex += colLength;
                            }
                            return sb.toString();
                        });
                        DataSet<Row> stringRowMapOperator = getStringRowMapOperator(mapOperator, schema, tableName, columns, regexStr, fileName);
                        if (allFileContents == null) {
                            allFileContents = stringRowMapOperator;
                        } else {
                            allFileContents = allFileContents.union(stringRowMapOperator);
                        }
                    } else if (ADD_SEQ_AND_DATE.equalsIgnoreCase((String) tuple5.f4)) {
                        //获取DATE
                        String f0 = (String) tuple5.f0;
                        int indexOf = f0.length();
                        String yearAndMon = ((String) tuple5.f2).substring(indexOf + 1, indexOf + 5);
                        String year = yearAndMon.substring(0, 2);
                        String mon = yearAndMon.substring(2, 4);
                        if ("03".equals(mon)) {
                            year = String.format("%02d", Integer.valueOf(year) - 1);
                        }
                        String date = "20" + year;
                        MapOperator<String, String> mapOperator = stringDataSource.map(u -> u + date);
                        DataSet<Row> stringRowMapOperator = getStringRowMapOperator(mapOperator, schema, tableName, columns, regexStr, fileName);
                        if (allFileContents == null) {
                            allFileContents = stringRowMapOperator;
                        } else {
                            allFileContents = allFileContents.union(stringRowMapOperator);
                        }
                    } else {
                        DataSet<Row> stringRowMapOperator = getStringRowMapOperator(stringDataSource, schema, tableName, columns, regexStr, fileName);
                        if (allFileContents == null) {
                            allFileContents = stringRowMapOperator;
                        } else {
                            allFileContents = allFileContents.union(stringRowMapOperator);
                        }
                    }
                } else {
                    Tuple2 tuple2 = new Tuple2();
                    tuple2.f0 = tuple5;
                    tuple2.f1 = filePath + " is not exists";
                    errList.add(tuple2);
                }
            }
        } else {
            Tuple2 tuple2 = new Tuple2();
            tuple2.f0 = tableName;
            tuple2.f1 = "table in db not found ";
            errList.add(tuple2);
        }
        return allFileContents;
    }

    private static void changeAndInsertDBData(String schema, DataSet<Row> insertData, String tableName, Map<String, List<String>> columns, String regexStr) throws SQLException {
        if (insertData != null) {
            List<String> colNames = columns.get(COL_NAMES);
            insertDB(schema, colNames, tableName, columns, insertData);
        }
    }

    private static MapOperator<String, Row> getStringRowMapOperator(DataSet<String> allFileContents, String schema, String tableName, Map<String, List<String>> columns, String regexStr, String fileName) throws SQLException {
        List<String> colNames = columns.get(COL_NAMES);
        List<String> colClass = columns.get(COL_CLASS);
        List<String> numericScaleList = columns.get(NUMERIC_SCALE);
        //拼接需要插入数据库的Row对象
        for (int i = 0; i < colNames.size(); i++) {
            if (FILE_NAME.equalsIgnoreCase(colNames.get(i))) {
                boolean execute = deleteDataByFileName(schema, tableName, fileName);
                if (execute) {
                    logger.debug("file_name is {} delete success! schema is {},tableName is {}", fileName, schema, tableName);
                }
            }
        }
        MapOperator<String, Row> insertData = allFileContents.map(new RichMapFunction<String, Row>() {
            private int index = 0;

            @Override
            public Row map(String line) throws Exception {
                index++;
                String regex = regexStr;
                if ("|".equals(regexStr)) {
                    regex = "\\|";
                }
                String[] split = line.split(regex, -1);
                Row row = new Row(colNames.size());
                //set seq
                int tableIndex = 0;
                if (colNames.get(0).contains("レコード") || "seq_no".equalsIgnoreCase(colNames.get(0))) {
                    setFieldValue(row, 0, colClass.get(0), String.valueOf(index), numericScaleList.get(0),tableName);
                    tableIndex = 1;
                }
                for (int i = tableIndex; i < colNames.size(); i++) {
                    String numericScale = numericScaleList.get(i);
                    String colName = colNames.get(i);
                    if (i > split.length) {
                        //处理共同字段
                        if (colName.equalsIgnoreCase("insert_job_id") || colName.equalsIgnoreCase("insert_pro_id") || colName.equalsIgnoreCase("upd_user_id") || colName.equalsIgnoreCase("upd_job_id") || colName.equalsIgnoreCase("upd_pro_id")) {
                            setFieldValue(row, i, colClass.get(i), "", numericScale,tableName);
                        } else if (colName.equalsIgnoreCase("insert_user_id") || colName.equalsIgnoreCase("partition_flag")) {
                            setFieldValue(row, i, colClass.get(i), tableName, numericScale,tableName);
                        } else if (colName.equalsIgnoreCase("upd_sys_date") || colName.equalsIgnoreCase("insert_sys_date")) {
                            setFieldValue(row, i, colClass.get(i), NOW_DATE, numericScale,tableName);
                        } else if (colName.equalsIgnoreCase(FILE_NAME)) {
                            setFieldValue(row, i, colClass.get(i), fileName, numericScale,tableName);
                        } else {
                            setFieldValue(row, i, colClass.get(i), "", numericScale,tableName);
                        }
                    } else {
                        if (tableIndex == 0) {
                            if (colName.equalsIgnoreCase(FILE_NAME)) {
                                setFieldValue(row, i, colClass.get(i), fileName, numericScale,tableName);
                            } else if (i == split.length) {
                                setFieldValue(row, i, colClass.get(i), "", numericScale,tableName);
                            } else {
                                setFieldValue(row, i, colClass.get(i), split[i].trim(), numericScale,tableName);
                            }
                        } else {
                            if (colName.equalsIgnoreCase(FILE_NAME)) {
                                setFieldValue(row, i, colClass.get(i), fileName, numericScale,tableName);
                            } else {
                                setFieldValue(row, i, colClass.get(i), split[i - 1].trim(), numericScale,tableName);
                            }
                        }
                    }
                }
                return row;
            }
        }).setParallelism(1);
        return insertData;
    }


    private static boolean checkParams(String activeProfile, String inputFilePath, String schema, String jobFile) {
        if (activeProfile == null) {
            logger.error("db_profile is null!");
            return false;
        }

        if (schema == null) {
            logger.error("schema is null!");
            return false;
        }

        if (jobFile == null) {
            logger.error("job_file is null!");
            return false;
        }

        File txtFile = new File(jobFile);
        if (!txtFile.isFile()) {
            logger.error("job_file is not file");
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
