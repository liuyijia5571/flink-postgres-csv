package com.example;

import com.example.util.TableUtil;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static com.example.util.TableUtil.*;

public class Mashin00ToPostgreSQL {


    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("args size < 2");
            System.exit(0);
            return;
        }

        String schemaName = args[0];
        String tableName = args[1];

        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行数
//        env.setParallelism(1);

        Map<String, List<String>> columns = getColumns(schemaName, tableName, true);
        List<String> colNames = columns.get("COL_NAMES");
        List<String> colClasses = columns.get("COL_CLASS");
        String insertSql = getInsertSql(colNames, schemaName, tableName);
        System.out.println(insertSql);

        // CSV 文件路径
        String folderPath = "input";
        File folder = new File(folderPath);
        if (folder.exists()) {
            File[] files = folder.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (!file.isDirectory()) {

                        // 读取 CSV 文件并创建 DataStream
                        String csvFilePath = folderPath + "/" + file.getName();

                        // 读取 CSV 文件并创建 DataStream
                        DataStreamSource<String> csvDataStream = env
                                .readTextFile(csvFilePath, "Shift-JIS");

                        // 将数据写入 PostgreSQL 数据库
                        csvDataStream.addSink(JdbcSink.sink(
                                insertSql,
                                (ps, t) -> {
                                    // 对每个数据元素进行写入操作
                                    String[] datas = t.split("\t");
                                    for (int i = 0; i < colNames.size(); i++) {
                                        String colName = colNames.get(i);
                                        String colClass = colClasses.get(i);
                                        //处理共同字段
//                                        if (colName.equalsIgnoreCase("insert_job_id") ||
//                                                colName.equalsIgnoreCase("insert_pro_id") ||
//                                                colName.equalsIgnoreCase("upd_user_id") ||
//                                                colName.equalsIgnoreCase("upd_job_id") ||
//                                                colName.equalsIgnoreCase("upd_pro_id")
//                                        ) {
//                                            setPsData(i+1 , colName, colClass, "", ps);
//                                        } else if (colName.equalsIgnoreCase("insert_user_id") ||
//                                                colName.equalsIgnoreCase("partition_flag")
//                                        ) {
//                                            setPsData(i+1, colName, colClass, tableName.toUpperCase(), ps);
//                                        } else if (colName.equalsIgnoreCase("upd_sys_date") ||
//                                                colName.equalsIgnoreCase("insert_sys_date")) {
//                                            setPsData(i+1, colName, colClass, "1990-01-01 00:00:00", ps);
//                                        } else {
//                                            if (i < datas.length) {
                                                setPsData(i+1 , colName, colClass, datas[i], ps,tableName);
//                                            } else {
//                                                setPsData(i+1, colName, colClass, "", ps);
//                                            }
//                                        }
                                    }
                                },
                                jdbcExecutionOptions,
                                getConnectionOptions()
                        ));
                    }
                }
            }
        }
        env.execute("Flink Mashin00ToPostgreSQL job");
    }

    private static void setPsData(int parameterIndex, String colName, String colClass, String dataValue, PreparedStatement ps,String tableName) throws SQLException {
//        //SKNHN1 前面补0
//        if (colName.equalsIgnoreCase("SKNHN1")) {
//            StringBuilder result = new StringBuilder();
//            int length = dataValue.length();
//            for (int i = 0; i < 48 - length; i++) {
//                result.append(0);
//            }
//            result.append(dataValue);
//            dataValue = result.toString();
//        }
        TableUtil.setPsData(parameterIndex, colName, colClass, dataValue, ps,tableName);
    }
}