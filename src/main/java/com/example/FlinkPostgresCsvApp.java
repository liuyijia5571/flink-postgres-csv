package com.example;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.source.SourceFunction;


import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static com.example.util.TableUtil.DB_PASSWORD;
import static com.example.util.TableUtil.DB_USER;

public class FlinkPostgresCsvApp {

    private static final String DB_URL = "jdbc:postgresql://192.168.166.168:5432/postgres";

    static Map<String, String> siksmMap = new HashMap();

    static {
        //长应
        //长野
        siksmMap.put("CHO", "21");
        // 須坂支社
        siksmMap.put("SUZ", "22");
        //中野支社
        siksmMap.put("NAK", "23");
        // 船桥
        siksmMap.put("FUN", "34");
        //市川支社
        siksmMap.put("ICH", "35");
        //ながのファイム                          FAM                     71
        //长应 的数据按联合规则放到联合表里去
        //联合
//        上　田			                UED		10
//        松　本			                MAT		11
//        諏　訪			                SUW	            12
//        佐　久			                SAK		            13
//        県央青			                TAK		            33
//        流通Ｃ                                       NRC                      70

    }
    public static void deleteFolder(File folder) {
        File[] files = folder.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteFolder(file);
                } else {
                    file.delete();
                }
            }
        }
        folder.delete();
    }

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Set the parallelism to 1 to ensure all data goes to a single file
        env.setParallelism(1);
        String folderPath = "output";

        File folder = new File(folderPath);
        if (folder.exists()) {
            deleteFolder(folder);
            System.out.println("Folder deleted successfully.");
        } else {
            System.out.println("Folder does not exist.");
        }

        // Get all table names
        Map<String, String> tableNames = getAllTableNames();

        // Process each table
        for (String tableName : tableNames.keySet()) {
            String code = tableNames.get(tableName);
            if ("1".equals(code)) {
                // Create a custom source function to read data from each table
                SourceFunction<Tuple2<String, String>> sourceFunction = new PostgresTableSource(tableName, tableNames.get(tableName),null);

                // Add the source function to the execution environment
                DataStream<Tuple2<String, String>> dataStream = env.addSource(sourceFunction);

                dataStream.writeAsCsv("output/" + tableName  + ".csv");
            }else {
                for (String siksmKey : siksmMap.keySet()) {
                    // Create a custom source function to read data from each table
                    SourceFunction<Tuple2<String, String>> sourceFunction = new PostgresTableSource(tableName, tableNames.get(tableName), siksmMap.get(siksmKey));

                    // Add the source function to the execution environment
                    DataStream<Tuple2<String, String>> dataStream = env.addSource(sourceFunction);
                    // Set the parallelism to 1 to ensure all data goes to a single file
                    dataStream.writeAsCsv("output/" + siksmKey  + "_" + tableName + ".csv");

                }
            }

        }

        // Execute the Flink job
        env.execute("Flink PostgreSQL to CSV Job");
    }

    private static Map<String, String> getAllTableNames() throws Exception {
        Map<String, String> tableNames = new HashMap<>();
        tableNames.put("ENKFILE0","SIKHN1");
//        tableNames.put("KAIFIL00","SIKKF1");
//        tableNames.put("KAISYO00","SIKKR1");
//        tableNames.put("KANFIL20","SIKSS1");
//        tableNames.put("KANFILD0","SIKNS1");
//        tableNames.put("SUMALL00","SIKSM1");
//        tableNames.put("SUMKIC00","1");
//        tableNames.put("SUMURC00","1");
//        tableNames.put("SUMURI00","SIKUR1");
//        tableNames.put("SYKENKB0","SIKSR1");
//        tableNames.put("SYKHANB0","SIKUR1");
//        tableNames.put("SYKTKOB0","SIKTK1");
//        tableNames.put("SYKURIB0","SIKUB1");
//        tableNames.put("SYKZAIB0","SIKTB1");
//        tableNames.put("SYKZATB0","SIKTB1");
//        tableNames.put("TKIFILT0","SIKTO1");
//        tableNames.put("TKIFILU0","SIKHA1");
//        tableNames.put("TKIHANM0","SIKUK1");
//        tableNames.put("TKITOKH0","SIKTH1");
//        tableNames.put("TKITOKM0","SIKTK1");

        return tableNames;
    }

    private static class PostgresTableSource implements SourceFunction<Tuple2<String, String>> {
        private volatile boolean isRunning = true;
        private final String tableName;
        private final String code;
        private final String sikmValue;

        public PostgresTableSource(String tableName, String code, String sikmValue) {
            this.tableName = tableName;
            this.code = code;
            this.sikmValue = sikmValue;
        }

        @Override
        public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
            Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            Statement stmt = conn.createStatement();
            String sql = "SELECT * FROM xuexiaodingtest." + tableName ;
            if (null != sikmValue )
                sql+= " where " + code + " = '" + sikmValue + "'";
            ResultSet rs = stmt.executeQuery(sql);

            int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next() && isRunning) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= columnCount; i++) {
                    sb.append(rs.getString(i));
                    if (i < columnCount) {
                        sb.append(",");
                    }
                }
                ctx.collect(new Tuple2<>(tableName, sb.toString()));
            }

            rs.close();
            stmt.close();
            conn.close();
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

}