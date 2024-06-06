package com.example;


import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.source.SourceFunction;


import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

import static com.example.util.TableUtil.*;

public class FlinkPostgresCsvApp {

    private static final String DB_URL = "jdbc:postgresql://192.168.166.168:5432/postgres";

    static Map<String, String> siksmMap = new HashMap();
    static List<String> subTable = new ArrayList();
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
        siksmMap.put("ICH", "35")
        ;
        subTable.add("KANFIL20");
        subTable.add("TKIFILU0");
        subTable.add("TKIHANM0");
        subTable.add("TKITOKM0");
        subTable.add("SUMURI00");
        subTable.add("KAISYO00");

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
            Map<String, List<String>> columns = getColumns(DB_URL, "xuexiaodingtest", tableName.toLowerCase());
            List<String> colNames = columns.get("COL_NAMES");
            String colStr = colNames.stream().reduce((s1, s2) -> s1 + "," + s2).orElse(null);
            if (subTable.contains(tableName)) {
                String[] splitTable = code.split("\n");
                for (int i = 0; i < splitTable.length; i++) {
                    // Create a custom source function to read data from each table
                    String[] splitWhere = splitTable[i].split("->");

                    SourceFunction<Tuple1<String>> sourceFunction = new PostgresTableSource(tableName, code,null,colStr ,splitWhere[0]);

                    // Add the source function to the execution environment
                    DataStream<Tuple1<String>> dataStream = env.addSource(sourceFunction);
                    String fileName = splitWhere[1].replace(".", "_").toUpperCase();

                    dataStream.writeAsCsv("output/" + fileName  + ".csv");
                }
            }else{
                if ("1".equals(code)) {
                    // Create a custom source function to read data from each table
                    SourceFunction<Tuple1<String>> sourceFunction = new PostgresTableSource(tableName, code,null,colStr,null);

                    // Add the source function to the execution environment
                    DataStream<Tuple1<String>> dataStream = env.addSource(sourceFunction);

                    dataStream.writeAsCsv("output/RENDAYALL_" + tableName  + ".csv");
                }else {
                    for (String siksmKey : siksmMap.keySet()) {
                        // Create a custom source function to read data from each table
                        SourceFunction<Tuple1<String>> sourceFunction = new PostgresTableSource(tableName, code, siksmMap.get(siksmKey), colStr,null);

                        // Add the source function to the execution environment
                        DataStream<Tuple1<String>> dataStream = env.addSource(sourceFunction);
                        // Set the parallelism to 1 to ensure all data goes to a single file
                        if (tableName.equalsIgnoreCase("KANFILD0")) {
                            dataStream.writeAsCsv("output/RENBAK" + siksmKey + "_" + tableName + ".csv");
                        }else {
                            dataStream.writeAsCsv("output/RENDAY" + siksmKey + "_" + tableName + ".csv");
                        }
                    }
                }
            }
        }

        // Execute the Flink job
        env.execute("Flink PostgreSQL to CSV Job");
    }

    private static Map<String, String> getAllTableNames() throws Exception {
        Map<String, String> tableNames = new HashMap<>();
        tableNames.put("ENKFILE0","SIKHN1");
        tableNames.put("KAIFIL00","SIKKF1");
        tableNames.put("KAISYO00","SIKKR1 = '21' and TDAKR1 >= 220101 and TDAKR1 <= 221231->worcho22.kaisyo00\n" +
                "SIKKR1 = '22' and TDAKR1 >= 220101 and TDAKR1 <= 221231->worsuz22.kaisyo00\n" +
                "SIKKR1 = '23' and TDAKR1 >= 220101 and TDAKR1 <= 221231->wornak22.kaisyo00\n" +
                "SIKKR1 = '34' and TDAKR1 >= 220101 and TDAKR1 <= 221231->worfun22.kaisyo00\n" +
                "SIKKR1 = '35' and TDAKR1 >= 220101 and TDAKR1 <= 221231->worich22.kaisyo00\n" +
                "SIKKR1 = '21' and TDAKR1 >= 230101 and TDAKR1 <= 231231->worcho23.kaisyo00\n" +
                "SIKKR1 = '22' and TDAKR1 >= 230101 and TDAKR1 <= 231231->worsuz23.kaisyo00\n" +
                "SIKKR1 = '23' and TDAKR1 >= 230101 and TDAKR1 <= 231231->wornak23.kaisyo00\n" +
                "SIKKR1 = '34' and TDAKR1 >= 230101 and TDAKR1 <= 231231->worfun23.kaisyo00\n" +
                "SIKKR1 = '35' and TDAKR1 >= 230101 and TDAKR1 <= 231231->worich23.kaisyo00");
        tableNames.put("KANFIL20","SIKSS1 = '21' and UDASS1 >= 202201 and UDASS1 <= 202212->worcho22.kanfil20\n" +
                "SIKSS1 = '22' and UDASS1 >= 202201 and UDASS1 <= 202212->worsuz22.kanfil20\n" +
                "SIKSS1 = '23' and UDASS1 >= 202201 and UDASS1 <= 202212->wornak22.kanfil20\n" +
                "SIKSS1 = '34' and UDASS1 >= 202201 and UDASS1 <= 202212->worfun22.kanfil20\n" +
                "SIKSS1 = '35' and UDASS1 >= 202201 and UDASS1 <= 202212->worich22.kanfil20\n" +
                "SIKSS1 = '21' and UDASS1 >= 202301 and UDASS1 <= 202312->worcho23.kanfil20\n" +
                "SIKSS1 = '22' and UDASS1 >= 202301 and UDASS1 <= 202312->worsuz23.kanfil20\n" +
                "SIKSS1 = '23' and UDASS1 >= 202301 and UDASS1 <= 202312->wornak23.kanfil20\n" +
                "SIKSS1 = '34' and UDASS1 >= 202301 and UDASS1 <= 202312->worfun23.kanfil20\n" +
                "SIKSS1 = '35' and UDASS1 >= 202301 and UDASS1 <= 202312->worich23.kanfil20\n" +
                "UDASS1 >= 202401 ->RENDAYALL.sykurib0");
        tableNames.put("KANFILD0","SIKNS1");
        tableNames.put("SUMALL00","SIKSM1");
        tableNames.put("SUMKIC00","1");
        tableNames.put("SUMURC00","1");
        tableNames.put("SUMURI00","SIKUR1 = '21' and pdyur1 = '2022'->worcho22.sumuri00\n" +
                "SIKUR1 = '22' and pdyur1 = '2022'->worsuz22.sumuri00\n" +
                "SIKUR1 = '23' and pdyur1 = '2022'->wornak22.sumuri00\n" +
                "SIKUR1 = '34' and pdyur1 = '2022'->worfun22.sumuri00\n" +
                "SIKUR1 = '35' and pdyur1 = '2022'->worich22.sumuri00\n" +
                "SIKUR1 = '21' and pdyur1 = '2023'->worcho23.sumuri00\n" +
                "SIKUR1 = '22' and pdyur1 = '2023'->worsuz23.sumuri00\n" +
                "SIKUR1 = '23' and pdyur1 = '2023'->wornak23.sumuri00\n" +
                "SIKUR1 = '34' and pdyur1 = '2023'->worfun23.sumuri00\n" +
                "SIKUR1 = '35' and pdyur1 = '2023'->worich23.sumuri00");
        tableNames.put("SYKENKB0","SIKSR1");
        tableNames.put("SYKHANB0","SIKUR1");
        tableNames.put("SYKTKOB0","SIKTK1");
//        tableNames.put("SYKURIB0","SIKUB1");
        tableNames.put("SYKZAIB0","SIKTB1");
        tableNames.put("SYKZATB0","SIKTB1");
//        tableNames.put("TKIFILT0","SIKTO1");
            tableNames.put("TKIFILU0","SIKHA1 = '21' and tdaha1 >= 20220101 and tdaha1 <= 20221231->worcho22.tkifilu0\n" +
                "SIKHA1 = '22' and tdaha1 >= 20220101 and tdaha1 <= 20221231->worsuz22.tkifilu0\n" +
                "SIKHA1 = '23' and tdaha1 >= 20220101 and tdaha1 <= 20221231->wornak22.tkifilu0\n" +
                "SIKHA1 = '34' and tdaha1 >= 20220101 and tdaha1 <= 20221231->worfun22.tkifilu0\n" +
                "SIKHA1 = '35' and tdaha1 >= 20220101 and tdaha1 <= 20221231->worich22.tkifilu0\n" +
                "SIKHA1 = '21' and tdaha1 >= 20230101 and tdaha1 <= 20231231->worcho23.tkifilu0\n" +
                "SIKHA1 = '22' and tdaha1 >= 20230101 and tdaha1 <= 20231231->worsuz23.tkifilu0\n" +
                "SIKHA1 = '23' and tdaha1 >= 20230101 and tdaha1 <= 20231231->wornak23.tkifilu0\n" +
                "SIKHA1 = '34' and tdaha1 >= 20230101 and tdaha1 <= 20231231->worfun23.tkifilu0\n" +
                "SIKHA1 = '35' and tdaha1 >= 20230101 and tdaha1 <= 20231231->worich23.tkifilu0");
        tableNames.put("TKIHANM0","SIKUK1 = '21' and nenuk1 = '2022'->worcho22.tkihanm0\n" +
                "SIKUK1 = '22' and nenuk1 = '2022'->worsuz22.tkihanm0\n" +
                "SIKUK1 = '23' and nenuk1 = '2022'->wornak22.tkihanm0\n" +
                "SIKUK1 = '34' and nenuk1 = '2022'->worfun22.tkihanm0\n" +
                "SIKUK1 = '35' and nenuk1 = '2022'->worich22.tkihanm0\n" +
                "SIKUK1 = '21' and nenuk1 = '2023'->worcho23.tkihanm0\n" +
                "SIKUK1 = '22' and nenuk1 = '2023'->worsuz23.tkihanm0\n" +
                "SIKUK1 = '23' and nenuk1 = '2023'->wornak23.tkihanm0\n" +
                "SIKUK1 = '34' and nenuk1 = '2023'->worfun23.tkihanm0\n" +
                "SIKUK1 = '35' and nenuk1 = '2023'->worich23.tkihanm0\n" +
                "SIKUK1 = '21' and nenuk1 = '2024'->rendaycho.tkihanm0\n" +
                "SIKUK1 = '22' and nenuk1 = '2024'->rendaysuz.tkihanm0\n" +
                "SIKUK1 = '23' and nenuk1 = '2024'->rendaynak.tkihanm0\n" +
                "SIKUK1 = '34' and nenuk1 = '2024'->rendayfun.tkihanm0\n" +
                "SIKUK1 = '35' and nenuk1 = '2024'->rendayich.tkihanm0");
//        tableNames.put("TKITOKH0","SIKTH1");
        tableNames.put("TKITOKM0","SIKTK1 = '21' and nentk1 = '2022'->worcho22.tkitokm0\n" +
                "SIKTK1 = '22' and nentk1 = '2022'->worsuz22.tkitokm0\n" +
                "SIKTK1 = '23' and nentk1 = '2022'->wornak22.tkitokm0\n" +
                "SIKTK1 = '34' and nentk1 = '2022'->worfun22.tkitokm0\n" +
                "SIKTK1 = '35' and nentk1 = '2022'->worich22.tkitokm0\n" +
                "SIKTK1 = '21' and nentk1 = '2023'->worcho23.tkitokm0\n" +
                "SIKTK1 = '22' and nentk1 = '2023'->worsuz23.tkitokm0\n" +
                "SIKTK1 = '23' and nentk1 = '2023'->wornak23.tkitokm0\n" +
                "SIKTK1 = '34' and nentk1 = '2023'->worfun23.tkitokm0\n" +
                "SIKTK1 = '35' and nentk1 = '2023'->worich23.tkitokm0\n" +
                "SIKTK1 = '21' and nentk1 = '2024'->rendaycho.tkitokm0\n" +
                "SIKTK1 = '22' and nentk1 = '2024'->rendaysuz.tkitokm0\n" +
                "SIKTK1 = '23' and nentk1 = '2024'->rendaynak.tkitokm0\n" +
                "SIKTK1 = '34' and nentk1 = '2024'->rendayfun.tkitokm0\n" +
                "SIKTK1 = '35' and nentk1 = '2024'->rendayich.tkitokm0");

        return tableNames;
    }

    private static class PostgresTableSource implements SourceFunction<Tuple1<String>> {
        private volatile boolean isRunning = true;
        private final String tableName;
        private final String code;
        private final String sikmValue;
        private final String collStr;
        private final String whereStr;

        public PostgresTableSource(String tableName, String code, String sikmValue, String collStr,String whereStr) {
            this.tableName = tableName;
            this.code = code;
            this.sikmValue = sikmValue;
            this.collStr = collStr;
            this.whereStr = whereStr;
        }

        @Override
        public void run(SourceContext<Tuple1<String>> ctx) throws Exception {
            Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            Statement stmt = conn.createStatement();
            StringBuilder sbSql = new StringBuilder();
            sbSql.append("SELECT ").append(collStr).append(" FROM xuexiaodingtest.")
                    .append(tableName);
            if (null != sikmValue )
                sbSql.append(" where ").append(code).append(" = '").append(sikmValue).append("'");
            if (whereStr != null) {
                sbSql.append(" where ").append(whereStr);
            }
            String sql = sbSql.toString();
            System.out.println("执行的sql：" + sql);
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
                ctx.collect(new Tuple1<>(sb.toString()));
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