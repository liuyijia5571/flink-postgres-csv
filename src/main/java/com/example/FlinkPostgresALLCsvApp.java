package com.example;


import com.example.util.ConfigLoader;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.example.util.ConfigLoader.TEST05;
import static com.example.util.ConfigLoader.getDatabasePassword;
import static com.example.util.ConfigLoader.getDatabaseUrl;
import static com.example.util.ConfigLoader.getDatabaseUsername;
import static com.example.util.TableUtil.*;

public class FlinkPostgresALLCsvApp {

    private static boolean executeFlag = false;

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Set the parallelism to 1 to ensure all data goes to a single file
        env.setParallelism(1);

        ConfigLoader.loadConfiguration(TEST05);

        String folderPath = "output/all";

        File folder = new File(folderPath);
        if (folder.exists()) {
            deleteFolder(folder);
            System.out.println("Folder deleted successfully.");
        } else {
            System.out.println("Folder does not exist.");
        }

        // Get all table names
        List<String> allTableNames = getAllTableNames();

        // Process each table
        for (String tableName : allTableNames) {
            String[] splitTable = tableName.split("\\.");
            String schema = splitTable[0];
            String tableNameStr = splitTable[1];
            Map<String, List<String>> columns = getColumns(schema.toLowerCase(), tableNameStr.toLowerCase());
            List<String> colNames = columns.get("COL_NAMES");
            String colStr = colNames.stream().reduce((s1, s2) -> s1 + "," + s2).orElse(null);

            if (null != colStr) {
                if (!executeFlag) {
                    executeFlag = true;
                }
                // Create a custom source function to read data from each table
                SourceFunction<Tuple1<String>> sourceFunction = new PostgresTableSource(tableName, null, null, colStr, null);

                // Add the source function to the execution environment
                DataStream<Tuple1<String>> dataStream = env.addSource(sourceFunction);

                dataStream.writeAsCsv(folderPath + "/" + schema + "_" + tableNameStr + ".txt");
            }

        }

        // Execute the Flink job
        if (executeFlag) env.execute("Flink PostgreSQL to CSV Job");
    }

    private static class PostgresTableSource implements SourceFunction<Tuple1<String>> {
        private volatile boolean isRunning = true;
        private final String tableName;
        private final String code;
        private final String sikmValue;
        private final String collStr;
        private final String whereStr;

        public PostgresTableSource(String tableName, String code, String sikmValue, String collStr, String whereStr) {
            this.tableName = tableName;
            this.code = code;
            this.sikmValue = sikmValue;
            this.collStr = collStr;
            this.whereStr = whereStr;
        }

        @Override
        public void run(SourceContext<Tuple1<String>> ctx) throws Exception {
            Connection conn = DriverManager.getConnection(getDatabaseUrl(), getDatabaseUsername(), getDatabasePassword());
            Statement stmt = conn.createStatement();
            StringBuilder sbSql = new StringBuilder();
            sbSql.append("SELECT ").append(collStr).append(" FROM ").append(tableName);
            if (null != sikmValue) sbSql.append(" where ").append(code).append(" = '").append(sikmValue).append("'");
            if (null != whereStr) {
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
                        sb.append("|");
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

    public static List<String> getAllTableNames() throws Exception {
        List<String> list = new ArrayList<>();
        list.add("RENDAYALL.SUMKIC00");
        list.add("RENDAYALL.SUMURC00");
        list.add("RENDAYALL.SYKURIB0");
        return list;
    }
}