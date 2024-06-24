package com.lyj;

import com.lyj.util.ConfigLoader;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.lyj.PostgresTxt1App.extracted;

public class PostgresTxt3App {

    private static final Logger logger = LoggerFactory.getLogger(PostgresTxt3App.class);

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            logger.error("args.length  < 3");
            return;
        }
        ConfigLoader.loadConfiguration(args[0]);

        String folderPath = args[1];

        String schema = args[2];

        // Set up the execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Set the parallelism to 1 to ensure all data goes to a single file
        env.setParallelism(1);

        // Get all table names
        Map<String, String> tableNames = getAllTableNames();

        extracted(tableNames, schema, env, folderPath);

        env.execute(PostgresTxt3App.class.getName() + System.currentTimeMillis());
    }

    private static Map<String, String> getAllTableNames() {
        Map<String, String> tableNames = new HashMap<>();
        tableNames.put("ENKFILE0", "SIKHN1");
        tableNames.put("KAIFIL00", "SIKKF1");
        tableNames.put("KANFILD0", "SIKNS1");
        tableNames.put("SUMALL00", "SIKSM1");
        tableNames.put("SUMKIC00", "1");
        tableNames.put("SUMURC00", "1");
        tableNames.put("SYKURIB0", "1");
        return tableNames;
    }
}
