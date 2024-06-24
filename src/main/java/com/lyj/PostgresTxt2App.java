package com.lyj;

import com.lyj.util.ConfigLoader;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.lyj.PostgresTxt1App.extracted;

public class PostgresTxt2App {

    private static final Logger logger = LoggerFactory.getLogger(PostgresTxt2App.class);

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
        env.execute(PostgresTxt2App.class.getName() + System.currentTimeMillis());
    }

    private static Map<String, String> getAllTableNames() {
        Map<String, String> tableNames = new HashMap<>();
        tableNames.put("SYKENKB0", "SIKSR1");
        tableNames.put("SYKHANB0", "SIKUR1");
        tableNames.put("SYKTKOB0", "SIKTK1");
        tableNames.put("SYKZAIB0", "SIKTB1");
        tableNames.put("SYKZATB0", "SIKTB1");
        return tableNames;
    }
}
