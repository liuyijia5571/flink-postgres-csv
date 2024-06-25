package com.lyj;

import com.lyj.util.ConfigLoader;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.lyj.PostgresTxt1App.checkParams;
import static com.lyj.PostgresTxt1App.extracted;
import static com.lyj.util.ConfigLoader.DB_PROFILE;

public class PostgresTxt2App {

    private static final Logger logger = LoggerFactory.getLogger(PostgresTxt2App.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // 通过命令行参来选择配置文件
        String activeProfile = params.get(DB_PROFILE);

        // CSV 文件路径
        String folderPath = params.get("output_path");

        String schema = params.get("schema");

        boolean checkParamsResult = checkParams(activeProfile,schema, folderPath);
        if (!checkParamsResult) {
            logger.error("params demo : " +
                    "--db_profile dev43 \n" +
                    "--output_path C:\\flink\\job\\output \n" +
                    "--schema xuexiaodingtest ");
            return;
        }

        ConfigLoader.loadConfiguration(activeProfile);

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
