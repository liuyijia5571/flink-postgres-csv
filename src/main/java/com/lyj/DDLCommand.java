package com.lyj;

import com.lyj.util.ConfigLoader;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static com.lyj.util.ConfigLoader.DB_PROFILE;
import static com.lyj.util.TableUtil.executeSql;

public class DDLCommand {

    public static void main(String[] args) throws Exception {


        final ParameterTool params = ParameterTool.fromArgs(args);
        // 通过命令行参来选择配置文件

        String activeProfile = params.get(DB_PROFILE,"data_prod");

        String exeFolderPath = params.get("DDL_PATH","C:\\青果\\黄信中要的数据\\data_0612\\DDL");

        ConfigLoader.loadConfiguration(activeProfile);

        String folderPath = exeFolderPath;
        File folder = new File(folderPath);
        if (folder.exists()) {
            File[] files = folder.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (!file.isDirectory()) {
                        String sqlFilePath = folderPath + "\\" + file.getName();
                        List<String> sqlLines = Files.readAllLines(Paths.get(sqlFilePath));

                        // 拼接 SQL 文件中的所有语句
                        StringBuilder sqlBuilder = new StringBuilder();
                        for (String line : sqlLines) {
                            sqlBuilder.append(line).append("\n");
                        }
                        String sql = sqlBuilder.toString();
                        System.out.println("执行的文件名：" + file.getName());
                        executeSql(sql);

                    }
                }
            }
        }
    }
}
