package com.example;


import com.example.util.ConfigLoader;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static com.example.util.TableUtil.executeSql;


public class DDLCommand {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("args.length  < 2");
            return;
        }

        ConfigLoader.loadConfiguration(args[1]);

        String folderPath = args[0];
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
                        executeSql( sql);

                    }
                }
            }
        }
    }
}
