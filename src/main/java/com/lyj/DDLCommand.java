package com.lyj;

import com.lyj.util.ConfigLoader;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static com.lyj.util.ConfigLoader.DB_PROFILE;
import static com.lyj.util.TableUtil.CHARSET_NAME_31J;
import static com.lyj.util.TableUtil.executeSql;

/**
 * 执行DDL 语句在指定的数据库中
 *
 * java -cp flink-postgres-csv-1.0-SNAPSHOT.jar com.lyj.DDLCommand --db_profile test02 --ddl_path D:\flink\suz_nin_code_repalce\output_exec --charset_name UTF-8
 */
public class DDLCommand {

    public static void main(String[] args) throws Exception {


        final ParameterTool params = ParameterTool.fromArgs(args);
        // 通过命令行参来选择配置文件

        String activeProfile = params.get(DB_PROFILE,"dev43_rc202511226");

        String exeFolderPath = params.get("ddl_path","D:\\renmasall\\20251126");

        String charsetName = params.get("charset_name", CHARSET_NAME_31J);

        ConfigLoader.loadConfiguration(activeProfile);

        String folderPath = exeFolderPath;
        File folder = new File(folderPath);
        if (folder.exists()) {
            File[] files = folder.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (!file.isDirectory()) {
                        String sqlFilePath = folderPath + "\\" + file.getName();
                        System.out.println("执行的文件名：" + file.getName());
                        List<String> sqlLines = Files.readAllLines(Paths.get(sqlFilePath), Charset.forName(charsetName));
//                        List<String> sqlLines = Files.readAllLines(Paths.get(sqlFilePath), Charset.forName("UTF-8"));
                        // 拼接 SQL 文件中的所有语句
                        StringBuilder sqlBuilder = new StringBuilder();
                        for (String line : sqlLines) {
                            sqlBuilder.append(line).append("\n");
                        }
                        String sql = sqlBuilder.toString();

                        executeSql(sql);

                    }
                }
            }
        }
    }
}
