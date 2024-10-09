package com.lyj.master;

import com.lyj.util.CustomTextOutputFormat;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.File;
import java.util.Arrays;

import static com.lyj.util.ExcelUtil.columnToIndex;
import static com.lyj.util.TableUtil.CHARSET_NAME_31J;

public class M28FJion {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        String inputFilePath = params.get("input_file_path", "C:\\flink\\m28");

        String newInputFilePath = params.get("result_file_path", "C:\\flink\\m28new");


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        File folder = new File(inputFilePath);
        if (folder.exists()) {
            File[] files = folder.listFiles();
            if (files != null) {
                for (int i = 0; i < files.length; i++) {
                    String fileName = files[i].getName();
                    DataSource<String> stringDataSource = env.readTextFile(inputFilePath + File.separator + fileName, CHARSET_NAME_31J);
                    MapOperator<String, Object> result = stringDataSource.map(u -> {
                        String[] split = u.split(",", -1);
                        int agIndex = columnToIndex("AG");
                        int ahIndex = columnToIndex("AH");
                        if (split.length >= ahIndex) {
                            String ahStr = split[ahIndex];
                            String agStr = split[agIndex];
                            split[ahIndex] = insertAfterFirstHiragana(ahStr,agStr);
                            split[agIndex] = "";
                            return Arrays.stream(split).reduce((a, b) -> a + "," + b).get();
                        }
                        return u;
                    });
                    CustomTextOutputFormat textOutputR052z = new CustomTextOutputFormat(new Path(newInputFilePath + File.separator + fileName), CHARSET_NAME_31J, "\r\n");
                    textOutputR052z.setWriteMode(FileSystem.WriteMode.OVERWRITE);
                    result.output(textOutputR052z).setParallelism(1);
                }
            }
            env.execute(M28FJion.class.getName());
        }


    }

    // 方法：判断字符是否为平假名、片假名（全角或半角）或字母
    public static boolean isHiraganaKatakanaOrLetter(char c) {
        return (c >= '\u3040' && c <= '\u309F')  // 平假名范围
                || (c >= '\u30A0' && c <= '\u30FF')  // 全角片假名范围
                || (c >= '\uFF65' && c <= '\uFF9F')  // 半角片假名范围
                || (c >= 'A' && c <= 'Z')            // 大写字母范围
                || (c >= 'a' && c <= 'z')
                ||c == '(';           // 小写字母范围
    }


    // 找到第一个平假名后插入字符串并替换等量空格
    public static String insertAfterFirstHiragana(String input, String toInsert) {
        int firstHiraganaIndex = -1;

        // 遍历字符串寻找第一个平假名
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (isHiraganaKatakanaOrLetter(c)) {
                firstHiraganaIndex = i;
                break;
            }
        }

        // 如果找到平假名
        if (firstHiraganaIndex != -1) {
            // 从平假名后开始寻找空格的位置
            int spaceStartIndex = firstHiraganaIndex + 1;
            int spaceEndIndex = spaceStartIndex;

            // 找到连续空格的范围
            while (spaceEndIndex < input.length() && input.charAt(spaceEndIndex) == ' ') {
                spaceEndIndex++;
            }

            // 构建新的字符串
            StringBuilder result = new StringBuilder();
            result.append(input.substring(0, spaceStartIndex));  // 插入平假名前的部分
            result.append(toInsert);                            // 插入新字符串
            result.append(input.substring(spaceEndIndex));       // 插入平假名后的非空格部分

            return result.toString();
        }

        // 如果没有找到平假名，返回原字符串
        return input;
    }
}
