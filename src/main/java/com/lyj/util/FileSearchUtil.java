package com.lyj.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class FileSearchUtil {

    private static final Logger logger = LoggerFactory.getLogger(FileSearchUtil.class);

    public static List<String> findDirectoriesContainingFile(String directoryPath, String fileName) {
        List<String> result = new ArrayList<>();
        File directory = new File(directoryPath);

        if (!directory.exists() || !directory.isDirectory()) {
            logger.error("{} is not directory", directoryPath);
            return result;
        }

        searchDirectories(directory, fileName, result, directoryPath);
        return result;
    }

    private static void searchDirectories(File directory, String fileName, List<String> result, String directoryPath) {
        // 获取目录下的所有文件和子目录
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    // 如果是目录，则递归搜索
                    searchDirectories(file, fileName, result, directoryPath);
                } else if (file.getName().equalsIgnoreCase(fileName)) {
                    // 如果是同名文件，则将其所在的目录路径添加到结果列表中
                    String filePath = file.getParent().replace(directoryPath, "");
                    result.add(filePath);
                }
            }
        }
    }

    public static void main(String[] args) {
        System.out.println("㈱トーホーせんどば　　　　　".replace("　", "") + 11);
        System.out.println("㈱山十青果　　　　　　");
    }
}
