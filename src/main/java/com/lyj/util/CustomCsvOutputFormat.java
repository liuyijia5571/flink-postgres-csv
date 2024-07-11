package com.lyj.util;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.FileSystem.WriteMode;

import java.io.BufferedWriter;
import java.io.File;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static com.lyj.util.TableUtil.CHARSET_NAME_31J;


public class CustomCsvOutputFormat implements OutputFormat<String> {

    private final Path outputPath;
    private final WriteMode writeMode;
    private transient BufferedWriter writer;

    public CustomCsvOutputFormat(String outputPath, WriteMode writeMode) {
        this.outputPath = new Path(outputPath);
        this.writeMode = writeMode;
    }

    @Override
    public void configure(Configuration parameters) {
        // No configuration needed
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        writer = new BufferedWriter(new OutputStreamWriter(
                outputPath.getFileSystem().create(outputPath, writeMode), CHARSET_NAME_31J));
    }

    @Override
    public void writeRecord(String record) throws IOException {
        writer.write(record);
        writer.newLine();
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }

    public static OutputFormat getOutputFormat(String inputPath, String formattedDate) {
        String outputPath = inputPath + File.separator + "file_record_" + formattedDate + ".csv";
        OutputFormat customCsvOutputFormat = new CustomCsvOutputFormat(outputPath, FileSystem.WriteMode.OVERWRITE);
        return customCsvOutputFormat;
    }

    public static String getFormattedDate() {
        // 获取当前时间的时间戳
        long currentTimeMillis = System.currentTimeMillis();

        // 创建日期对象
        Date date = new Date(currentTimeMillis);

        // 定义日期格式
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

        // 将日期对象格式化为字符串
        String formattedDate = dateFormat.format(date);
        return formattedDate;
    }
}
