package com.lyj.util;


import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.core.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

public class CustomTextOutputFormat<T> extends FileOutputFormat<T> {
    private static final long serialVersionUID = 1L;
    private final String encoding;
    private final String lineDelimiter;
    private transient PrintWriter writer;

    public CustomTextOutputFormat(Path filePath, String encoding, String lineDelimiter) {
        super.setOutputFilePath(filePath);
        this.encoding = encoding;
        this.lineDelimiter = lineDelimiter;
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        super.open(taskNumber, numTasks);
        writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(this.stream, encoding)));
    }

    @Override
    public void writeRecord(T record) throws IOException {
        writer.print(record.toString());
        writer.print(lineDelimiter); // 使用自定义换行符
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            writer.close();
        }
        super.close();
    }
}