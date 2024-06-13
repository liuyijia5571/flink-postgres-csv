package com.example.util;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class MultiFileSourceFunction implements SourceFunction<Tuple3<String, String, String>>, ListCheckpointed<Long> {

    private final List<String> filePaths;
    private volatile boolean isRunning = true;
    private long offset;

    public MultiFileSourceFunction(List<String> filePaths) {
        this.filePaths = filePaths;
    }

    @Override
    public void run(SourceFunction.SourceContext<Tuple3<String, String, String>> ctx) {
        // 恢复时从上次的 offset 开始读取
        for (String filePath : filePaths) {
            int lastIndexOf = filePath.lastIndexOf(File.separator);
            String flieName = filePath.substring(lastIndexOf);
            String[] tableNameArr = flieName.split("_");
            String schemaName = tableNameArr[0].toLowerCase();
            String tableName = tableNameArr[1].split("\\.")[0].toLowerCase();
            try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
                reader.skip(offset);
                String line;
                while (isRunning && (line = reader.readLine()) != null) {
                    synchronized (ctx.getCheckpointLock()) {
                        Tuple3<String, String, String> tuple3 = new Tuple3<>();
                        tuple3.f0 = schemaName;
                        tuple3.f1 = tableName;
                        tuple3.f2 = line;
                        ctx.collect(tuple3);
                        offset += line.getBytes().length; // 更新 offset
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public List<Long> snapshotState(long checkpointId, long timestamp) {
        // 返回当前 offset
        return Collections.singletonList(offset);
    }

    @Override
    public void restoreState(List<Long> state) {
        // 恢复 offset
        if (state.isEmpty()) {
            offset = 0L;
        } else {
            offset = state.get(0);
        }
    }
}
