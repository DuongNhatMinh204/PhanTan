package com.yrrhelp;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yrrhelp.proto.Operation;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DataStore {
    private final Map<String, String> store = new HashMap<>();
    private final List<Operation> operationLog = new ArrayList<>();
    private final String filePath;
    private final String logFilePath ;
    private final ObjectMapper mapper = new ObjectMapper();

    public DataStore(String filePath) {
        this.filePath = filePath;
        this.logFilePath = filePath.replace(".json", "_log.json");
        loadFromFile();
        loadOperationLog();
    }

    public synchronized void put(String key, String value) {
        store.put(key, value);
        operationLog.add(Operation.newBuilder()
                .setKey(key)
                .setValue(value)
                .setIsDelete(false)
                .setTimestamp(System.currentTimeMillis())
                .build());
        saveToFile();
        saveOperationLog();
    }

    public synchronized String get(String key) {
        return store.get(key);
    }

    public synchronized void delete(String key) {
        store.remove(key);
        operationLog.add(Operation.newBuilder()
                .setKey(key)
                .setValue("")
                .setIsDelete(true)
                .setTimestamp(System.currentTimeMillis())
                .build());
        saveToFile();
        saveOperationLog();
    }

    public synchronized Map<String, String> getAllData() {
        return new HashMap<>(store);
    }

    public synchronized List<Operation> getOperationLog(long fromTimestamp){
        return operationLog.stream()
                .filter(op -> op.getTimestamp() >= fromTimestamp)
                .collect(Collectors.toList());
    }

    private void loadFromFile() {
        try {
            File file = new File(filePath);
            if (file.exists()) {
                store.putAll(mapper.readValue(file, Map.class));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void saveToFile() {
        try {
            mapper.writeValue(new File(filePath), store);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private void loadOperationLog() {
        try {
            File file = new File(logFilePath);
            if (file.exists()) {
                List<Map<String, Object>> rawLog = mapper.readValue(file, new TypeReference<List<Map<String, Object>>>() {});
                for (Map<String, Object> entry : rawLog) {
                    operationLog.add(Operation.newBuilder()
                            .setKey((String) entry.get("key"))
                            .setValue((String) entry.get("value"))
                            .setIsDelete((Boolean) entry.get("isDelete"))
                            .setTimestamp(((Number) entry.get("timestamp")).longValue())
                            .build());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void saveOperationLog() {
        try {
            List<Map<String, Object>> rawLog = new ArrayList<>();
            for (Operation op : operationLog) {
                Map<String, Object> entry = new HashMap<>();
                entry.put("key", op.getKey());
                entry.put("value", op.getValue());
                entry.put("isDelete", op.getIsDelete());
                entry.put("timestamp", op.getTimestamp());
                rawLog.add(entry);
            }
            mapper.writeValue(new File(logFilePath), rawLog);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}