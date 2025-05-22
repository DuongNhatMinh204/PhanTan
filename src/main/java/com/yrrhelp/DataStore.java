package com.yrrhelp;

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
    private final ObjectMapper mapper = new ObjectMapper();

    public DataStore(String filePath) {
        this.filePath = filePath;
        loadFromFile();
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
}