package com.yrrhelp;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DataStore {
    private final Map<String, String> store = new HashMap<>();
    private final String filePath;
    private final ObjectMapper mapper = new ObjectMapper();

    public DataStore(String filePath) {
        this.filePath = filePath;
        loadFromFile();
    }

    public synchronized void put(String key, String value) {
        store.put(key, value);
        saveToFile();
    }

    public synchronized String get(String key) {
        return store.get(key);
    }

    public synchronized void delete(String key) {
        store.remove(key);
        saveToFile();
    }

    public synchronized Map<String, String> getAllData() {
        return new HashMap<>(store);
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