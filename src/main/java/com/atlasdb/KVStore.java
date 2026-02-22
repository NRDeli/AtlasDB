package com.atlasdb;

import java.util.concurrent.ConcurrentHashMap;

public class KVStore {

    private final ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();

    public void put(String key, String value) {
        if (key == null) throw new IllegalArgumentException("key cannot be null");
        store.put(key, value);
    }

    public String get(String key) {
        return key == null ? null : store.get(key);
    }

    public void delete(String key) {
        if (key != null) store.remove(key);
    }

    public int size() {
        return store.size();
    }

    public void clear() {
        store.clear();
    }
}
