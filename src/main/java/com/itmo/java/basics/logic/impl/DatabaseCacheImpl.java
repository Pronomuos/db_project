package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.DatabaseCache;

import java.util.*;

public class DatabaseCacheImpl implements DatabaseCache {
    private static final int CAPACITY = 5_000;
    Map<String, byte[]> cache;


    public DatabaseCacheImpl() {
        this.cache = new LinkedHashMap<>(CAPACITY);
    }

    @Override
    public byte[] get(String key) {
        byte[] value =  cache.get(key);
        if (value != null) {
            cache.remove(key);
            cache.put(key, value);
        }
        return value;
    }

    @Override
    public void set(String key, byte[] value) {
        if (cache.containsKey(key)) {
            cache.remove(key);
        } else if (cache.size() == CAPACITY) {
            Iterator<String> it = cache.keySet().iterator();
            it.next();
            it.remove();
        }
        cache.put(key, value);
    }

    @Override
    public void delete(String key) {
        cache.remove(key);
    }
}
