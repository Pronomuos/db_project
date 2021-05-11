package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.DatabaseCache;
import com.itmo.java.basics.logic.Table;

import java.util.Optional;

public class CachingTable implements Table {
    Table table;
    DatabaseCache cache;

    public CachingTable(Table table) {
        this.table = table;
        this.cache = new DatabaseCacheImpl();
    }

    @Override
    public String getName() {
        return table.getName();
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        cache.set(objectKey, objectValue);
        table.write(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        Optional<byte[]> value = Optional.ofNullable(cache.get(objectKey));
        if (value.isEmpty()) {
            value = table.read(objectKey);
            value.ifPresent(bytes -> cache.set(objectKey, bytes));
        }
        return value;
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        table.delete(objectKey);
        cache.delete(objectKey);
    }
}

