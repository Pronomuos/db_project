package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;

import java.io.File;
import java.nio.file.Path;
import java.util.*;

public class DatabaseImpl implements Database {

    private final String dbName;
    private final Path databaseRoot;
    private final Map<String, Table> tables = new HashMap<String, Table>();

    private DatabaseImpl (String dbName, Path databaseRoot) {
        this.dbName = dbName;
        this.databaseRoot = databaseRoot;
    }

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        File dbDir = new File (databaseRoot.toString(), dbName);
        if (dbDir.exists()) {
            throw new DatabaseException("Database " + dbName + " already exists!");
        }
        if (!dbDir.mkdir()) {
            throw new DatabaseException("Impossible to create " + dbName + " database.");
        }
        return new DatabaseImpl(dbName, databaseRoot);
    }

    @Override
    public String getName() {
        return dbName;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        if (tables.entrySet().stream().anyMatch(val -> val.getKey().equals(tableName))) {
            throw new DatabaseException("Table " + tableName + " already exists!");
        }
        TableIndex index = new TableIndex();
        tables.put(tableName, TableImpl.create(tableName, Path.of(databaseRoot.toString(), dbName), index));
    }

    @Override
    public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException {
        var table = tables.entrySet().stream().filter(x -> x.getKey().equals(tableName)).findAny().
                orElseThrow(() -> new DatabaseException("Cannot find the table, called - " + tableName + "."));
        table.getValue().write(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException {
        var table = tables.entrySet().stream().filter(x -> x.getKey().equals(tableName)).findAny().
                orElseThrow(() -> new DatabaseException("Cannot find the table, called - " + tableName + "."));
        return table.getValue().read(objectKey);
    }

    @Override
    public void delete(String tableName, String objectKey) throws DatabaseException {
        var table = tables.entrySet().stream().filter(x -> x.getKey().equals(tableName)).findAny().
                orElseThrow(() -> new DatabaseException("Cannot find the table, called - " + tableName + "."));
        table.getValue().delete(objectKey);
    }
}
