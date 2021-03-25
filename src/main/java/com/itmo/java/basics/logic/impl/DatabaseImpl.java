package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DatabaseImpl implements Database {


    private String dbName;
    private Path databaseRoot;
    private List<Table> tables;

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        File dbDir = new File (databaseRoot.toString(), dbName);
        if (dbDir.exists())
            throw new DatabaseException("Database " + dbName + " already exists!");
        if (!dbDir.mkdir())
            throw new DatabaseException("Impossible to create " + dbName + " database.");

        DatabaseImpl db = new DatabaseImpl();
        db.setDbName(dbName);
        db.setDatabaseRoot(databaseRoot);
        db.setTables(new ArrayList<>());

        return db;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public void setDatabaseRoot(Path databaseRoot) {
        this.databaseRoot = databaseRoot;
    }

    public void setTables(List<Table> tables) { this.tables = tables; }

    @Override
    public String getName() {
        return dbName;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        if (tables.stream().anyMatch(val -> val.getName().equals(tableName)))
            throw new DatabaseException("Table " + tableName + " already exists!");

        TableIndex index = new TableIndex();
        tables.add(TableImpl.create(tableName, Path.of(databaseRoot.toString(), dbName), index));
    }

    @Override
    public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException {
        var table = tables.stream().filter(x -> x.getName().equals(tableName)).findAny().
                orElseThrow(() -> new DatabaseException("Cannot find the table, called - " + tableName + "."));

        table.write(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException {
        var table = tables.stream().filter(x -> x.getName().equals(tableName)).findAny().
                orElseThrow(() -> new DatabaseException("Cannot find the table, called - " + tableName + "."));

        return table.read(objectKey);
    }

    @Override
    public void delete(String tableName, String objectKey) throws DatabaseException {
        var table = tables.stream().filter(x -> x.getName().equals(tableName)).findAny().
                orElseThrow(() -> new DatabaseException("Cannot find the table, called - " + tableName + "."));

        table.delete(objectKey);
    }
}
