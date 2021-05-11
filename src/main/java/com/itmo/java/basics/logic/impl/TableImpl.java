package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Table;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

public class TableImpl implements Table {
    private final String tableName;
    private final Path pathToDatabaseRoot;
    private final TableIndex tableIndex;
    private Segment currentSegment = null;

    private TableImpl(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) {
        this.tableName = tableName;
        this.pathToDatabaseRoot = pathToDatabaseRoot;
        this.tableIndex = tableIndex;
    }

    private TableImpl(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex, Segment currentSegment) {
        this.tableName = tableName;
        this.pathToDatabaseRoot = pathToDatabaseRoot;
        this.tableIndex = tableIndex;
        this.currentSegment = currentSegment;
    }

    public static Table initializeFromContext(TableInitializationContext context) {
        return new CachingTable(new TableImpl(context.getTableName(), context.getTablePath(), context.getTableIndex(), context.getCurrentSegment()));

    }


    public static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        File tableDir = new File (pathToDatabaseRoot.toString(), tableName);
        if (tableDir.exists()) {
            throw new DatabaseException("Table " + tableName + " already exists!");
        }
        if (!tableDir.mkdir()) {
            throw new DatabaseException("Impossible to create " + tableName + " table.");
        }
        return new CachingTable(new TableImpl(tableName, pathToDatabaseRoot, tableIndex));
    }

    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        if (currentSegment == null) {
            currentSegment =  SegmentImpl.create(SegmentImpl.createSegmentName(tableName),
                    Path.of(pathToDatabaseRoot.toString(), tableName));
        }
        try {
            if (currentSegment.isReadOnly()) {
                currentSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName),
                        Path.of(pathToDatabaseRoot.toString(), tableName));
            }
            currentSegment.write(objectKey, objectValue);
            tableIndex.onIndexedEntityUpdated(objectKey, currentSegment);
        } catch (IOException ex) {
            throw new DatabaseException("Error while writing data into segment.", ex);
        }
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        var segment = tableIndex.searchForKey(objectKey);
        if (segment.isEmpty()) {
            return Optional.empty();
        }
        try {
            return segment.get().read(objectKey);
        } catch (IOException ex) {
            throw new DatabaseException("Error while reading a segment.", ex);
        }
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        if(currentSegment == null) {
            throw new DatabaseException("There was no keys created in the table - " + tableName + ".");
        }
        var segment = tableIndex.searchForKey(objectKey);
        if (segment.isEmpty()) {
            throw new DatabaseException("Table - " + tableName + ". No such a key " + objectKey + ".");
        }
        try {
            if (currentSegment.isReadOnly()) {
                currentSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName),
                        Path.of(pathToDatabaseRoot.toString(), tableName));
            }
            currentSegment.delete(objectKey);
            tableIndex.onIndexedEntityUpdated(objectKey, currentSegment);
        } catch (IOException ex) {
            throw new DatabaseException("Error while writing data into segment.", ex);
        }
    }
}