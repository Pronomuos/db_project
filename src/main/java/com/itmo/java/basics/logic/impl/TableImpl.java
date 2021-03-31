package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Table;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class TableImpl implements Table {

    private String tableName;
    private Path pathToDatabaseRoot;
    private TableIndex tableIndex;
    private List<Segment> segments;

    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        File tableDir = new File (pathToDatabaseRoot.toString(), tableName);
        if (tableDir.exists())
            throw new DatabaseException("Table " + tableName + " already exists!");
        if (!tableDir.mkdir())
            throw new DatabaseException("Impossible to create " + tableName + " table.");

        TableImpl table = new TableImpl();
        table.setTableName(tableName);
        table.setPathToDatabaseRoot(pathToDatabaseRoot);
        table.setTableIndex(tableIndex);
        table.setSegments(new ArrayList<>());


        return table;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setPathToDatabaseRoot(Path pathToDatabaseRoot) {
        this.pathToDatabaseRoot = pathToDatabaseRoot;
    }

    public void setTableIndex(TableIndex tableIndex) {
        this.tableIndex = tableIndex;
    }

    public void setSegments(List<Segment> segments) {
        this.segments = segments;
    }

    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        if (segments.isEmpty())
            segments.add(SegmentImpl.create(SegmentImpl.createSegmentName(tableName),
                    Path.of(pathToDatabaseRoot.toString(), tableName)));

        try {
            if (segments.get(segments.size() - 1).isReadOnly())
                segments.add(SegmentImpl.create(SegmentImpl.createSegmentName(tableName),
                        Path.of(pathToDatabaseRoot.toString(), tableName)));
            segments.get(segments.size() - 1).write(objectKey, objectValue);
            tableIndex.onIndexedEntityUpdated(objectKey, segments.get(segments.size() - 1));
        } catch (IOException ex) {
            throw new DatabaseException("Error while writing data into segment.", ex);
        }
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        var segment = tableIndex.searchForKey(objectKey);
        if (segment.isEmpty())
            return Optional.empty();

        try {
            return segment.get().read(objectKey);
        } catch (IOException ex) {
            throw new DatabaseException("Error while reading a segment.", ex);
        }
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        if(segments.isEmpty())
            throw new DatabaseException("There was no keys created in the table - " + tableName + ".");

        var segment = tableIndex.searchForKey(objectKey);
        if (segment.isEmpty())
            throw new DatabaseException("Table - " + tableName + ". No such a key " + objectKey + ".");

        try {
            if (segments.get(segments.size() - 1).isReadOnly())
                segments.add(SegmentImpl.create(SegmentImpl.createSegmentName(tableName),
                        Path.of(pathToDatabaseRoot.toString(), tableName)));
            segments.get(segments.size() - 1).delete(objectKey);
            tableIndex.onIndexedEntityUpdated(objectKey, segments.get(segments.size() - 1));
        } catch (IOException ex) {
            throw new DatabaseException("Error while writing data into segment.", ex);
        }
    }
}

