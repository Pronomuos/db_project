package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.io.DatabaseInputStream;
import com.itmo.java.basics.logic.io.DatabaseOutputStream;

import java.io.*;
import java.nio.file.Path;
import java.util.Optional;

public class SegmentImpl implements Segment {

    private static final int maxSize = 100_000;

    private String segmentName;
    private SegmentIndex segmentIndex;
    private DatabaseInputStream inStream;
    private DatabaseOutputStream outStream;
    private boolean isReadOnly = false;
    private long curOffset = 0;

    static Segment create(String segmentName, Path tableRootPath) throws DatabaseException, IOException {
        File segmentFile = new File (tableRootPath.toString() + segmentName);
        if (!segmentFile.exists())
            throw new DatabaseException("Segment " + segmentName + " already exists!");
        if (!segmentFile.createNewFile())
            throw new IOException("Impossible to create " + segmentName + " table.");

        SegmentImpl segment = new SegmentImpl();
        segment.setSegmentName(segmentName);
        segment.setSegmentIndex(new SegmentIndex());

        segment.setInStream(new DatabaseInputStream(new FileInputStream(segmentFile)));
        segment.setOutStream(new DatabaseOutputStream(new FileOutputStream(segmentFile, true)));


        return segment;
    }

    static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    public void setSegmentName(String segmentName) {
        this.segmentName = segmentName;
    }

    public void setSegmentIndex(SegmentIndex segmentIndex) {
        this.segmentIndex = segmentIndex;
    }

    public void setInStream(DatabaseInputStream inStream) { this.inStream = inStream; }

    public void setOutStream(DatabaseOutputStream outStream) { this.outStream = outStream; }

    @Override
    public String getName() {
        return segmentName;
    }

    @Override
    public boolean write(String objectKey, byte[] objectValue) throws IOException {
        SetDatabaseRecord record;
        try {
            record = SetDatabaseRecord.builder().
                    keySize(objectKey.length()).
                    key(objectKey.getBytes()).
                    valSize(objectValue.length).
                    value(objectValue).
                    build();
        } catch (DatabaseException ex) {
            throw new IOException("Error while converting data into record.", ex);
        }

        curOffset += outStream.write(record);
        segmentIndex.onIndexedEntityUpdated(objectKey, new SegmentOffsetInfoImpl(curOffset));
        if (curOffset >= maxSize) {
            isReadOnly = true;
            return false;
        }

        return true;
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {
        var offset = segmentIndex.searchForKey(objectKey);
        if (offset.isEmpty())
            throw new IOException("Segment - " + segmentName + ". No such a key " + objectKey + ".");

        var skip = inStream.skip(offset.get().getOffset());
        if (skip < offset.get().getOffset())
            throw new IOException("Could not get to the position in the file.");
        var record = inStream.readDbUnit();
        if (record.isEmpty() || record.get().getValue() == null)
            return Optional.empty();

        return Optional.of(record.get().getValue());
    }

    @Override
    public boolean isReadOnly() {
        return isReadOnly;
    }

    @Override
    public boolean delete(String objectKey) throws IOException {
        RemoveDatabaseRecord record;
        try {
            record =  new RemoveDatabaseRecord(objectKey.length(), objectKey.getBytes());
        } catch (DatabaseException ex) {
            throw new IOException("Error while converting data into record.", ex);
        }

        curOffset += outStream.write(record);
        segmentIndex.onIndexedEntityUpdated(objectKey, new SegmentOffsetInfoImpl(curOffset));
        if (curOffset >= maxSize) {
            isReadOnly = true;
            return false;
        }

        return true;
    }
}
