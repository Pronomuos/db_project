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

    static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        File segmentFile = new File (tableRootPath.toString(), segmentName);

        try {
            if (!segmentFile.createNewFile())
                throw new DatabaseException("Segment " + segmentName + " already exists!");
        } catch (IOException ex) {
            throw new DatabaseException("Impossible to create " + segmentName + " table.");
        }


        SegmentImpl segment = new SegmentImpl();
        segment.setSegmentName(segmentName);
        segment.setSegmentIndex(new SegmentIndex());

        try {
            segment.setInStream(new DatabaseInputStream(new FileInputStream(segmentFile)));
            segment.setOutStream(new DatabaseOutputStream(new FileOutputStream(segmentFile, true)));
        } catch (FileNotFoundException ex) {
            throw new DatabaseException(segmentName + " is not found.", ex);
        }


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
                    valSize(Optional.ofNullable(objectValue).isEmpty() ||
                            objectValue.length == 0 ? -1 : objectValue.length).
                    value(objectValue).
                    build();
        } catch (DatabaseException ex) {
            throw new IOException("Error while converting data into record.", ex);
        }

        segmentIndex.onIndexedEntityUpdated(objectKey, new SegmentOffsetInfoImpl(curOffset));
        curOffset += outStream.write(record);
        if (curOffset >= maxSize) {
            outStream.close();
            isReadOnly = true;
            return false;
        }

        return true;
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {
        var offset = segmentIndex.searchForKey(objectKey);
        if (offset.isEmpty())
            return Optional.empty();

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
