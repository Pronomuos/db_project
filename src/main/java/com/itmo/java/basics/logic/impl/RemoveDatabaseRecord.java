package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.WritableDatabaseRecord;

public class RemoveDatabaseRecord implements WritableDatabaseRecord {

    private int keySize;
    private byte[] key;

    public RemoveDatabaseRecord(int keySize, byte[] key) throws DatabaseException {
        this.keySize = keySize;
        this.key = key;
        if (keySize < 0)
            throw new DatabaseException("Invalid key size.");
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public byte[] getValue() {
        return null;
    }

    @Override
    public long size() {
        return 4 * 2 + keySize;
    }

    @Override
    public boolean isValuePresented() {
        return false;
    }

    @Override
    public int getKeySize() {
        return keySize;
    }

    @Override
    public int getValueSize() {
        return -1;
    }
}
