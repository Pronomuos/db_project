package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.WritableDatabaseRecord;

import java.util.Optional;


public class SetDatabaseRecord implements WritableDatabaseRecord {

    private final byte[] key;
    private final byte[] value;

    public SetDatabaseRecord (byte[] key, byte[] value) throws DatabaseException {
        if (key == null)
            throw new DatabaseException("Cannot create database record without key.");

        if (value != null)
            this.value = value.length == 0 ? null : value;
        else
            this.value = null;

        this.key = key;

    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public byte[] getValue() {
        return value;
    }

    @Override
    public long size() {
        return  4 * 2 + getKeySize() + (getValueSize() == -1 ? 0 : getValueSize());
    }

    @Override
    public boolean isValuePresented() {
        return value != null;
    }

    @Override
    public int getKeySize() {
        return key.length;
    }

    @Override
    public int getValueSize() {
        return isValuePresented() ? value.length : -1;
    }

}

