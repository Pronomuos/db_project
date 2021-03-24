package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.WritableDatabaseRecord;

import java.util.Optional;


public class SetDatabaseRecord implements WritableDatabaseRecord {

    private int keySize;
    private byte[] key;
    private int valSize;
    private byte[] value;

    public void setKeySize(int keySize) {
        this.keySize = keySize;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public void setValSize(int valSize) {
        this.valSize = valSize;
    }

    public void setValue(byte[] value) {
        this.value = value;
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
        return  4 * 2 + keySize + (valSize == -1 ? 0 : valSize);
    }

    @Override
    public boolean isValuePresented() {
        return valSize != -1;
    }

    @Override
    public int getKeySize() {
        return keySize;
    }

    @Override
    public int getValueSize() {
        return valSize;
    }

    public static SetDatabaseRecordBuilder builder() {
        return new SetDatabaseRecordBuilder();
    }

    public static class SetDatabaseRecordBuilder {
        private Integer keySize = null;
        private byte[] key = null;
        private Integer valSize = null;
        private byte[] value = null;

        public SetDatabaseRecordBuilder keySize (int keySize) {
            this.keySize = keySize;
            return this;
        }

        public SetDatabaseRecordBuilder key(byte[] key) {
            this.key = key;
            return this;
        }

        public SetDatabaseRecordBuilder valSize(int valSize) {
            this.valSize = valSize;
            return this;
        }

        public SetDatabaseRecordBuilder value(byte[] value) {
            this.value = value;
            return this;
        }


        public SetDatabaseRecord build() throws DatabaseException {
            SetDatabaseRecord record = new SetDatabaseRecord();
            if (key == null || keySize == null || valSize == null)
                throw new DatabaseException("Cannot create database record without either key or key size" +
                        "or value size.");

            if (keySize < 0 || valSize < -1)
                throw new DatabaseException("Invalid key or value sizes.");

            if (valSize == -1 && value != null)
                throw new DatabaseException("Database should be empty with value size -1.");

            record.setKeySize(keySize);
            record.setKey(key);
            record.setValSize(valSize);
            record.setValue(value);

            return record;
        }
    }
}

