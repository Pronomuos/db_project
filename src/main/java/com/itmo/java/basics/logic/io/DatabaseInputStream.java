package com.itmo.java.basics.logic.io;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.impl.SetDatabaseRecord;

import java.io.*;
import java.util.Optional;

/**
 * Класс, отвечающий за чтение данных из БД
 */
public class DatabaseInputStream extends DataInputStream {
    private static final int REMOVED_OBJECT_SIZE = -1;

    public DatabaseInputStream(InputStream inputStream) {
        super(inputStream);
    }

    /**
     * Читает следующую запись (см {@link DatabaseOutputStream#write(WritableDatabaseRecord)})
     * @return следующую запись, если она существует. {@link Optional#empty()} - если конец файла достигнут
     */
    public Optional<DatabaseRecord> readDbUnit() throws IOException {
        Optional<DatabaseRecord> record = Optional.empty();
        int keySize, valSize;
        byte [] key, value;
        try {
            keySize = readInt();
            key = new byte[keySize];
            for (int i = 0; i < keySize; ++i)
                key[i] = readByte();
            valSize = readInt();
            value = new byte[keySize];
            for (int i = 0; i < keySize; ++i)
                value[i] = readByte();
        } catch (EOFException ex) {
            return record;
        } catch (IOException ex) {
            throw new IOException("Error while reading data.", ex);
        }

        try {
             record = Optional.of(SetDatabaseRecord.builder().
                    keySize(keySize).
                    key(key).
                    valSize(valSize).
                    value(value).
                    build());
        } catch (DatabaseException ex) {
            throw new IOException("Error while converting data into record.", ex);
        }

        return record;
    }
}
