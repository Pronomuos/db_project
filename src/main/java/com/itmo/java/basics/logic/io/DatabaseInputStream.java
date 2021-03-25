package com.itmo.java.basics.logic.io;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.impl.RemoveDatabaseRecord;
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
        DatabaseRecord record;
        try {
            int keySize = readInt();
            if (keySize <= 0)
                throw new DatabaseException("Key size is <= 0 while reading data.");
            byte[] key = new byte [keySize];
            read(key);
            int valSize = readInt();
            if (valSize != -1) {
                byte[] value = new byte[valSize];
                read(value);
                record = new SetDatabaseRecord(key, value);
            }
            else
                record = new RemoveDatabaseRecord(key);
        } catch (EOFException ex) {
            return Optional.empty();
        } catch (IOException ex) {
            throw new IOException("Error while reading data.", ex);
        } catch (DatabaseException ex) {
            throw new IOException("Error while converting data into record.", ex);
        }

        return Optional.of(record);
    }
}



