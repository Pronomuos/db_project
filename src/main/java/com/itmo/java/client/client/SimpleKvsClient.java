package com.itmo.java.client.client;

import com.itmo.java.client.command.*;
import com.itmo.java.client.connection.KvsConnection;
import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.client.exception.DatabaseExecutionException;
import com.itmo.java.protocol.model.RespObject;

import java.util.function.Supplier;

public class SimpleKvsClient implements KvsClient {

    private final String databaseName;
    private final Supplier<KvsConnection> connectionSupplier;

    /**
     * Конструктор
     *
     * @param databaseName       имя базы, с которой работает
     * @param connectionSupplier метод создания подключения к базе
     */
    public SimpleKvsClient(String databaseName, Supplier<KvsConnection> connectionSupplier) {
       this.databaseName = databaseName;
       this.connectionSupplier = connectionSupplier;
    }

    @Override
    public String createDatabase() throws DatabaseExecutionException {
        var cmd = new CreateDatabaseKvsCommand(databaseName);
        RespObject res;
        try {
            res = connectionSupplier.get().send(cmd.getCommandId(), cmd.serialize());
        }
        catch (ConnectionException ex) {
            throw new DatabaseExecutionException ("Error while creating a cmd for database in client.", ex);
        }
        if (res.isError()) {
            throw new DatabaseExecutionException("Executed create database cmd was failed. " + res.asString());
        }
        return res.asString();
    }

    @Override
    public String createTable(String tableName) throws DatabaseExecutionException {
        var cmd = new CreateTableKvsCommand(databaseName, tableName);
        RespObject res;
        try {
            res = connectionSupplier.get().send(cmd.getCommandId(), cmd.serialize());
        }
        catch (ConnectionException ex) {
            throw new DatabaseExecutionException ("Error while creating a cmd for table in client.", ex);
        }
        if (res.isError()) {
            throw new DatabaseExecutionException("Executed create table cmd was failed. " + res.asString());
        }
        return res.asString();
    }

    @Override
    public String get(String tableName, String key) throws DatabaseExecutionException {
        var cmd = new GetKvsCommand(databaseName, tableName, key);
        RespObject res;
        try {
            res = connectionSupplier.get().send(cmd.getCommandId(), cmd.serialize());
        }
        catch (ConnectionException ex) {
            throw new DatabaseExecutionException ("Error while creating a cmd to get a value in client.", ex);
        }
        if (res.isError()) {
            throw new DatabaseExecutionException("Executed get cmd was failed. " + res.asString());
        }
        return res.asString();
    }

    @Override
    public String set(String tableName, String key, String value) throws DatabaseExecutionException {
        var cmd = new SetKvsCommand(databaseName, tableName, key, value);
        RespObject res;
        try {
            res = connectionSupplier.get().send(cmd.getCommandId(), cmd.serialize());
        }
        catch (ConnectionException ex) {
            throw new DatabaseExecutionException ("Error while creating a cmd to set a value in client.", ex);
        }
        if (res.isError()) {
            throw new DatabaseExecutionException("Executed set cmd was failed. " + res.asString());
        }
        return res.asString();
    }

    @Override
    public String delete(String tableName, String key) throws DatabaseExecutionException {
        var cmd = new DeleteKvsCommand(databaseName, tableName, key);
        RespObject res;
        try {
            res = connectionSupplier.get().send(cmd.getCommandId(), cmd.serialize());
        }
        catch (ConnectionException ex) {
            throw new DatabaseExecutionException ("Error while creating a cmd to delete a value in client.", ex);
        }
        if (res.isError()) {
            throw new DatabaseExecutionException("Executed delete cmd was failed. " + res.asString());
        }
        return res.asString();
    }
}
