package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.protocol.model.RespObject;

import java.util.List;
import java.util.Optional;

/**
 * Команда для создания удаления значения по ключу
 */
public class DeleteKeyCommand implements DatabaseCommand {
    private final static int REQUIRED_ARGS_NUM = 5;
    private final ExecutionEnvironment env;
    private final List<RespObject> commandArgs;

    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, таблицы, ключ
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public DeleteKeyCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        if (env == null || commandArgs == null) {
            throw new Error ("Error while initializing DeleteKeyCommand - one of the parameters is null.");
        }
        this.env = env;
        this.commandArgs = commandArgs;
        if (commandArgs.size() != REQUIRED_ARGS_NUM) {
            throw new IllegalArgumentException("Error while creating table cmd in server: incorrect number of args.");
        }
    }

    /**
     * Удаляет значение по ключу
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с удаленным значением. Например, "previous"
     */
    @Override
    public DatabaseCommandResult execute() {
        String databaseName = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
        String tableName = commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
        String key = commandArgs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString();
        Optional<Database> db = env.getDatabase(databaseName);
        if (db.isEmpty()) {
            return DatabaseCommandResult.error("Error while executing DeleteKeyCommand: no db called - " + databaseName + ".");
        }
        Optional<byte[]> res;
        try {
            res = db.get().read(tableName, key);
            db.get().delete(tableName, key);
        }
        catch (DatabaseException ex) {
            return DatabaseCommandResult.error("Error while executing DeleteKeyCommand: no table called - " + tableName +
                    " in database " + databaseName + ".");
        }
        return DatabaseCommandResult.success(res.get());
    }
}
