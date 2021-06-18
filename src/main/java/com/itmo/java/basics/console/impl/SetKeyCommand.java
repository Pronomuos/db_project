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
 * Команда для создания записи значения
 */
public class SetKeyCommand implements DatabaseCommand {
    private final static int REQUIRED_ARGS_NUM = 6;
    private final ExecutionEnvironment env;
    private final List<RespObject> commandArgs;

    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, таблицы, ключ, значение
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public SetKeyCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        if (env == null || commandArgs == null) {
            throw new Error ("Error while initializing SetKeyCommand - one of the parameters is null.");
        }
        this.env = env;
        this.commandArgs = commandArgs;
        if (commandArgs.size() != REQUIRED_ARGS_NUM) {
            throw new IllegalArgumentException("Error while creating table cmd in server: incorrect number of args.");
        }
    }

    /**
     * Записывает значение
     *
     * @return {@link DatabaseCommandResult#success(byte[])} c предыдущим значением. Например, "previous" или null, если такого не было
     */
    @Override
    public DatabaseCommandResult execute() {
        String databaseName = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
        String tableName = commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
        String key = commandArgs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString();
        String value = commandArgs.get(DatabaseCommandArgPositions.VALUE.getPositionIndex()).asString();
        Optional<Database> db = env.getDatabase(databaseName);
        if (db.isEmpty()) {
            return DatabaseCommandResult.error("Error while executing SetKeyCommand: no db called - " + databaseName + ".");
        }
        Optional<byte[]> res;
        try {
            res = db.get().read(tableName, key);
            db.get().write(tableName, key, value.getBytes());
        }
        catch (DatabaseException ex) {
            return DatabaseCommandResult.error("Error while executing SetKeyCommand.");
        }
        return DatabaseCommandResult.success(res.isEmpty() ? null : res.get());
    }
}
