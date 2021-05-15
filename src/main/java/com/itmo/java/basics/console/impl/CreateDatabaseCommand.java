package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.DatabaseFactory;
import com.itmo.java.protocol.model.RespObject;

import java.util.List;

/**
 * Команда для создания базы данных
 */
public class CreateDatabaseCommand implements DatabaseCommand {
    private final static int REQUIRED_ARGS_NUM = 3;
    private final ExecutionEnvironment env;
    private final DatabaseFactory factory;
    private final List<RespObject> commandArgs;

    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param factory     функция создания базы данных (пример: DatabaseImpl::create)
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя создаваемой бд
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public CreateDatabaseCommand(ExecutionEnvironment env, DatabaseFactory factory, List<RespObject> commandArgs) {
        if (env == null || factory == null || commandArgs == null) {
            throw new Error ("Error while initializing CreateDatabaseCommand - one of the parameters is null.");
        }
        this.env = env;
        this.factory = factory;
        this.commandArgs = commandArgs;
        if (commandArgs.size() != REQUIRED_ARGS_NUM) {
            throw new IllegalArgumentException("Error while creating database cmd in server: incorrect number of args.");
        }
    }

    /**
     * Создает бд в нужном env
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с сообщением о том, что заданная база была создана. Например, "Database db1 created"
     */
    @Override
    public DatabaseCommandResult execute() {
        Database db;
        String databaseName = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
        try {
            db = factory.createNonExistent(databaseName, env.getWorkingPath());
        }
        catch (DatabaseException ex) {
            return DatabaseCommandResult.error("Error while executing CreateDatabaseCommand.");
        }
        env.addDatabase(db);
        return DatabaseCommandResult.success(("Database " + databaseName + " created.").getBytes());
    }
}
