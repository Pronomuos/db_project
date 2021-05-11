package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.impl.DatabaseImpl;

import java.io.File;

public class DatabaseServerInitializer implements Initializer {
    private final DatabaseInitializer databaseInitializer;

    public DatabaseServerInitializer(DatabaseInitializer databaseInitializer) {
        this.databaseInitializer = databaseInitializer;
    }

    /**
     * Если заданная в окружении директория не существует - создает ее
     * Добавляет информацию о существующих в директории базах, нацинает их инициалиализацию
     *
     * @param context контекст, содержащий информацию об окружении
     * @throws DatabaseException если произошла ошибка при создании директории, ее обходе или ошибка инициализации бд
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        try {
            File envDir = new File(String.valueOf(context.executionEnvironment().getWorkingPath()));
            if (!envDir.exists()) {
                envDir.mkdir();
            } else {
                String[] databaseNames = envDir.list((current, name) -> new File(current, name).isDirectory());
                if (databaseNames == null) {
                    throw new DatabaseException(context.executionEnvironment().getWorkingPath() +
                            " does not denote a directory, or an I/O error occurred.");
                }
                for (var databaseName : databaseNames) {
                    var databaseInitContext = new DatabaseInitializationContextImpl(databaseName,
                            context.executionEnvironment().getWorkingPath());
                    var downstreamContext = InitializationContextImpl.builder()
                            .executionEnvironment(context.executionEnvironment())
                            .currentDatabaseContext(databaseInitContext)
                            .build();
                    databaseInitializer.perform(downstreamContext);
                }
            }
        }
        catch (Exception ex) {
            throw new DatabaseException("Error in database server initialization. Error while reading environment with path - "
                    + context.executionEnvironment().getWorkingPath() + ".", ex);
        }
    }
}

