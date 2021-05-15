package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.impl.DatabaseImpl;

import java.io.File;

public class DatabaseInitializer implements Initializer {
    private final TableInitializer tableInitializer;

    public DatabaseInitializer(TableInitializer tableInitializer) {
        this.tableInitializer = tableInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой бд.
     * Запускает инициализацию всех таблиц это базы
     *
     * @param initialContext контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к базе, невозможно прочитать содержимого папки,
     *                           или если возникла ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext initialContext) throws DatabaseException {
        try {
            File dbDir = new File(String.valueOf(initialContext.currentDbContext().getDatabasePath()));
            String[] tableNames = dbDir.list((current, name) -> new File(current, name).isDirectory());
            if (tableNames == null) {
                throw new DatabaseException(initialContext.currentDbContext().getDatabasePath() +
                        " does not denote a directory, or an I/O error occurred.");
            }
            for (String tableName : tableNames) {
                TableInitializationContext tableInitContext = new TableInitializationContextImpl(tableName,
                        initialContext.currentDbContext().getDatabasePath() , new TableIndex());
                InitializationContext downstreamContext = InitializationContextImpl.builder()
                        .executionEnvironment(initialContext.executionEnvironment())
                        .currentDatabaseContext(initialContext.currentDbContext())
                        .currentTableContext(tableInitContext)
                        .build();
                tableInitializer.perform(downstreamContext);
            }
            initialContext.executionEnvironment().addDatabase(DatabaseImpl.initializeFromContext(initialContext.currentDbContext()));
        } catch (Exception ex) {
            throw new DatabaseException("Error database initialization. Error while reading database, called - "
                    + initialContext.currentDbContext().getDbName() + ".", ex);
        }
    }
}