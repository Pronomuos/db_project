package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import com.itmo.java.basics.logic.impl.TableImpl;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;


public class TableInitializer implements Initializer {
    private final SegmentInitializer segmentInitializer;

    public TableInitializer(SegmentInitializer segmentInitializer) {
        this.segmentInitializer = segmentInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой таблице.
     * Запускает инициализацию всех сегментов в порядке их создания (из имени)
     *
     * @param context контекст с информацией об инициализируемой бд, окружении, таблицы
     * @throws DatabaseException если в контексте лежит неправильный путь к таблице, невозможно прочитать содержимого папки,
     *  или если возникла ошибка ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        try {
            File tableDir = new File(String.valueOf(context.currentTableContext().getTablePath()));
            File[] segmentDirs = tableDir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File f, String name) {
                    return name.startsWith(context.currentTableContext().getTableName());
                }
            });
            if (segmentDirs == null) {
                throw new DatabaseException(context.currentTableContext().getTablePath() +
                        " does not denote a directory, or an I/O error occurred.");
            }
            Arrays.sort(segmentDirs);
            for (var segment : segmentDirs) {
                SegmentInitializationContext segmentInitContext = new SegmentInitializationContextImpl(segment.getName(), segment.toPath().getParent());
                InitializationContext downstreamContext = InitializationContextImpl.builder()
                        .executionEnvironment(context.executionEnvironment())
                        .currentDatabaseContext(context.currentDbContext())
                        .currentTableContext(context.currentTableContext())
                        .currentSegmentContext(segmentInitContext)
                        .build();
                segmentInitializer.perform(downstreamContext);
            }
            context.currentDbContext().addTable(TableImpl.initializeFromContext(context.currentTableContext()));
        }
        catch (Exception ex) {
            throw new DatabaseException("Error in table initialization. Error while reading table, called - "
                    + context.currentTableContext().getTableName() + ".", ex);
        }
    }
}
