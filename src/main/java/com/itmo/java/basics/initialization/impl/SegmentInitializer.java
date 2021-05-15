package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.impl.SegmentImpl;
import com.itmo.java.basics.logic.io.DatabaseInputStream;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Optional;

public class SegmentInitializer implements Initializer {

    /**
     * Добавляет в контекст информацию об инициализируемом сегменте.
     * Составляет индекс сегмента
     * Обновляет инфу в индексе таблицы
     *
     * @param context контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к сегменту, невозможно прочитать содержимое. Ошибка в содержании
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        Path path = context.currentSegmentContext().getSegmentPath();
        var keys = new ArrayList<String>();
        try {
            try (var inStream = new DatabaseInputStream(new BufferedInputStream(new FileInputStream(String.valueOf(path))))) {
                while (context.currentSegmentContext().getCurrentSize() < Files.size(path)) {
                    Optional<DatabaseRecord> record = inStream.readDbUnit();
                    if (record.isEmpty()) {
                        break;
                    }
                    var keyString = new String(record.get().getKey(), StandardCharsets.UTF_8);
                    context.currentSegmentContext().getIndex().onIndexedEntityUpdated(keyString,
                            new SegmentOffsetInfoImpl(context.currentSegmentContext().getCurrentSize()));
                    long offset = context.currentSegmentContext().getCurrentSize() + record.get().size();
                    context.currentSegmentContext().updateCurrentSize(offset);
                    keys.add(keyString);
                 }
                Segment segment = SegmentImpl.initializeFromContext(context.currentSegmentContext());
                for (String key : keys) {
                    context.currentTableContext().getTableIndex().onIndexedEntityUpdated(key, segment);
                }
                 context.currentTableContext().updateCurrentSegment(segment);
            }
        } catch (FileNotFoundException ex) {
            throw new DatabaseException("Cannot find segment with path - " + path.toString() + " in segment initialization.", ex);
        } catch (IOException ex) {
            throw new DatabaseException("Error while reading segment, called " + context.currentSegmentContext().getSegmentName() + " in segment initialization.", ex);
        }
    }
}


