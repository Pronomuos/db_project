package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

/**
 * Массив RESP объектов
 */
public class RespArray implements RespObject {

    /**
     * Код объекта
     */
    public static final byte CODE = '*';
    private final List<RespObject> objects;

    public RespArray(RespObject... objects) {
        this.objects = Arrays.asList(objects);
    }

    /**
     * Ошибка ли это? Ответ - нет
     *
     * @return false
     */
    @Override
    public boolean isError() {
        return false;
    }

    /**
     * Строковое представление
     *
     * @return результаты метода {@link RespObject#asString()} для всех хранимых объектов, разделенные пробелом
     */
    @Override
    public String asString() {
        int capacity = 0;
        for (RespObject obj : objects) {
            capacity += obj.asString().length() + 1;
        }
        var strBuilder = new StringBuilder(capacity);
        for (RespObject obj : objects) {
            strBuilder.append(obj.asString()).append(' ');
        }
        return strBuilder.toString();
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        os.write(String.valueOf(objects.size()).getBytes());
        os.write(CRLF);
        for (RespObject obj : objects) {
            obj.write(os);
        }
    }

    public List<RespObject> getObjects() {
        return objects;
    }
}
