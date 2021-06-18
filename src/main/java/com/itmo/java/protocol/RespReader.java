package com.itmo.java.protocol;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespError;
import com.itmo.java.protocol.model.RespObject;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RespReader implements AutoCloseable {

    /**
     * Специальные символы окончания элемента
     */
    private static final byte CR = '\r';
    private static final byte LF = '\n';
    private final DataInputStream reader;

    public RespReader(InputStream is) {
       this.reader = new DataInputStream(is);
    }

    /**
     * Есть ли следующий массив в стриме?
     */
    public boolean hasArray() throws IOException {
        return reader.readByte() == RespArray.CODE;
    }

    /**
     * Считывает из input stream следующий объект. Может прочитать любой объект, сам определит его тип на основе кода объекта.
     * Например, если первый элемент "-", то вернет ошибку. Если "$" - bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespObject readObject() throws IOException {
        byte flag = reader.readByte();
        switch (flag) {
            case RespError.CODE:
                return readError();
            case RespBulkString.CODE:
                return readBulkString();
            case RespArray.CODE:
                return readArray();
            case RespCommandId.CODE:
                return readCommandId();
            default:
                throw new IOException("Error while reading RespObject: resp object is not a RespObject.");
        }
    }

    /**
     * Считывает объект ошибки
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespError readError() throws IOException {
        String message = readLine();
        return new RespError(message.getBytes());
    }

    /**
     * Читает bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespBulkString readBulkString() throws IOException {
        int size = Integer.parseInt(readLine());
        if (size == -1)
            return RespBulkString.NULL_STRING;
        byte[] data = reader.readNBytes(size);
        String end = readLine();
        if (!end.isEmpty()) {
            throw new IOException("Error while reading readBulkString: string is not of a given size.");
        }
        return new RespBulkString(data);
    }

    /**
     * Считывает массив RESP элементов
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespArray readArray() throws IOException {
        List<RespObject> objects = new ArrayList<>();
        int size = Integer.parseInt(readLine());
        for (int i = 0; i < size; i++) {
            objects.add(readObject());
        }
        return new RespArray(objects.toArray(new RespObject[objects.size()]));
    }

    /**
     * Считывает id команды
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespCommandId readCommandId() throws IOException {
        int commandId = reader.readInt();
        readLine();
        return new RespCommandId(commandId);
    }

    private String readLine() throws IOException {
        StringBuilder res = new StringBuilder();
        while (true) {
            char readByte = (char) reader.readByte();
            if (readByte == CR) {
                if ((char) reader.readByte() != LF) {
                    throw new IOException("Error while reading in RespReader: LF is absent.");
                }
                return res.toString();
            }
            res.append(readByte);
        }
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
