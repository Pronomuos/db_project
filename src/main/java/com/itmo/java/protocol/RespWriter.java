package com.itmo.java.protocol;

import com.itmo.java.protocol.model.RespObject;

import java.io.IOException;
import java.io.OutputStream;

public class RespWriter implements AutoCloseable{


    private final OutputStream os;

    public RespWriter(OutputStream os) {
        this.os = os;
    }

    /**
     * Записывает в output stream объект
     */
    public void write(RespObject object) throws IOException {
        try {
            object.write(os);
            os.flush();
        } catch (Exception ex) {
            throw new IOException("Error while writing RespObject.", ex);
        }

    }

    @Override
    public void close() throws IOException {
        os.close();
    }
}
