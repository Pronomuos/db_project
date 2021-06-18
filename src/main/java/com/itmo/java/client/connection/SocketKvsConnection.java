package com.itmo.java.client.connection;

import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespObject;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;

/**
 * С помощью {@link RespWriter} и {@link RespReader} читает/пишет в сокет
 */
public class SocketKvsConnection implements KvsConnection {

    private final Socket socket;
    private final RespReader socketReader;
    private final RespWriter socketWriter;

    public SocketKvsConnection(ConnectionConfig config) {
        try {
            socket = new Socket(config.getHost(), config.getPort());
            socketReader = new RespReader(socket.getInputStream());
            socketWriter = new RespWriter(socket.getOutputStream());
        } catch (UnknownHostException ex) {
            throw new RuntimeException("Error while initializing SocketKvsConnection: unknown host.", ex);
        } catch (IOException ex) {
            throw new RuntimeException("Error while initializing SocketKvsConnection: ioexception.", ex);
        }
    }

    /**
     * Отправляет с помощью сокета команду и получает результат.
     * @param commandId id команды (номер)
     * @param command   команда
     * @throws ConnectionException если сокет закрыт или если произошла другая ошибка соединения
     */
    @Override
    public synchronized RespObject send(int commandId, RespArray command) throws ConnectionException {
        try {
            socketWriter.write(command);
            return socketReader.readObject();
        } catch (IOException ex) {
            throw new ConnectionException("Error while sending info from client socket: I/O error occurred", ex);
        }
    }

    /**
     * Закрывает сокет (и другие использованные ресурсы)
     */
    @Override
    public void close() {
        try {
            socketReader.close();
            socketWriter.close();
            socket.close();
        } catch (IOException ex) {
            throw new RuntimeException("Error while closing SocketKvsConnection.", ex);
        }

    }
}
