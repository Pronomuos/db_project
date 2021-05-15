package com.itmo.java.client.connection;

import com.itmo.java.basics.DatabaseServer;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespObject;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * Реализация подключения, когда есть прямая ссылка на объект
 * (пока еще нет реализации сокетов)
 */
public class DirectReferenceKvsConnection implements KvsConnection {

    private final DatabaseServer databaseServer;

    public DirectReferenceKvsConnection(DatabaseServer databaseServer) {
        this.databaseServer = databaseServer;
    }

    @Override
    public RespObject send(int commandId, RespArray command) throws ConnectionException {
        List<RespObject> objects = command.getObjects();
        DatabaseCommandResult result;
        try {
            result = databaseServer.executeNextCommand(new RespArray(command.getObjects().toArray(new RespObject[objects.size()]))).get();
        }
        catch (CancellationException ex) {
             throw new ConnectionException("Error while sending a command to server: this future was cancelled.", ex);
        }
        catch (ExecutionException ex) {
            throw new ConnectionException("Error while sending a command to server: this future completed exceptionally", ex);
        }
        catch (InterruptedException ex) {
            throw new ConnectionException("Error while sending a command to server: the current thread was interrupted while waiting", ex);
        }
        return result.serialize();
    }

    /**
     * Ничего не делает ¯\_(ツ)_/¯
     */
    @Override
    public void close() {
    }
}
