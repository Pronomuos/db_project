package com.itmo.java.basics.connector;

import com.itmo.java.basics.DatabaseServer;
import com.itmo.java.basics.config.ConfigLoader;
import com.itmo.java.basics.config.DatabaseServerConfig;
import com.itmo.java.basics.config.ServerConfig;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.console.impl.ExecutionEnvironmentImpl;
import com.itmo.java.basics.initialization.impl.DatabaseInitializer;
import com.itmo.java.basics.initialization.impl.DatabaseServerInitializer;
import com.itmo.java.basics.initialization.impl.SegmentInitializer;
import com.itmo.java.basics.initialization.impl.TableInitializer;
import com.itmo.java.basics.resp.CommandReader;
import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Класс, который предоставляет доступ к серверу через сокеты
 */
public class JavaSocketServerConnector implements Closeable {

    /**
     * Экзекьютор для выполнения ClientTask
     */
    private final ExecutorService clientIOWorkers = Executors.newSingleThreadExecutor();
    private final ServerSocket serverSocket;
    private final ExecutorService connectionAcceptorExecutor = Executors.newSingleThreadExecutor();
    private final DatabaseServer databaseServer;

    /**
     * Стартует сервер. По аналогии с сокетом открывает коннекшн в конструкторе.
     * <p>
     * Начинает слушать заданный порт, начинает аксептить клиентские сокеты. На каждый из них начинает клиентскую таску
     */
    public JavaSocketServerConnector(DatabaseServer databaseServer, ServerConfig config) throws IOException {
        this.databaseServer = databaseServer;
        this.serverSocket = new ServerSocket(config.getPort());
    }

    public void start() {
        connectionAcceptorExecutor.submit(() -> {
            try {
                while (!serverSocket.isClosed()) {
                    Socket client = serverSocket.accept();
                    clientIOWorkers.submit(new ClientTask(client, databaseServer));
                }
            } catch (IOException ex) {
                close();
                throw new RuntimeException("Error while running server: ioexception.", ex);
            }
        });
    }

    /**
     * Закрывает все, что нужно ¯\_(ツ)_/¯
     */
    @Override
    public void close() {
        try {
            System.out.println("Stopping socket connector");
            clientIOWorkers.shutdownNow();
            serverSocket.close();
            connectionAcceptorExecutor.shutdownNow();
        } catch (IOException ex) {
            throw new RuntimeException("Error while closing JavaSocketServerConnector: ioexception.", ex);
        }
    }

    public static void main(String[] args) throws Exception {
        ConfigLoader loader = new ConfigLoader();
        DatabaseServerConfig dbSrvConfig = loader.readConfig();
        DatabaseServerInitializer initializer =
                new DatabaseServerInitializer(
                        new DatabaseInitializer(
                                new TableInitializer(
                                        new SegmentInitializer())));
        ExecutionEnvironment env = new ExecutionEnvironmentImpl(dbSrvConfig.getDbConfig());
        DatabaseServer server = DatabaseServer.initialize(env, initializer);
        JavaSocketServerConnector connector = new JavaSocketServerConnector(server, dbSrvConfig.getServerConfig());
        connector.start();
    }

    /**
     * Runnable, описывающий исполнение клиентской команды.
     */
    static class ClientTask implements Runnable, Closeable {

        private final Socket client;
        private final DatabaseServer server;
        private final CommandReader clientReader;
        private final RespWriter clientWriter;
        /**
         * @param client клиентский сокет
         * @param server сервер, на котором исполняется задача
         */
        public ClientTask(Socket client, DatabaseServer server) {
            try {
                this.client = client;
                this.server = server;
                this.clientReader = new CommandReader(new RespReader(client.getInputStream()), server.getEnv());
                this.clientWriter = new RespWriter(client.getOutputStream());
            } catch (IOException ex) {
                throw new RuntimeException("Error while initializing client task: ioexception.", ex);
            }
        }

        /**
         * Исполняет задачи из одного клиентского сокета, пока клиент не отсоединился или текущий поток не был прерван (interrupted).
         * Для кажной из задач:
         * 1. Читает из сокета команду с помощью {@link CommandReader}
         * 2. Исполняет ее на сервере
         * 3. Записывает результат в сокет с помощью {@link RespWriter}
         */
        @Override
        public void run() {
            try {
                while (true) {
                    if (clientReader.hasNextCommand()) {
                        clientWriter.write(server.executeNextCommand(clientReader.readCommand()).get().serialize());
                    }
                }
            } catch (IOException | CancellationException | InterruptedException ex) {
                close();
            } catch (ExecutionException ex) {
                throw new RuntimeException("Error while running client task: this future completed exceptionally", ex);
            }
        }

        /**
         * Закрывает клиентский сокет
         */
        @Override
        public void close() {
            try {
                clientReader.close();
                clientWriter.close();
                client.close();
            } catch (Exception ex) {
                throw new RuntimeException("Error while closing ClientTask.", ex);
            }

        }
    }
}