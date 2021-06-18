package com.itmo.java.basics.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Класс, отвечающий за подгрузку данных из конфигурационного файла формата .properties
 */
public class ConfigLoader {
    private String configFileName = "server.properties";
    /**
     * По умолчанию читает из server.properties
     */
    public ConfigLoader() {}

    /**
     * @param name Имя конфикурационного файла, откуда читать
     */
    public ConfigLoader(String name) {
       configFileName = name;
    }

    /**
     * Считывает конфиг из указанного в конструкторе файла.
     * Если не удалось считать из заданного фойла, или какого-то конкретно значения не оказалось,
     * то используют дефолтные значения из {@link DatabaseConfig} и {@link ServerConfig}
     * <br/>
     * Читаются: "kvs.workingPath", "kvs.host", "kvs.port" (но в конфигурационном файле допустимы и другие проперти
     */
    public DatabaseServerConfig readConfig() {
        DatabaseServerConfig dvSrvConfig;
        try {
            Properties properties = new Properties();
            InputStream input;
             try {
                 input = new FileInputStream(configFileName);
             } catch (IOException ex) {
                 input = getClass().getClassLoader().getResourceAsStream(configFileName);
             }
            if (input != null) {
                properties.load(input);
            }
            String workingPath = properties.getProperty("kvs.workingPath");
            if (workingPath == null) {
                workingPath = DatabaseConfig.DEFAULT_WORKING_PATH;
            }
            String host = properties.getProperty("kvs.host");
            if (host == null) {
                host = ServerConfig.DEFAULT_HOST;
            }
            String portStr = properties.getProperty("kvs.port");
            int port;
            if (portStr == null) {
                port = ServerConfig.DEFAULT_PORT;
            }
            else {
                port = Integer.parseInt(portStr);
            }
            DatabaseConfig dbConfig = new DatabaseConfig(workingPath);
            ServerConfig srvConfig = new ServerConfig(host, port);
            dvSrvConfig = DatabaseServerConfig.builder()
                    .dbConfig(dbConfig)
                    .serverConfig(srvConfig)
                    .build();
        } catch (IOException ex) {
            throw new RuntimeException("Error while reading config file: loading properties.", ex);
        }
        return dvSrvConfig;
    }
}
