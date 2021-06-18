package com.itmo.java.basics.resp;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommands;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespObject;

import java.io.IOException;
import java.util.List;

public class CommandReader implements AutoCloseable {

    private final RespReader reader;
    private final ExecutionEnvironment env;
    public CommandReader(RespReader reader, ExecutionEnvironment env) {
        this.reader = reader;
        this.env = env;
    }

    /**
     * Есть ли следующая команда в ридере?
     */
    public boolean hasNextCommand() throws IOException {
        return reader.hasArray();
    }

    /**
     * Считывает комманду с помощью ридера и возвращает ее
     *
     * @throws IllegalArgumentException если нет имени команды и id
     */
    public DatabaseCommand readCommand() throws IOException {
        List<RespObject> commandArgs = reader.readArray().getObjects();
        try {
            Integer.parseInt(commandArgs.get(DatabaseCommandArgPositions.COMMAND_ID.getPositionIndex()).asString());
        } catch (NumberFormatException | ArrayIndexOutOfBoundsException ex) {
            throw new IllegalArgumentException("Error while reading command in command reader: no id in the command.", ex);
        }
        return DatabaseCommands.valueOf(commandArgs.get(DatabaseCommandArgPositions.COMMAND_NAME.getPositionIndex()).asString())
                .getCommand(env, commandArgs);
    }

    @Override
    public void close() throws Exception {
        reader.close();
    }
}
