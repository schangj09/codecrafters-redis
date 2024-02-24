package org.baylight.redis.protocol;

import java.io.IOException;

import org.baylight.redis.EofCommand;
import org.baylight.redis.TerminateCommand;
import org.baylight.redis.io.BufferedInputLineReader;

public abstract class RedisCommand {
    public static RedisCommand parseCommand(BufferedInputLineReader reader) throws IOException {
        RespValue value = RespTypeParser.parse(reader);
        if (value == null) {
            return null;
        }
        if (value.isBulkString()) {
            RespBulkString bulkString = (RespBulkString) value;
            return getCommand(bulkString);
        } else if (value.getType() == RespType.ARRAY) {
            RespArrayValue array = (RespArrayValue) value;
            if (array.getSize() >= 1) {
                RespBulkString bulkString = (RespBulkString) array.getValues()[0];
                RedisCommand command = getCommand(bulkString);
                if (command != null) {
                    command.setArgs(array.getValues());
                    return command;
                }
            }
        }
        return null;
    }

    private static RedisCommand getCommand(RespBulkString bulkString) {
        String command = bulkString.getValueAsString().toUpperCase();

        try {
            RedisCommand.Type commandType = RedisCommand.Type.valueOf(command);
            return switch (commandType) {
                case PING -> new PingCommand();
                case TERMINATE -> new TerminateCommand();
                case EOF -> new EofCommand();
                case ECHO -> new EchoCommand();
                case null, default -> {
                    System.out.println("Unknown command: " + command);
                    yield null;
                }
            };
        } catch (Exception e) {
            System.out.println("Unknown command: " + command);
            return null;
        }
    }

    public enum Type {
        PING,
        ECHO,
        TERMINATE,
        EOF
    }

    private final Type type;

    public RedisCommand(Type type) {
        this.type = type;
    }

    protected void setArgs(RespValue[] values) {
        // ignore by default
    }

    public abstract byte[] getResponse();

    public abstract String toString();
}
