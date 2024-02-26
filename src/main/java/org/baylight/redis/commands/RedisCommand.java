package org.baylight.redis.commands;

import java.io.IOException;

import org.baylight.redis.EofCommand;
import org.baylight.redis.TerminateCommand;
import org.baylight.redis.io.BufferedInputLineReader;
import org.baylight.redis.protocol.RespArrayValue;
import org.baylight.redis.protocol.RespType;
import org.baylight.redis.protocol.RespTypeParser;
import org.baylight.redis.protocol.RespValue;

public abstract class RedisCommand {
    public static RedisCommand parseCommand(BufferedInputLineReader reader) throws IOException {
        RespValue value = RespTypeParser.parse(reader);
        if (value == null) {
            return null;
        }
        if (value.getType() == RespType.ARRAY) {
            RespArrayValue array = (RespArrayValue) value;
            if (array.getSize() >= 1) {
                RedisCommand command = getCommand(array.getValues()[0]);
                if (command != null) {
                    command.setArgs(array.getValues());
                    return command;
                }
            }
        }
        return getCommand(value);
    }

    private static RedisCommand getCommand(RespValue value) {
        String command = value.getValueAsString().toUpperCase();
        RedisCommand.Type commandType = RedisCommand.Type.of(command);
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
    }

    public enum Type {
        PING,
        ECHO,
        TERMINATE,
        EOF;

        static Type of(String command) {
            try {
                return Type.valueOf(command);
            } catch (Exception e) {
                return null;
            }
        }
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
