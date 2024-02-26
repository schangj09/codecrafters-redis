package org.baylight.redis.commands;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.baylight.redis.EofCommand;
import org.baylight.redis.RedisService;
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
            case ECHO -> new EchoCommand();
            case GET -> new GetCommand();
            case PING -> new PingCommand();
            case SET -> new SetCommand();
            // special non-standard commands
            case EOF -> new EofCommand();
            case TERMINATE -> new TerminateCommand();
            case null, default -> {
                System.out.println("Unknown command: " + command);
                yield null;
            }
        };
    }

    public enum Type {
        GET,
        ECHO,
        PING,
        SET,
        // Folling are non-standard commands for baylight
        EOF, // close a client connection
        TERMINATE; // close all connections and kill the server

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

    protected void setArgs(RespValue[] args) {
        // ignore by default
    }
    protected void validateNumArgs(RespValue[] args, Function<Integer, Boolean> validLengthCondition) {
        if (!validLengthCondition.apply(args.length)) {
            throw new RuntimeException(
                String.format("%s: Invalid number of arguments: %d", type.name(), args.length));
        }
    }

    protected void validateArgIsString(RespValue[] args, int index) {
        RespValue arg = args[index];
        if (!arg.isBulkString() && !arg.isSimpleString()) {
            throw new RuntimeException(String.format("%s: Invalid arg, expected string. %d: %s", type.name(), index, arg));
        }
    }

    public void validateArgIsInteger(RespValue[] args, int index) {
        RespValue arg = args[index];
        if (arg.getValueAsLong() == null) {
            throw new RuntimeException(String.format("%s: Invalid arg, expected integer %d: %s", type.name(), index, arg));
        }
    }

    public void validateArgForStateTransition(
        RespValue[] args, int i, int state, int nextState, Map<Integer, List<Integer>> transitions
    ) {
        if (nextState == -1 || !transitions.get(state).contains(nextState)) {
            throw new IllegalArgumentException(
                String.format("%s: Invalid or missing argument at index. %d ", type, i, Arrays.toString(args)));
        }
    }


    public abstract byte[] execute(RedisService service);

    public abstract String toString();
}
