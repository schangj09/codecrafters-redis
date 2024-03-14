package org.baylight.redis.commands;

import java.io.IOException;

import org.baylight.redis.EofCommand;
import org.baylight.redis.TerminateCommand;
import org.baylight.redis.protocol.RespArrayValue;
import org.baylight.redis.protocol.RespType;
import org.baylight.redis.protocol.RespValue;

/**
 * The RedisCommandParser class is responsible for parsing Redis commands from a
 * BufferedInputLineReader and returning the corresponding RedisCommand object.
 */
public class RedisCommandConstructor {

    /**
     * Parses a Redis command from the given BufferedInputLineReader and returns the corresponding
     * RedisCommand object. It uses the RespValueParser class to parse the command value and then
     * determines the command type based on the first element of the parsed array. It calls the
     * getCommand method to create the appropriate RedisCommand object and sets its arguments using
     * the parsed array values.
     *
     * @param reader The BufferedInputLineReader from which to parse the Redis command.
     * @return The RedisCommand object representing the parsed Redis command.
     * @throws IOException If an I/O error occurs while reading the input.
     */
    public RedisCommand newCommandFromValue(RespValue value) {
        if (value == null) {
            return null;
        }
        if (value.getType() == RespType.ARRAY) {
            return getCommand((RespArrayValue) value);
        } else {
            return getCommand(new RespArrayValue(new RespValue[] { value }));
        }
    }

    /**
     * Creates and returns the appropriate RedisCommand object based on the command type specified
     * in the 0 position of the given RespArrayValue. It sets the command arguments using the values
     * of the RespArrayValue.
     *
     * @param array The RespArrayValue containing the parsed Redis command.
     * @return The RedisCommand object representing the parsed Redis command.
     */
    RedisCommand getCommand(RespArrayValue array) {
        // if the array was read from a ClientConnection input stream, then it
        // carries the context of the start bytes offset for ReplConf GETACK command
        // - otherwise, just default to 0L
        long arrayStartBytesOffset = array.getContext() == null ? 0L
                : array.getContext().getStartBytesOffset();
        String command = getCommandName(array.getValues()[0]);
        RedisCommand.Type commandType = RedisCommand.Type.of(command);
        RedisCommand redisCommand = switch (commandType) {
        case ECHO -> new EchoCommand();
        case GET -> new GetCommand();
        case INFO -> new InfoCommand();
        case PING -> new PingCommand();
        case PSYNC -> new PsyncCommand();
        case REPLCONF -> new ReplConfCommand(arrayStartBytesOffset);
        case SET -> new SetCommand();
        case WAIT -> new WaitCommand();
        // special non-standard commands
        case EOF -> new EofCommand();
        case TERMINATE -> new TerminateCommand();
        case null, default -> {
            System.out.println("Unknown command: " + command);
            yield null;
        }
        };
        if (redisCommand != null) {
            redisCommand.setArgs(array.getValues());
        }
        return redisCommand;
    }

    /**
     * Returns the name of the Redis command as an uppercase string.
     *
     * @param value The RespValue object containing the Redis command name.
     * @return The name of the Redis command as a string.
     */
    String getCommandName(RespValue value) {
        String name = value.getValueAsString();
        return name != null ? name.toUpperCase() : null;
    }

}
