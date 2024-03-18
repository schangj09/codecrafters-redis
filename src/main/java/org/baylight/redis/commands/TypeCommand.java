package org.baylight.redis.commands;

import java.util.Map;

import org.baylight.redis.RedisServiceBase;
import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespValue;

/**
 * Represents a TYPE command in a Redis server. This class is a subclass of RedisCommand and is
 * responsible for setting the command arguments and executing the command.
 */
public class TypeCommand extends RedisCommand {
    private RespBulkString key;

    /**
     * Constructs a new TypeCommand object with the TYPE command type.
     */
    public TypeCommand() {
        super(Type.TYPE);
    }

    /**
     * Constructs a new TypeCommand object with the TYPE command type and the specified key.
     * 
     * @param key the key for the TYPE command
     */
    public TypeCommand(RespBulkString key) {
        super(Type.TYPE);
        this.key = key;
    }

    /**
     * Types the key for the TYPE command.
     * 
     * @return the key for the TYPE command
     */
    public RespBulkString getKey() {
        return key;
    }

    /**
     * Sets the command arguments by parsing the provided RespValue array. The arguments should
     * contain the key as the first element.
     * 
     * @param args the command arguments
     */
    @Override
    public void setArgs(RespValue[] args) {
        ArgReader argReader = new ArgReader(type.name(), new String[] { ":string", // command name
                ":string" // key
        });
        Map<String, RespValue> optionsMap = argReader.readArgs(args);
        this.key = optionsMap.get("1").asBulkString();
    }

    /**
     * Executes the TYPE command by retrieving the value associated with the specified key from the
     * Redis service. If the key exists and is not expired, the value is returned as a byte array.
     * If the key does not exist or is expired, null is returned.
     * 
     * @param service the Redis service to execute the command on
     * @return the value associated with the key, or null if the key does not exist or is expired
     */
    @Override
    public byte[] execute(RedisServiceBase service) {
        return service.getType(key.getValueAsString()).asResponse();
    }

    /**
     * Returns a string representation of the TypeCommand object.
     * 
     * @return a string representation of the TypeCommand object
     */
    @Override
    public String toString() {
        return "TypeCommand [key=" + key + "]";
    }

}
