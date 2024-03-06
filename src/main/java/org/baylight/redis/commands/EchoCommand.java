package org.baylight.redis.commands;

import org.baylight.redis.RedisServiceBase;
import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespConstants;
import org.baylight.redis.protocol.RespValue;

/**
 * The `EchoCommand` class is a subclass of the `RedisCommand` class and represents a command to
 * echo a bulk string argument in a Redis service.
 */
public class EchoCommand extends RedisCommand {

    RespBulkString bulkStringArg;

    /**
     * Return the echo value.
     * 
     * @return the value to be echoed
     */
    public RespBulkString getEchoValue() {
        return bulkStringArg;
    }

    /**
     * Creates an instance of `EchoCommand` with no arguments.
     */
    public EchoCommand() {
        super(Type.ECHO);
    }

    /**
     * Creates an instance of `EchoCommand` with a bulk string argument.
     *
     * @param bulkStringArg the bulk string argument for the command
     */
    public EchoCommand(RespBulkString bulkStringArg) {
        super(Type.ECHO);
        this.bulkStringArg = bulkStringArg;
    }

    /**
     * Sets the arguments for the command. It validates the number of arguments and ensures that the
     * second argument is a string.
     *
     * @param args the arguments for the command
     * @throws IllegalArgumentException if the number of arguments is not 2 or if the second
     *                                  argument is not a string
     */
    @Override
    public void setArgs(RespValue[] args) {
        validateNumArgs(args, len -> len == 2);
        validateArgIsString(args, 1);

        this.bulkStringArg = args[1].asBulkString();
    }

    /**
     * Executes the command in a Redis service. It returns the bulk string argument as the response.
     *
     * @param service the Redis service to execute the command in
     * @return the bulk string argument as the response
     */
    @Override
    public byte[] execute(RedisServiceBase service) {
        return bulkStringArg != null ? bulkStringArg.asResponse() : RespConstants.NULL;
    }

    /**
     * Returns a string representation of the `EchoCommand` object.
     *
     * @return a string representation of the `EchoCommand` object
     */
    @Override
    public String toString() {
        return "EchoCommand [bulkStringArg=" + bulkStringArg + "]";
    }
}
