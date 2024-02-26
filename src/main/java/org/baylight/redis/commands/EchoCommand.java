package org.baylight.redis.commands;

import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespValue;

public class EchoCommand extends RedisCommand {

    RespBulkString bulkStringArg;

    public EchoCommand() {
        super(Type.ECHO);
    }

    public EchoCommand(RespBulkString bulkStringArg) {
        super(Type.ECHO);
        this.bulkStringArg = bulkStringArg;
    }

    @Override
    public void setArgs(RespValue[] values) {
        if (values.length != 2) {
            throw new RuntimeException(String.format("EchoCommand: Invalid number of arguments: %d", values.length));
        }
        if (!values[1].isBulkString() && !values[1].isSimpleString()) {
            throw new RuntimeException(String.format("EchoCommand: Invalid argument: %s", values[1]));
        }
        this.bulkStringArg = values[1].isBulkString()
                ? (RespBulkString) values[1]
                : new RespBulkString(values[1].getValueAsString().getBytes());
    }

    @Override
    public byte[] getResponse() {
        return bulkStringArg != null ? bulkStringArg.asResponse() : new byte[] {};
    }

    @Override
    public String toString() {
        return "EchoCommand [bulkStringArg=" + bulkStringArg + "]";
    }
}
