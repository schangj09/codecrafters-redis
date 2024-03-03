package org.baylight.redis.commands;

import org.baylight.redis.RedisServiceBase;
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
    public void setArgs(RespValue[] args) {
        validateNumArgs(args, len -> len == 2);
        validateArgIsString(args, 1);

        this.bulkStringArg = args[1].asBulkString();
    }

    @Override
    public byte[] execute(RedisServiceBase service) {
        return bulkStringArg != null ? bulkStringArg.asResponse() : new byte[] {};
    }

    @Override
    public String toString() {
        return "EchoCommand [bulkStringArg=" + bulkStringArg + "]";
    }
}
