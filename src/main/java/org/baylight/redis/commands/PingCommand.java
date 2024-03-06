package org.baylight.redis.commands;

import org.baylight.redis.RedisServiceBase;
import org.baylight.redis.protocol.RespArrayValue;
import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespValue;

public class PingCommand extends RedisCommand {

    public PingCommand() {
        super(Type.PING);
    }

    @Override
    public byte[] execute(RedisServiceBase service) {
        return "+PONG\r\n".getBytes();
    }

    @Override
    public byte[] asCommand() {
        return new RespArrayValue(
                new RespValue[] { new RespBulkString(getType().name().getBytes()) }).asResponse();

    }

    @Override
    public String toString() {
        return "PingCommand";
    }
}
