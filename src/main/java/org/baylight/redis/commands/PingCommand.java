package org.baylight.redis.commands;

import org.baylight.redis.RedisServiceBase;

public class PingCommand extends RedisCommand {

    public PingCommand() {
        super(Type.PING);
    }

    @Override
    public byte[] execute(RedisServiceBase service) {
        return "+PONG\r\n".getBytes();
    }

    @Override
    public String toString() {
        return "PingCommand";
    }
}
