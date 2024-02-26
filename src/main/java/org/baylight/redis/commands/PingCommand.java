package org.baylight.redis.commands;
public class PingCommand extends RedisCommand {

    public PingCommand() {
        super(Type.PING);
    }

    @Override
    public byte[] getResponse() {
        return "+PONG\r\n".getBytes();
    }

    @Override
    public String toString() {
        return "PingCommand";
    }
}
