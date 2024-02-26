package org.baylight.redis;
import org.baylight.redis.commands.RedisCommand;

public class TerminateCommand extends RedisCommand {

    public TerminateCommand() {
        super(Type.TERMINATE);
    }

    @Override
    public byte[] getResponse() {
        return null;
    }

    @Override
    public String toString() {
        return "TerminateCommand []";
    }

}
