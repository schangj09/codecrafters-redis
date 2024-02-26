package org.baylight.redis;
import org.baylight.redis.commands.RedisCommand;

public class EofCommand extends RedisCommand {

    public EofCommand() {
        super(Type.EOF);
    }

    @Override
    public byte[] getResponse() {
        return null;
    }

    @Override
    public String toString() {
        return "EofCommand []";
    }

}
