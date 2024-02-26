package org.baylight.redis.commands;

import org.baylight.redis.RedisService;
import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespConstants;
import org.baylight.redis.protocol.RespValue;

public class GetCommand extends RedisCommand {
    private RespBulkString key;

    public GetCommand() {
        super(Type.GET);
    }

    public GetCommand(RespBulkString key) {
        super(Type.GET);
        this.key = key;
    }

    @Override
    public void setArgs(RespValue[] args) {
        validateNumArgs(args, len -> len >= 2);
        validateArgIsString(args, 1);
        this.key = args[1].asBulkString();
    }

    @Override
    public byte[] execute(RedisService service) {
        if (service.containsKey(key.getValueAsString()))
            return new RespBulkString(service.get(key.getValueAsString())).asResponse();
        else
            return RespConstants.NULL;
    }

    @Override
    public String toString() {
        return "GetCommand [key=" + key + "]";
    }

}
