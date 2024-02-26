package org.baylight.redis.commands;

import org.baylight.redis.RedisService;
import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespConstants;
import org.baylight.redis.protocol.RespValue;

public class SetCommand extends RedisCommand {

    RespBulkString key;
    RespBulkString value;

    public SetCommand() {
        super(Type.ECHO);
    }

    public SetCommand(RespBulkString key, RespBulkString value) {
        super(Type.ECHO);
        this.key = key;
        this.value = value;
    }

    @Override
    public void setArgs(RespValue[] args) {
        validateNumArgs(args, len -> len >= 3);
        validateArgIsString(args, 1);
        validateArgIsString(args, 2);
        this.key = args[1].asBulkString();
        this.value = args[2].asBulkString();
    }

    @Override
    public byte[] execute(RedisService service) {
        service.set(key.getValueAsString(), value.getValue());
        return RespConstants.OK;
    }

    @Override
    public String toString() {
        return "SetCommand [key=" + key + ", value=" + value + "]";
    }
}
