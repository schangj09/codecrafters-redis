package org.baylight.redis.commands;

import org.baylight.redis.RedisServiceBase;
import org.baylight.redis.protocol.RespArrayValue;
import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespSimpleErrorValue;
import org.baylight.redis.protocol.RespValue;
import org.baylight.redis.streams.IllegalStreamItemIdException;

public class XrangeCommand extends RedisCommand {
    private String key;
    private String start;
    private String end;

    public XrangeCommand() {
        super(Type.XRANGE);
    }

    public XrangeCommand(String key, String start, String end) {
        super(Type.XRANGE);
        this.key = key;
        this.start = start;
        this.end = end;
    }

    @Override
    public byte[] execute(RedisServiceBase service) {
        try {
            service.xrange(key, start, end);
            return new RespArrayValue(new RespValue[] {}).asResponse();
        } catch (IllegalStreamItemIdException e) {
            return new RespSimpleErrorValue(e.getMessage()).asResponse();
        }
    }

    @Override
    public byte[] asCommand() {
        return new RespArrayValue(
                new RespValue[] { new RespBulkString(getType().name().getBytes()) }).asResponse();
    }

    @Override
    protected void setArgs(RespValue[] args) {
        validateArgIsString(args, 0);
        if (!args[0].getValueAsString().toLowerCase().equals("xrange")) {
            throw new IllegalArgumentException(
                    String.format("%s: Invalid command arg: %s", type.name(),
                            args[0].getValueAsString()));
        }
        validateArgIsString(args, 1);
        key = args[1].getValueAsString();
        validateArgIsString(args, 2);
        start = args[2].getValueAsString();
        validateArgIsString(args, 3);
        end = args[3].getValueAsString();
    }

    @Override
    public String toString() {
        return "XrangeCommand [key=" + key + ", start=" + start + ", end=" + end + "]";
    }

    public String getKey() {
        return key;
    }

    public String getStart() {
        return start;
    }

    public String getEnd() {
        return end;
    }

}
