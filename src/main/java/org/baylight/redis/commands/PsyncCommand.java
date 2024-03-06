package org.baylight.redis.commands;

import java.util.HashMap;
import java.util.Map;

import org.baylight.redis.RedisServiceBase;
import org.baylight.redis.protocol.RespArrayValue;
import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespInteger;
import org.baylight.redis.protocol.RespSimpleStringValue;
import org.baylight.redis.protocol.RespValue;

public class PsyncCommand extends RedisCommand {

    private static ArgReader ARG_READER = new ArgReader(Type.PSYNC.name(), new String[] { ":string", // command
                                                                                                     // name
            ":string", // first arg
            ":int" // second arg
    });

    private Map<String, RespValue> optionsMap = new HashMap<>();

    public PsyncCommand() {
        super(Type.PSYNC);
    }

    public PsyncCommand(String arg1, Long arg2) {
        super(Type.PSYNC);
        optionsMap.put("0", new RespSimpleStringValue(Type.PSYNC.name()));
        optionsMap.put("1", new RespSimpleStringValue(arg1));
        optionsMap.put("2", new RespInteger(arg2));
    }

    @Override
    protected void setArgs(RespValue[] args) {
        optionsMap = ARG_READER.readArgs(args);
    }

    @Override
    public byte[] execute(RedisServiceBase service) {
        return service.psync(optionsMap);
    }

    @Override
    public byte[] asCommand() {
        RespValue[] cmdValues;
        cmdValues = new RespValue[] { new RespBulkString(getType().name().getBytes()),
                new RespBulkString(optionsMap.get("1").getValueAsString().getBytes()),
                new RespBulkString(optionsMap.get("2").getValueAsString().getBytes()) };
        return new RespArrayValue(cmdValues).asResponse();
    }

    @Override
    public String toString() {
        return "PsyncCommand [optionsMap=" + optionsMap + "]";
    }

}
