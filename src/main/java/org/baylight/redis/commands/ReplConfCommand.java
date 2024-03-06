package org.baylight.redis.commands;

import java.util.HashMap;
import java.util.Map;

import org.baylight.redis.RedisServiceBase;
import org.baylight.redis.protocol.RespArrayValue;
import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespSimpleStringValue;
import org.baylight.redis.protocol.RespValue;

public class ReplConfCommand extends RedisCommand {
    private static final String LISTENING_PORT_NAME = "listening-port";
    private static final String CAPA_NAME = "capa";

    public static enum Option {
        LISTENING_PORT(LISTENING_PORT_NAME), CAPA(CAPA_NAME);

        String name;

        Option(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    private static ArgReader ARG_READER = new ArgReader(Type.REPLCONF.name(),
            new String[] { ":string", // command name
                    "[listening-port:int capa:string]" });

    private Map<String, RespValue> optionsMap = new HashMap<>();

    public ReplConfCommand() {
        super(Type.REPLCONF);
    }

    public ReplConfCommand(Option option, String optionValue) {
        super(Type.REPLCONF);
        optionsMap.put("0", new RespSimpleStringValue(Type.REPLCONF.name()));
        optionsMap.put(option.getName(), new RespSimpleStringValue(optionValue));
    }

    @Override
    protected void setArgs(RespValue[] args) {
        optionsMap = ARG_READER.readArgs(args);
    }

    @Override
    public byte[] execute(RedisServiceBase service) {
        return service.replicationConfirm(optionsMap);
    }

    @Override
    public byte[] asCommand() {
        RespValue[] cmdValues;
        if (optionsMap.containsKey(LISTENING_PORT_NAME)) {
            cmdValues = new RespValue[] { new RespBulkString(getType().name().getBytes()),
                    new RespBulkString(LISTENING_PORT_NAME.getBytes()), new RespBulkString(
                            optionsMap.get(LISTENING_PORT_NAME).getValueAsString().getBytes()) };
        } else if (optionsMap.containsKey(CAPA_NAME)) {
            cmdValues = new RespValue[] { new RespBulkString(getType().name().getBytes()),
                    new RespBulkString(CAPA_NAME.getBytes()),
                    new RespBulkString(optionsMap.get(CAPA_NAME).getValueAsString().getBytes()) };
        } else {
            cmdValues = new RespValue[] { new RespBulkString(getType().name().getBytes()) };
        }
        return new RespArrayValue(cmdValues).asResponse();
    }

    @Override
    public String toString() {
        return "ReplConfCommand [optionsMap=" + optionsMap + "]";
    }

}
