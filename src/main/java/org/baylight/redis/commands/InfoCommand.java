package org.baylight.redis.commands;

import java.util.Map;

import org.baylight.redis.RedisServiceBase;
import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespSimpleStringValue;
import org.baylight.redis.protocol.RespValue;

public class InfoCommand extends RedisCommand {

    private static ArgReader ARG_READER = new ArgReader(Type.INFO.name(), new String[] {
            ":string", // command name
            "[server]",
            "[clients]",
            "[memory]",
            "[persistence]",
            "[stats]",
            "[replication]",
            "[cpu]",
            "[commandstats]",
            "[latencystats]",
            "[sentinel]",
            "[cluster]",
            "[modules]",
            "[keyspace]",
            "[errorstats]",
            "[all]",
            "[default]",
            "[everything]"
    });

    private Map<String, RespValue> optionsMap = Map.of(
        "0", new RespSimpleStringValue(Type.INFO.name()));

    public InfoCommand() {
        super(Type.INFO);
    }

    @Override
    protected void setArgs(RespValue[] args) {
        optionsMap = ARG_READER.readArgs(args);
    }

    @Override
    public byte[] execute(RedisServiceBase service) {
        return new RespBulkString(service.info(optionsMap).getBytes()).asResponse();
    }

    @Override
    public String toString() {
        return "InfoCommand [optionsMap=" + optionsMap + "]";
    }

}
