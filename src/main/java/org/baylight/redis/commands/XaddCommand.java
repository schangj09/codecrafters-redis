package org.baylight.redis.commands;

import java.util.HashMap;
import java.util.Map;

import org.baylight.redis.RedisServiceBase;
import org.baylight.redis.protocol.RespArrayValue;
import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespValue;

public class XaddCommand extends RedisCommand {
    private String key;
    private String itemId;
    Map<String, RespValue> itemMap = new HashMap<>();

    public XaddCommand() {
        super(Type.XADD);
    }

    public XaddCommand(String key, String itemId) {
        super(Type.XADD);
        this.key = key;
        this.itemId = itemId;
    }

    @Override
    public byte[] execute(RedisServiceBase service) {
        service.xadd(key, itemId, itemMap);
        return new RespBulkString(key.getBytes()).asResponse();
    }

    @Override
    public byte[] asCommand() {
        return new RespArrayValue(
                new RespValue[] { new RespBulkString(getType().name().getBytes()) }).asResponse();
    }

    @Override
    protected void setArgs(RespValue[] args) {
        validateArgIsString(args, 0);
        if (!args[0].getValueAsString().toLowerCase().equals("xadd")) {
            throw new IllegalArgumentException(
                    String.format("Invalid command arg: %s", args[0].getValueAsString()));
        }
        validateArgIsString(args, 1);
        key = args[1].getValueAsString();
        validateArgIsString(args, 2);
        itemId = args[2].getValueAsString();

        int nextArg = 3;
        for (int i = nextArg; i < args.length; i += 2) {
            validateArgIsString(args, i);
            validateArgIsString(args, i + 1);
            String itemKey = args[i].getValueAsString();
            RespValue itemValue = args[i + 1];
            itemMap.put(itemKey, itemValue);
        }
    }

    @Override
    public String toString() {
        return "XaddCommand [key=" + key + ", itemId=" + itemId + ", itemMap=" + itemMap + "]";
    }

    public String getKey() {
        return key;
    }

    public String getItemId() {
        return itemId;
    }
}
