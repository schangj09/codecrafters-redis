package org.baylight.redis.commands;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.baylight.redis.RedisServiceBase;
import org.baylight.redis.protocol.RespArrayValue;
import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespSimpleErrorValue;
import org.baylight.redis.protocol.RespValue;
import org.baylight.redis.streams.IllegalStreamItemIdException;
import org.baylight.redis.streams.StreamId;

public class XaddCommand extends RedisCommand {

    private static ArgReader ARG_READER = new ArgReader(Type.XADD.name(), new String[] {
            ":string", // command name
            ":string", // key
            ":string", // itemId
            ":var" // itemMap
    });

    private String key;
    private String itemId;
    RespValue[] itemMap = null;

    public XaddCommand() {
        super(Type.XADD);
    }

    public XaddCommand(String key, String itemId) {
        super(Type.XADD);
        this.key = key;
        this.itemId = itemId;
        this.itemMap = new RespValue[0];
    }

    @Override
    public byte[] execute(RedisServiceBase service) {
        try {
            StreamId streamId = service.xadd(key, itemId, itemMap);
            return new RespBulkString(streamId.getId().getBytes()).asResponse();
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
        Map<String, RespValue> optionsMap = ARG_READER.readArgs(args);
        key = optionsMap.get("1").getValueAsString();
        itemId = optionsMap.get("2").getValueAsString();

        if (!optionsMap.containsKey("3")) {
            throw new IllegalArgumentException(String
                    .format("%s: missing map values", type.name()));
        } else {
            RespArrayValue itemMapArg = (RespArrayValue) optionsMap.get("3");
            itemMap = itemMapArg.getValues();

            Set<RespValue> itemKeys = new HashSet<>();
            if ((itemMap.length) % 2 == 1) {
                throw new IllegalArgumentException(
                        String.format("%s: Invalid number of item key value pairs", type.name()));
            }
            for (int i = 0; i < itemMap.length; i += 2) {
                if (itemKeys.contains(itemMap[i])) {
                    throw new IllegalArgumentException(
                            String.format("%s: Duplicate item key: %s", type.name(), itemMap[i]));
                }
                itemKeys.add(itemMap[i]);
            }
        }
    }

    @Override
    public String toString() {
        return "XaddCommand [key=" + key + ", itemId=" + itemId + ", itemMap="
                + Arrays.toString(itemMap) + "]";
    }

    public String getKey() {
        return key;
    }

    public String getItemId() {
        return itemId;
    }

    public RespValue[] getItemMap() {
        return itemMap;
    }

}
